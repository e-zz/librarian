#!/usr/bin/env python3
"""
Standalone PDF downloader: DOI/arXiv → resolve → parallel download.

Resolves DOIs and arXiv identifiers to arXiv PDFs, downloads them in parallel,
and optionally imports metadata + PDFs into Zotero via the cookjohn MCP plugin.

Key features:
  • Accepts DOIs, arXiv IDs, arXiv URLs — auto-resolves DOI → arXiv when possible.
  • Parallel downloads with configurable concurrency (respects arXiv rate limits).
  • CrossRef metadata enrichment (journal, volume, DOI) for Zotero imports.
  • Optional Zotero integration via local cookjohn MCP plugin (no API key needed).
  • Saves .meta.json sidecar files alongside PDFs for downstream RAGFlow enrichment.
  • arXiv TOU-compliant rate limiter with exponential backoff on 429 responses.

Open-source configuration (set via environment variables before running):
  MINERU_DOWNLOAD_DIR   — download directory (default: ./downloads/)
  MINERU_CONCURRENCY    — max parallel PDF downloads (default: 4)
  CONTACT_EMAIL         — polite User-Agent for CrossRef API (default: generic GitHub URL)

Usage:
  python pdf_downloader.py 10.1007/s42484-025-00254-8 https://arxiv.org/abs/2311.08990
  python pdf_downloader.py --input dois.txt --out ~/MinerU/downloads/
  python pdf_downloader.py --input dois.txt --zotero --zotero-collection qml
"""

# =============================================================================
# Imports
# =============================================================================

import argparse
import json
import os
import queue
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

# =============================================================================
# Configuration (overridable via environment variables)
# =============================================================================

# MINERU_DOWNLOAD_DIR: download directory for PDFs
# Fallback: "./downloads/" (relative to CWD)
# Override: set the MINERU_DOWNLOAD_DIR environment variable
DOWNLOAD_DIR = Path(os.environ.get("MINERU_DOWNLOAD_DIR", "./downloads/"))

# CONCURRENCY: maximum parallel PDF downloads
# This is the default; the --concurrency CLI flag takes precedence at runtime.
# Override: set the MINERU_CONCURRENCY environment variable
CONCURRENCY = int(os.environ.get("MINERU_CONCURRENCY", "4"))

# CONTACT_EMAIL: polite User-Agent string for CrossRef API calls.
# Set this to your email or project URL so API providers can reach you if
# something goes wrong. Falls back to a generic project URL.
CONTACT_EMAIL = os.environ.get(
    "CONTACT_EMAIL",
    "librarian (https://github.com/e-zz/librarian)"
)

# arXiv API endpoint — stable, no env var override needed.
# Uses the export subdomain for programmatic access (not the web frontend).
# Documentation: https://info.arxiv.org/help/api/index.html
ARXIV_API = "https://export.arxiv.org/api/query"

# =============================================================================
# arXiv Rate Limiter
# =============================================================================
#
# arXiv Terms of Use (https://info.arxiv.org/help/api/tou.html) mandate:
#   "make no more than one request every three seconds, and limit
#    requests to a single connection at a time."
#
# This rate limiter enforces TOU compliance across all arXiv HTTP requests
# (both API queries and PDF downloads) via a single global lock.
#
# HTTP 429 (Too Many Requests) responses trigger exponential backoff:
#   1s, 2s, 4s, 8s, 16s — up to 5 retries, capped at 30s.
# =============================================================================

class ArXivRateLimiter:
    """Thread-safe global rate limiter for all arXiv HTTP requests.

    A single threading.Lock serializes ALL arXiv traffic (API + PDF downloads)
    to maintain the single-connection requirement. API calls enforce a 3-second
    minimum interval between requests; PDF downloads use a 1-second interval
    (the TOU's 3s rule technically applies to the API, but PDF downloads still
    trigger IP throttling at high concurrency).

    HTTP 429 responses trigger exponential backoff: 1s, 2s, 4s, 8s, 16s
    up to 5 retries, capped at 30s.
    """

    # Class-level state shared across all threads
    _lock = threading.Lock()        # Serializes ALL arXiv traffic
    _last_request = 0.0            # Timestamp of the last request (time.time())
    _max_retries = 5               # Max retries on HTTP 429
    _backoff_cap = 30.0            # Max backoff delay in seconds

    @classmethod
    def _wait_and_stamp(cls, min_interval: float):
        """Acquire the global lock, wait for the minimum interval, then stamp time.

        Args:
            min_interval: Minimum seconds since the last request (3.0 for API,
                          1.0 for PDF downloads).
        """
        with cls._lock:
            elapsed = time.time() - cls._last_request
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            cls._last_request = time.time()

    @classmethod
    def api_get(cls, url: str, timeout: float = 15) -> requests.Response:
        """Rate-limited GET request to export.arxiv.org with 429 retry.

        Enforces the 3-second minimum interval per arXiv TOU.
        Automatically retries on HTTP 429 with exponential backoff.

        Args:
            url: The arXiv API URL to query.
            timeout: Request timeout in seconds (default: 15).

        Returns:
            requests.Response object (may have non-200 status if all retries exhausted).
        """
        for attempt in range(cls._max_retries + 1):
            cls._wait_and_stamp(3.0)  # TOU: 1 request / 3 seconds
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 429 and attempt < cls._max_retries:
                delay = min(2.0 ** attempt, cls._backoff_cap)
                print(
                    f"  [arXiv] HTTP 429 → retry in {delay:.0f}s "
                    f"(attempt {attempt + 1}/{cls._max_retries})"
                )
                time.sleep(delay)
                continue
            return resp
        return resp  # All retries exhausted; return last response

    @classmethod
    def api_urlopen(cls, url: str, timeout: float = 15):
        """Rate-limited urllib.request.urlopen to export.arxiv.org with 429 retry.

        urllib raises HTTPError on 4xx/5xx (unlike requests), so 429 is caught
        as an exception rather than a status code. Used for XML/Atom responses
        where urllib's built-in XML parsing is convenient.

        Args:
            url: The arXiv API URL to query.
            timeout: Request timeout in seconds (default: 15).

        Returns:
            http.client.HTTPResponse object.

        Raises:
            urllib.error.HTTPError: For non-429 errors or when retries exhausted.
        """
        for attempt in range(cls._max_retries + 1):
            cls._wait_and_stamp(3.0)  # TOU: 1 request / 3 seconds
            try:
                return urllib.request.urlopen(url, timeout=timeout)
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < cls._max_retries:
                    delay = min(2.0 ** attempt, cls._backoff_cap)
                    print(
                        f"  [arXiv] HTTP 429 → retry in {delay:.0f}s "
                        f"(attempt {attempt + 1}/{cls._max_retries})"
                    )
                    time.sleep(delay)
                    continue
                raise

    @classmethod
    def pdf_get(cls, url: str, stream: bool = True, timeout: float = 60) -> requests.Response:
        """Rate-limited GET for arxiv.org/pdf/ downloads.

        PDF downloads aren't strictly covered by the API TOU's 3-second rule,
        but high-concurrency downloads still trigger IP-level throttling.
        Uses a 1-second minimum interval; shares the same lock for overall
        single-connection compliance.

        Args:
            url: The arXiv PDF URL to download.
            stream: Whether to stream the response (default: True).
            timeout: Request timeout in seconds (default: 60, generous for PDFs).

        Returns:
            requests.Response object.
        """
        for attempt in range(cls._max_retries + 1):
            cls._wait_and_stamp(1.0)  # 1-second interval for PDF downloads
            resp = requests.get(url, stream=stream, timeout=timeout)
            if resp.status_code == 429 and attempt < cls._max_retries:
                delay = min(2.0 ** attempt, cls._backoff_cap)
                print(
                    f"  [arXiv PDF] HTTP 429 → retry in {delay:.0f}s "
                    f"(attempt {attempt + 1}/{cls._max_retries})"
                )
                time.sleep(delay)
                continue
            return resp
        return resp

# =============================================================================
# cookjohn MCP Endpoint (local Zotero plugin)
# =============================================================================
#
# cookjohn is a local Zotero plugin that exposes a Streamable HTTP MCP server.
# It runs inside the Zotero desktop application and is reachable at
# http://127.0.0.1:23120/mcp — this is the default port and a hard-coded
# constant because cookjohn does not currently support relocation.
#
# No API key is needed because cookjohn talks to the local Zotero instance
# directly (loopback interface). This avoids the complexity of WebDAV sync
# or pyzotero API keys.
# =============================================================================

COOKJOHN_MCP = "http://127.0.0.1:23120/mcp"  # cookjohn default endpoint
COOKJOHN_TIMEOUT = 30                          # seconds for MCP calls


def _cookjohn_call(tool_name: str, arguments: dict, timeout: int = COOKJOHN_TIMEOUT) -> dict:
    """Call a cookjohn MCP tool via JSON-RPC over HTTP.

    cookjohn implements the Model Context Protocol (MCP) over Streamable HTTP.
    This function sends a JSON-RPC 2.0 request and extracts the text result
    from the MCP response envelope.

    Args:
        tool_name: Name of the cookjohn tool (e.g. 'write_item', 'search_library').
        arguments: Dictionary of arguments for the tool.
        timeout: Request timeout in seconds (default: 30).

    Returns:
        Parsed JSON result from the tool (typically a dict).

    Raises:
        urllib.error.URLError: If cookjohn is unreachable (Zotero not running?).
        KeyError: If the MCP response is malformed.
    """
    req = urllib.request.Request(
        COOKJOHN_MCP,
        data=json.dumps({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": 1,
        }).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    raw = json.loads(resp.read())
    # MCP response structure:
    #   {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": "..."}]}}
    text = raw["result"]["content"][0]["text"]
    return json.loads(text)


# =============================================================================
# Resolution: DOI / arXiv ID / arXiv URL → normalized form
# =============================================================================


def resolve_to_arxiv(raw: str) -> dict | None:
    """Normalize any input format to an arXiv ID + URL.

    Accepts:
      - arXiv URLs (https://arxiv.org/abs/2311.08990)
      - Bare arXiv IDs (2311.08990, 2311.08990v1)
      - DOIs (10.1007/...)
      - DOI URLs (https://doi.org/10.1007/...)

    For DOIs, queries the arXiv API to check if there's an associated arXiv
    version. If not, returns the DOI with no arXiv ID (the caller can still
    use Zotero to fetch the paper from the DOI).

    Args:
        raw: A string containing a DOI, arXiv ID, or URL.

    Returns:
        A dict with keys: arxiv_id, arxiv_url, doi.
        Returns None if the input is completely unrecognizable.
    """
    raw = raw.strip()

    # --- arXiv URL pattern ---
    # Matches: https://arxiv.org/abs/2311.08990, https://arxiv.org/abs/2311.08990v2
    m = re.search(r'arxiv\.org/abs/([\d.]+(?:v\d+)?)', raw)
    if m:
        aid = m.group(1).rstrip('/')
        return {
            "arxiv_id": aid,
            "arxiv_url": f"https://arxiv.org/abs/{aid}",
            "doi": None,
        }

    # --- Bare arXiv ID ---
    # Matches the standard arXiv identifier format: YYMM.NNNNN or YYMM.NNNNNvN
    if re.match(r'^\d{4}\.\d{4,8}(v\d+)?$', raw):
        return {
            "arxiv_id": raw,
            "arxiv_url": f"https://arxiv.org/abs/{raw}",
            "doi": None,
        }

    # --- DOI ---
    # Accepts bare DOIs (10.xxx/...) and full DOI URLs
    doi = raw
    if doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "")
    if doi.startswith("10."):
        try:
            # Query the arXiv API to find an associated arXiv ID for this DOI
            resp = ArXivRateLimiter.api_get(
                f"{ARXIV_API}?search_query=doi:{doi}&max_results=1",
                timeout=15,
            )
            m2 = re.search(r'<id>http://arxiv\.org/abs/([\d.]+)', resp.text)
            if m2:
                aid = m2.group(1)
                return {
                    "arxiv_id": aid,
                    "arxiv_url": f"https://arxiv.org/abs/{aid}",
                    "doi": doi,
                }
            # DOI found but no arXiv version exists — return DOI-only result
            return {"arxiv_id": None, "arxiv_url": None, "doi": doi}
        except Exception:
            pass

    # Unrecognizable format
    return None


# =============================================================================
# Download
# =============================================================================


def download_arxiv(arxiv_id: str, dest_dir: Path = DOWNLOAD_DIR) -> Path | None:
    """Download a PDF from arXiv by its ID.

    Skips if the file already exists and is non-empty (>1 KB).
    Streams the download in 64 KB chunks to handle large PDFs gracefully.

    Args:
        arxiv_id: The arXiv paper ID (e.g., '2311.08990').
        dest_dir: Target directory for the downloaded PDF (default: DOWNLOAD_DIR).

    Returns:
        Path to the downloaded file, or None on failure.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{arxiv_id}.pdf"

    # Skip if already downloaded (non-empty file)
    if dest.exists() and dest.stat().st_size > 1000:
        return dest

    url = f"https://arxiv.org/pdf/{arxiv_id}"
    try:
        resp = ArXivRateLimiter.pdf_get(url, stream=True, timeout=60)
        if resp.status_code != 200:
            return None
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(65536):  # 64 KB chunks
                f.write(chunk)
        return dest
    except Exception:
        return None


# =============================================================================
# Zotero Helpers (via cookjohn MCP path)
# =============================================================================
#
# These functions handle metadata fetching and Zotero item creation.
# Metadata is fetched from CrossRef (richer) with arXiv XML as fallback.
# PDFs are attached to Zotero items automatically.
#
# NOTE: This uses the local cookjohn MCP plugin, NOT the WebDAV/pyzotero API.
# See pipeline_full_web.py for the pyzotero variant.
# =============================================================================


def fetch_arxiv_metadata(arxiv_id: str) -> dict | None:
    """Fetch metadata from arXiv's Atom XML feed for a given arXiv ID.

    If the arXiv entry contains a DOI, delegates to CrossRef for richer
    metadata (journal name, volume, issue, publisher — things arXiv XML
    doesn't provide). Falls back to parsing arXiv XML directly if CrossRef
    is unreachable or the DOI has no CrossRef record.

    Args:
        arxiv_id: The arXiv paper ID (e.g., '2311.08990').

    Returns:
        A dict with keys: title, authors, abstract, year.
        Returns None if the arXiv API is unreachable or the paper doesn't exist.
    """
    url = f"{ARXIV_API}?id_list={arxiv_id}&max_results=1"
    try:
        resp = ArXivRateLimiter.api_urlopen(url, timeout=15)
        tree = ET.parse(resp)
        root = tree.getroot()

        # arXiv Atom XML uses these namespaces
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'arxiv': 'http://arxiv.org/schemas/atom',
        }

        entry = root.find('atom:entry', ns)
        if entry is None:
            return None

        # --- If the entry has a DOI, prefer CrossRef (richer metadata) ---
        doi_el = entry.find('arxiv:doi', ns)
        if doi_el is not None and doi_el.text:
            doi = doi_el.text.strip()
            crossref_meta = fetch_doi_metadata(doi)
            if crossref_meta:
                return crossref_meta

        # --- Fallback: parse arXiv XML directly ---
        title_el = entry.find('atom:title', ns)
        title = (
            title_el.text.strip().replace('\n', ' ')
            if title_el is not None and title_el.text
            else ''
        )

        summary_el = entry.find('atom:summary', ns)
        abstract = (
            summary_el.text.strip().replace('\n', ' ')
            if summary_el is not None and summary_el.text
            else ''
        )

        published_el = entry.find('atom:published', ns)
        year = (
            published_el.text[:4]
            if published_el is not None and published_el.text
            else ''
        )

        # Parse authors from arXiv XML
        authors = []
        for author in entry.findall('atom:author', ns):
            name_el = author.find('atom:name', ns)
            if name_el is not None and name_el.text:
                name = name_el.text.strip()
                parts = name.rsplit(' ', 1)
                if len(parts) == 2:
                    authors.append({
                        'creatorType': 'author',
                        'lastName': parts[1],
                        'firstName': parts[0],
                    })
                else:
                    authors.append({'creatorType': 'author', 'name': name})

        return {
            'title': title,
            'authors': authors,
            'abstract': abstract,
            'year': year,
        }

    except Exception as e:
        print(f"  [zotero] arXiv metadata fetch failed for {arxiv_id}: {e}")
        return None


def fetch_doi_metadata(doi: str) -> dict | None:
    """Fetch full metadata from the CrossRef API for a given DOI.

    CrossRef provides the richest metadata of any free resolution service:
    journal name, volume, issue, pages, DOI, ISBN/ISSN, publisher, language,
    document type, conference name, thesis institution, and more.

    The User-Agent header is set to CONTACT_EMAIL (configurable via the
    CONTACT_EMAIL environment variable) as a courtesy to CrossRef.

    Args:
        doi: The DOI string (e.g., '10.1007/s42484-025-00254-8').

    Returns:
        A dict with bibliographic fields including title, authors, abstract,
        year, journal, DOI, type, subtype, volume, issue, pages, publisher,
        ISBN, ISSN, language, URL, university, conferenceName.
        Returns None on failure.
    """
    url = f"https://api.crossref.org/works/{doi}"
    try:
        # NOTE: The User-Agent header is required by CrossRef's terms of service.
        # Setting a meaningful User-Agent helps the API team contact you if
        # your usage pattern causes issues, and improves rate limit treatment.
        req = urllib.request.Request(
            url,
            headers={"User-Agent": CONTACT_EMAIL},
        )
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
        msg = data.get("message", {})

        # --- Core fields ---
        title_list = msg.get("title", [])
        title = (
            title_list[0].strip().replace('\n', ' ')
            if title_list else ""
        )

        abstract = (msg.get("abstract", "") or "").strip().replace('\n', ' ')

        # Extract year from various date fields (CrossRef returns an array
        # of [year, month, day] parts for each date type)
        year = ""
        for date_field in ("published-print", "published-online", "issued", "created"):
            date_parts = msg.get(date_field, {}).get("date-parts", [[None]])
            if date_parts and date_parts[0] and date_parts[0][0]:
                year = str(date_parts[0][0])
                break

        # --- Authors ---
        authors = []
        for author in msg.get("author", []):
            family = author.get("family", "")
            given = author.get("given", "")
            if family:
                authors.append({
                    'creatorType': 'author',
                    'lastName': family,
                    'firstName': given,
                })
            elif given:
                authors.append({'creatorType': 'author', 'name': given})

        # --- Journal / container ---
        container = msg.get("container-title", [])
        journal = container[0] if container else ""

        # --- Build result dict ---
        result = {
            'title': title,
            'authors': authors,
            'abstract': abstract,
            'year': year,
            'journal': journal,
            'doi': msg.get("DOI", doi),
            # Document type for Zotero itemType selection (see _crossref_type_to_zotero)
            'type': msg.get("type", ""),
            'subtype': msg.get("subtype", ""),
        }

        # --- Optional bibliographic fields (only present when CrossRef has them) ---
        if msg.get("volume"):
            result['volume'] = msg["volume"]
        if msg.get("issue"):
            result['issue'] = msg["issue"]
        if msg.get("page"):
            result['pages'] = msg["page"]
        if msg.get("publisher"):
            result['publisher'] = msg["publisher"]

        # ISBN (CrossRef returns an array for books — multiple ISBN variants)
        isbn_list = msg.get("ISBN", [])
        if isbn_list:
            result['isbn'] = (
                " ".join(isbn_list)
                if isinstance(isbn_list, list)
                else str(isbn_list)
            )

        # ISSN (CrossRef also returns an array)
        issn_list = msg.get("ISSN", [])
        if issn_list:
            result['issn'] = (
                " ".join(issn_list)
                if isinstance(issn_list, list)
                else str(issn_list)
            )

        if msg.get("language"):
            result['language'] = msg["language"]

        # URL (prefer the DOI-resolved URL)
        result['url'] = msg.get("URL", f"https://doi.org/{doi}")

        # Thesis-specific: university from institution field
        institutions = msg.get("institution", [])
        if institutions and isinstance(institutions, list) and institutions[0].get("name"):
            result['university'] = institutions[0]["name"]

        # Conference name
        event = msg.get("event", {})
        if event.get("name"):
            result['conferenceName'] = event["name"]

        return result

    except Exception as e:
        print(f"  [zotero] CrossRef metadata fetch failed for {doi}: {e}")
        return None


def _crossref_type_to_zotero(cr_type: str, subtype: str = "") -> str | None:
    """Map a CrossRef document type to a Zotero itemType.

    CrossRef uses its own type taxonomy (e.g., 'journal-article', 'book-chapter').
    Zotero uses a different taxonomy (e.g., 'journalArticle', 'bookSection').
    This function translates between the two.

    Special cases:
      - 'dataset' → None (skip — Zotero doesn't import datasets sensibly)
      - 'posted-content' → checks subtype for 'preprint' vs default journalArticle
      - Unknown types → 'journalArticle' (safe fallback)

    Args:
        cr_type: CrossRef document type string.
        subtype: CrossRef subtype (used for posted-content → preprint detection).

    Returns:
        A Zotero itemType string, or None to skip the item.
    """
    mapping = {
        "journal-article":      "journalArticle",
        "proceedings-article":  "conferencePaper",
        "book":                 "book",
        "book-chapter":         "bookSection",
        "book-section":         "bookSection",
        "book-part":            "bookSection",
        "reference-entry":      "bookSection",
        "dissertation":         "thesis",
        "report":               "report",
        "report-component":     "report",
        "monograph":            "book",
        "standard":             "report",
        "dataset":              None,  # skip — no Zotero equivalent
    }
    if cr_type in mapping:
        return mapping[cr_type]

    # posted-content: check subtype for preprint
    if cr_type == "posted-content":
        if subtype == "preprint":
            return "preprint"
        return "journalArticle"  # arXiv papers default to journalArticle

    # Default fallback for unrecognized types
    return "journalArticle"


def resolve_collection_key(name_or_key: str) -> str | None:
    """Resolve a Zotero collection name to its 8-character key.

    Accepts either a collection name (e.g., 'Quantum ML') or an existing
    8-character alphanumeric key. Keys pass through as-is with a validation
    check; names are resolved via cookjohn's search_collections tool.

    Args:
        name_or_key: A collection name or 8-character key.

    Returns:
        The 8-character collection key, or None if not found.
    """
    # Short-circuit: if it looks like an 8-char key, try it directly
    if len(name_or_key) == 8 and name_or_key.isalnum():
        try:
            _cookjohn_call('search_collections', {'q': name_or_key, 'limit': 1})
            return name_or_key  # Likely a valid key
        except Exception:
            pass  # Fall through to name search

    # Search by name
    try:
        results = _cookjohn_call('search_collections', {'q': name_or_key, 'limit': 5})
        # results is a list of {key, name, path, depth, parentCollection}
        for col in results:
            if col.get('name', '').lower() == name_or_key.lower():
                return col['key']
    except Exception:
        pass

    print(
        f"  [zotero] WARNING: collection '{name_or_key}' not found, "
        f"item will be uncategorized"
    )
    return None


def zotero_add_entry(
    arxiv_url: str,
    arxiv_id: str,
    collection_key: str | None = None,
    pdf_path: str | None = None,
) -> dict:
    """Add a paper to Zotero via cookjohn MCP (local plugin, no API key).

    This is the core Zotero import function. It:
      1. Deduplicates by searching for the arXiv URL in Zotero.
      2. If the item exists but has no PDF attached, attaches the downloaded PDF.
      3. If the item doesn't exist, fetches rich metadata via CrossRef (preferred)
         or arXiv XML (fallback).
      4. Creates the Zotero item with type-appropriate fields (journal article,
         book, thesis, etc.).
      5. Attaches the downloaded PDF file.
      6. Adds the item to the specified collection.
      7. Saves a .meta.json sidecar file alongside the PDF for downstream
         RAGFlow enrichment (title, authors, abstract, etc.).

    IMPORTANT: This function is NOT thread-safe — Zotero cookjohn MCP does not
    support concurrent writes. Use ZoteroQueue (below) for serialized access.

    Args:
        arxiv_url: The arXiv abstract page URL.
        arxiv_id: The arXiv paper ID (or DOI string for DOI-only items).
        collection_key: Optional Zotero collection key to place the item in.
        pdf_path: Optional path to the downloaded PDF file.

    Returns:
        A dict with keys: status (str), key (str), title (str), etc.
        Status values: 'exists', 'attached', 'added', 'failed'.
    """
    # ── Step 1: Dedup — check if this item already exists in Zotero ──
    try:
        result = _cookjohn_call('search_library', {
            'q': arxiv_url,
            'mode': 'preview',
            'limit': 3,
        })
        total = result.get('pagination', {}).get('total', 0)
        if isinstance(total, str):
            total = int(total)
        if total > 0 and result.get('results'):
            item = result['results'][0]
            item_key = item['key']
            has_attachments = bool(item.get('attachments'))

            # Item exists but no PDF attached → attach the downloaded PDF now
            if not has_attachments and pdf_path:
                try:
                    _cookjohn_call('write_item', {
                        'action': 'import',
                        'filePath': pdf_path,
                        'parentItemKey': item_key,
                        'title': f'{arxiv_id}.pdf',
                    })
                    return {
                        'status': 'attached',
                        'key': item_key,
                        'title': item.get('title', '?')[:80],
                    }
                except Exception as e:
                    print(
                        f"  [zotero] PDF attach failed for existing {arxiv_id}: {e}"
                    )

            return {
                'status': 'exists',
                'key': item_key,
                'title': item.get('title', '?')[:80],
                'has_pdf': has_attachments,
            }
    except Exception as e:
        print(f"  [zotero] dedup search failed for {arxiv_id}: {e}")

    # ── Step 2: Fetch metadata ──
    is_doi = arxiv_id.startswith('10.')
    if is_doi:
        meta = fetch_doi_metadata(arxiv_id)
    else:
        meta = fetch_arxiv_metadata(arxiv_id)

    if not meta:
        meta = {
            'title': f'[DOI:{arxiv_id}]' if is_doi else f'[arXiv:{arxiv_id}]',
            'authors': [],
            'abstract': '',
            'year': '',
        }

    # ── Step 3: Save metadata sidecar for RAGFlow enrichment ──
    if pdf_path:
        try:
            meta_path = Path(pdf_path).with_suffix('.meta.json')
            meta_path.write_text(
                json.dumps({
                    'arxiv_id': arxiv_id,
                    'arxiv_url': arxiv_url,
                    'item_type': item_type,
                    **{k: v for k, v in meta.items()},
                }, ensure_ascii=False, indent=2),
                encoding='utf-8',
            )
        except Exception:
            pass

    # ── Step 4: Create Zotero item with type-aware metadata ──
    try:
        cr_type = meta.get('type', '')
        cr_subtype = meta.get('subtype', '')
        item_type = _crossref_type_to_zotero(cr_type, cr_subtype)
        if item_type is None:
            return {
                'status': 'failed',
                'error': f'dataset type skipped: {cr_type}',
            }

        # Common fields for all item types
        fields = {
            'title': meta.get('title', ''),
            'url': meta.get('url', arxiv_url),
            'date': meta.get('year', ''),
            'abstractNote': meta.get('abstract', ''),
        }

        # DOI / identifier
        doi = meta.get('doi', arxiv_id if is_doi else '')
        if doi:
            fields['DOI'] = doi

        # ── Type-specific field mapping ──

        if item_type in ('journalArticle', 'conferencePaper', 'preprint'):
            fields['libraryCatalog'] = 'CrossRef' if doi else 'arXiv'
            fields['extra'] = f'DOI: {doi}' if doi else f'arXiv: {arxiv_id}'
            if meta.get('journal'):
                fields['publicationTitle'] = meta['journal']
            if meta.get('volume'):
                fields['volume'] = meta['volume']
            if meta.get('issue'):
                fields['issue'] = meta['issue']
            if meta.get('pages'):
                fields['pages'] = meta['pages']
            if meta.get('issn'):
                fields['ISSN'] = meta['issn']
            if meta.get('conferenceName') and item_type == 'conferencePaper':
                fields['conferenceName'] = meta['conferenceName']
            if item_type == 'preprint':
                fields['archive'] = 'arXiv'
                fields['archiveLocation'] = arxiv_id
                # For preprints without DOI, still mark as arXiv
                if not doi:
                    fields['extra'] = f'arXiv: {arxiv_id}'

        elif item_type == 'book':
            fields['libraryCatalog'] = 'CrossRef'
            fields['extra'] = f'DOI: {doi}' if doi else ''
            if meta.get('publisher'):
                fields['publisher'] = meta['publisher']
            if meta.get('isbn'):
                fields['ISBN'] = meta['isbn']

        elif item_type == 'bookSection':
            fields['libraryCatalog'] = 'CrossRef'
            fields['extra'] = f'DOI: {doi}' if doi else ''
            if meta.get('journal'):
                fields['bookTitle'] = meta['journal']  # container-title = book title
            if meta.get('publisher'):
                fields['publisher'] = meta['publisher']
            if meta.get('isbn'):
                fields['ISBN'] = meta['isbn']
            if meta.get('pages'):
                fields['pages'] = meta['pages']

        elif item_type == 'thesis':
            fields['libraryCatalog'] = 'CrossRef'
            fields['extra'] = f'DOI: {doi}' if doi else ''
            university = meta.get('university') or meta.get('publisher', '')
            if university:
                fields['publisher'] = university
            if cr_subtype:
                fields['type'] = cr_subtype  # PhD / Master's, etc.

        elif item_type == 'report':
            fields['libraryCatalog'] = 'CrossRef'
            fields['extra'] = f'DOI: {doi}' if doi else ''
            institution = meta.get('publisher') or meta.get('university', '')
            if institution:
                fields['institution'] = institution
            if meta.get('pages'):
                fields['pages'] = meta['pages']

        # Fields common to most non-article types
        if item_type not in ('journalArticle', 'conferencePaper', 'preprint'):
            if meta.get('language'):
                fields['language'] = meta['language']

        # Create the Zotero item
        resp = _cookjohn_call('write_item', {
            'action': 'create',
            'itemType': item_type,
            'fields': fields,
            'creators': meta.get('authors', []),
            'tags': ['hermes-auto-import'],
        })
        if not resp.get('success'):
            return {'status': 'failed', 'error': str(resp)}

        item_key = resp['data']['itemKey']

        # ── Step 5: Attach the PDF to the new item ──
        pdf_attached = False
        if pdf_path:
            try:
                _cookjohn_call('write_item', {
                    'action': 'import',
                    'filePath': pdf_path,
                    'parentItemKey': item_key,
                    'title': f'{arxiv_id}.pdf',
                })
                pdf_attached = True
            except Exception as e:
                print(f"  [zotero] PDF attach failed for {arxiv_id}: {e}")

        # ── Step 6: Add to collection ──
        if collection_key:
            try:
                _cookjohn_call('add_items_to_collection', {
                    'collectionKey': collection_key,
                    'itemKeys': [item_key],
                })
            except Exception as e:
                print(f"  [zotero] collection add failed for {arxiv_id}: {e}")

        return {
            'status': 'added',
            'key': item_key,
            'title': meta.get('title', '')[:80],
            'type': item_type,
            'has_pdf': pdf_attached,
        }

    except Exception as e:
        return {'status': 'failed', 'error': str(e)}


# =============================================================================
# ZoteroQueue: Thread-safe serialized Zotero writes
# =============================================================================
#
# The cookjohn Zotero MCP plugin is NOT thread-safe — concurrent writes can
# cause data corruption or missed attachments. ZoteroQueue solves this by:
#   1. Accepting (arxiv_url, arxiv_id, pdf_path) tuples from any thread.
#   2. Processing them sequentially in a dedicated background thread.
#   3. Collecting results for summary reporting.
#
# Usage pattern:
#   zq = ZoteroQueue(collection_key="my_collection")
#   zq.start()
#   # ... download and submit items from worker threads ...
#   zq.submit(url, id, pdf_path)
#   zq.finish()          # drain queue, block until done
#   zq.print_summary()   # human-readable summary
# =============================================================================


class ZoteroQueue:
    """Thread-safe queue for serialized Zotero cookjohn calls.

    Workers submit (arxiv_url, arxiv_id, pdf_path) tuples via submit();
    a dedicated background thread processes them one at a time.
    The finish() method drains the queue and returns all results.

    Args:
        collection_key: Optional Zotero collection key or name.
            Resolved to a key once in the worker thread, not at construction time.
    """

    def __init__(self, collection_key: str | None = None):
        self._q: queue.Queue = queue.Queue()
        self._results: list = []
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._collection_key = collection_key

    def start(self):
        """Launch the background worker thread.

        The worker runs as a daemon thread (won't prevent program exit).
        """
        self._thread = threading.Thread(
            target=self._worker,
            daemon=True,
            name="zotero-queue"
        )
        self._thread.start()

    def _worker(self):
        """Background worker: dequeue items and process them one at a time.

        Resolves the collection key once at startup (not on every item)
        to avoid redundant cookjohn calls.
        """
        # Resolve collection key once in the worker thread
        resolved_collection = None
        if self._collection_key:
            try:
                resolved_collection = resolve_collection_key(self._collection_key)
            except Exception:
                pass

        while True:
            item = self._q.get()
            if item is None:  # sentinel — drain and exit
                break
            arxiv_url, arxiv_id, pdf_path = item
            result = zotero_add_entry(
                arxiv_url,
                arxiv_id,
                resolved_collection,
                pdf_path,
            )
            with self._lock:
                self._results.append(result)
            self._q.task_done()

    def submit(self, arxiv_url: str, arxiv_id: str, pdf_path: str | None = None):
        """Enqueue a Zotero import request.

        Non-blocking — returns immediately; the worker thread processes
        items in FIFO order.

        Args:
            arxiv_url: The arXiv abstract page URL.
            arxiv_id: The arXiv paper ID.
            pdf_path: Path to the downloaded PDF (None = metadata only).
        """
        self._q.put((arxiv_url, arxiv_id, pdf_path))

    def finish(self, timeout: float = 120) -> list:
        """Signal drain and wait for all items to be processed.

        Sends a None sentinel to terminate the worker, then joins the
        worker thread with the specified timeout.

        Args:
            timeout: Max seconds to wait for queue drain (default: 120).

        Returns:
            List of result dicts from zotero_add_entry().
        """
        self._q.put(None)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        return self._results

    def print_summary(self):
        """Print a human-readable summary of Zotero import results.

        Categorizes results by status: added, existed, attached, failed.
        Shows counts and truncated titles for each category.
        """
        added = [r for r in self._results if r['status'] == 'added']
        existed = [r for r in self._results if r['status'] == 'exists']
        attached = [r for r in self._results if r['status'] == 'attached']
        failed = [r for r in self._results if r['status'] == 'failed']

        if not self._results:
            return

        print(f"\n── Zotero import ──")
        for r in added:
            pdf = " +PDF" if r.get('has_pdf') else ""
            tp = f" [{r.get('type', '?')}]"
            print(f"  ✓ added{pdf}{tp}  {r['title'][:70]}")
        for r in attached:
            print(f"  ⇄ attach {r['title'][:70]}")
        for r in existed:
            pdf = " (has PDF)" if r.get('has_pdf') else " (no PDF)"
            print(f"  ○ exists{pdf}  {r['title'][:70]}")
        for r in failed:
            print(f"  ✗ FAIL   {r.get('error', '?')[:70]}")

        print(
            f"  added={len(added)}  "
            f"attached={len(attached)}  "
            f"existed={len(existed)}  "
            f"failed={len(failed)}"
        )


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    """Parse args, resolve inputs, download PDFs in parallel, optionally import to Zotero.

    Workflow:
      1. Parse CLI arguments (items, --input file, --out, --concurrency, etc.)
      2. Resolve each input to an arXiv ID/URL (or DOI-only fallback).
      3. Optionally start the Zotero import queue.
      4. Download PDFs in parallel using ThreadPoolExecutor.
      5. As each download completes, submit to Zotero if --zotero is set.
      6. Flush the Zotero queue and print a summary.
      7. Print PDF paths for downstream pipeline consumption.
    """
    p = argparse.ArgumentParser(
        description="DOI/arXiv → PDF downloader (parallel)"
    )

    # Positional: items (DOIs, arXiv IDs, or arXiv URLs)
    p.add_argument(
        "items",
        nargs="*",
        help="DOIs, arXiv IDs, or arXiv URLs",
    )

    # Optional: input file (one item per line)
    p.add_argument(
        "--input", "-i",
        default=None,
        help="File with one DOI/arXiv per line",
    )

    # Optional: output directory (defaults to DOWNLOAD_DIR config)
    p.add_argument(
        "--out", "-o",
        default=str(DOWNLOAD_DIR),
        help="Output directory (default: $MINERU_DOWNLOAD_DIR or ./downloads/)",
    )

    # Optional: concurrency (overrides the CONCURRENCY config/env var)
    p.add_argument(
        "--concurrency", "-c",
        type=int,
        default=CONCURRENCY,
        help=f"Max parallel downloads (default: {CONCURRENCY}, env: MINERU_CONCURRENCY)",
    )

    # Optional: Zotero import (opt-in, off by default)
    p.add_argument(
        "--zotero",
        action="store_true",
        help="After download, add paper metadata to Zotero via cookjohn (serialized)",
    )

    # Optional: Zotero collection (name or key)
    p.add_argument(
        "--zotero-collection",
        default=None,
        help="Zotero collection key or name (used with --zotero)",
    )

    # Optional: dry run (resolve only, don't download)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve only, don't download",
    )

    args = p.parse_args()

    # ── Collect inputs ──
    inputs = list(args.items)
    if args.input:
        inputs.extend(
            line.strip()
            for line in Path(args.input).read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if not inputs:
        p.print_help()
        sys.exit(0)

    # ── Resolve each input ──
    print(f"Resolving {len(inputs)} inputs ...")
    resolved = {}
    for raw in inputs:
        r = resolve_to_arxiv(raw)
        if r:
            resolved[raw] = r
            label = r.get("arxiv_url") or f"DOI:{r['doi']} (no arXiv)"
            print(f"  {raw[:50]} → {label}")
        else:
            print(f"  {raw[:50]} → FAIL (unresolvable)")

    if not resolved:
        sys.exit("Nothing resolved.")

    if args.dry_run:
        sys.exit(0)

    # ── Separate arXiv-having and DOI-only items ──
    to_download = {k: v for k, v in resolved.items() if v.get("arxiv_id")}
    no_arxiv = {k: v for k, v in resolved.items() if not v.get("arxiv_id")}

    if no_arxiv:
        print(
            f"\n{len(no_arxiv)} papers have no arXiv version "
            f"(will skip download):"
        )
        for raw, info in no_arxiv.items():
            print(f"  {raw[:60]}  DOI={info['doi']}")

    if not to_download:
        print("Nothing to download.")
        sys.exit(0)

    # ── Start Zotero queue (before downloads — runs in background) ──
    zq = None
    if args.zotero:
        zq = ZoteroQueue(collection_key=args.zotero_collection)
        zq.start()

    # ── Download in parallel — submit to Zotero as each completes ──
    dest_dir = Path(args.out)
    print(
        f"\nDownloading {len(to_download)} PDFs to {dest_dir} "
        f"(concurrency={args.concurrency}) ..."
    )
    ok = 0

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {
            ex.submit(download_arxiv, v["arxiv_id"], dest_dir): (raw, v)
            for raw, v in to_download.items()
        }
        for fut in as_completed(futures):
            raw, info = futures[fut]
            arxiv_id = info["arxiv_id"]
            arxiv_url = info["arxiv_url"]
            try:
                path = fut.result()
                if path:
                    size = path.stat().st_size / 1024
                    print(f"  ✓ {arxiv_id}  ({size:.0f} KB)")
                    ok += 1
                    if zq:
                        zq.submit(arxiv_url, arxiv_id, str(path))
                else:
                    print(f"  ✗ {arxiv_id}  download failed → Zotero metadata only")
                    if zq:
                        zq.submit(arxiv_url, arxiv_id, None)
            except Exception as e:
                print(f"  ✗ {arxiv_id}  {e} → Zotero metadata only")
                if zq:
                    zq.submit(arxiv_url, arxiv_id, None)

    # ── Also submit DOI-only items (no arXiv PDF, Zotero fetches from DOI) ──
    if zq and no_arxiv:
        for raw, info in no_arxiv.items():
            if info.get("doi"):
                doi = info["doi"]
                doi_url = f"https://doi.org/{doi}"
                # Use DOI as pseudo-id; Zotero resolves metadata from the DOI URL
                zq.submit(doi_url, doi, None)

    print(f"\n{ok}/{len(to_download)} downloaded")

    # ── Drain Zotero queue ──
    if zq:
        zq.finish()
        zq.print_summary()

    # ── Print PDF paths for piping to downstream pipeline ──
    if ok:
        print("\n# PDF paths for pipeline_async:")
        for raw, info in to_download.items():
            pdf_path = dest_dir / f"{info['arxiv_id']}.pdf"
            if pdf_path.exists():
                print(str(pdf_path))


if __name__ == "__main__":
    main()
