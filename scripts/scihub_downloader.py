#!/usr/bin/env python3
"""
Sci-Hub / PMC PDF Downloader via CloakBrowser
==============================================

Generalised replacement for cloak_extract_v5.py and cloak_download_v4.py.

Modes
-----
  extract   Visit each URL/DOI, print the discovered PDF URL to stdout
            (no download).  Useful for inspection / piping into wget.
  download  Extract the PDF URL, then save the PDF to --output-dir
            (default: ./downloads/).

Input sources (choose ONE)
--------------------------
  --doi <DOI>         Single DOI (e.g. "10.1063/1.3499317")
  --doi-file <FILE>   Text file with one DOI per line (blank / # lines skipped)
  --url <URL>         Single URL + optional label, e.g.
                      "https://sci-hub.st/10.1063/1.3499317" or
                      "https://sci-hub.st/10.1063/1.3499317,MyLabel"
  --url-file <FILE>   CSV or JSON file with URL+label pairs.
                        CSV:  url,label
                        JSON: [{"url": "...", "label": "..."}]
                        If label column/key is absent, the last path segment
                        of the URL is used as the file stem.

Output directory
----------------
  --output-dir ./downloads   Where PDF files land.  Created if missing.

Dependencies
------------
  pip install cloakbrowser
  curl  (system command — only used in --mode extract for optional piping;
         --mode download uses the browser response body directly)

Examples
--------
  # Extract PDF URLs for two DOIs
  python scihub_downloader.py --mode extract --doi 10.1063/1.3499317 \
    --doi-file my_dois.txt --output-dir ./pdfs

  # Download from a DOI list
  python scihub_downloader.py --mode download --doi-file doIs.txt

  # Download from a list of arbitrary URLs (PMC, Sci-Hub, etc.)
  python scihub_downloader.py --mode download \
    --url-file pmc_papers.csv

  # Single URL with custom label
  python scihub_downloader.py --mode download \
    --url "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9380002,MyPaper"
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

# NOTE: cloakbrowser imported lazily inside main_async() so --help works
# without the optional dependency installed.


# ---------------------------------------------------------------------------
# PDF URL extraction (shared by Sci-Hub, PMC, and any site embedding PDFs)
# ---------------------------------------------------------------------------
async def extract_pdf_url(page, base_url):
    """
    Extract the direct PDF URL from the current page.

    Checks, in order:
      1. <object data="...pdf" type="application/pdf">
      2. <embed src="...pdf" type="application/pdf">
      3. <iframe> whose src contains ".pdf"

    Relative paths are resolved against base_url.
    Returns an empty string when no PDF URL is found.
    """
    url = await page.evaluate(
        """() => {
            // <object data="...pdf">
            let obj = document.querySelector('object[type="application/pdf"]');
            if (obj && obj.data) return obj.data;

            // <embed src="...pdf">
            let emb = document.querySelector('embed[type="application/pdf"]');
            if (emb && emb.src) return emb.src;

            // <iframe src="...pdf">
            let frames = document.querySelectorAll('iframe');
            for (let f of frames) {
                if (f.src && f.src.includes('.pdf')) return f.src;
            }

            return '';
        }"""
    )
    # Resolve relative URLs against the page's origin
    if url and url.startswith("/"):
        p = urlparse(base_url)
        url = f"{p.scheme}://{p.netloc}{url}"
    return url.strip()


# ---------------------------------------------------------------------------
# Input parsing helpers
# ---------------------------------------------------------------------------
def _parse_doi_file(path):
    """Read a file, yield (doi, doi) tuples.  Blank lines, full-line
    #-comments, and inline #-comments are stripped."""
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            yield line, line


def _parse_url_file(path):
    """
    Read a CSV or JSON file and yield (url, label) tuples.

    CSV:  header row expected:  url,label
          If no header or label column is absent the last URL path segment
          is used as the label.

    JSON: array of objects with at least "url" and optionally "label".
    """
    text = Path(path).read_text(encoding="utf-8").strip()
    # Try JSON first
    if text.startswith("[") or text.startswith("{"):
        items = json.loads(text)
        if isinstance(items, dict):
            items = [items]
        for item in items:
            url = item.get("url", "").strip()
            if not url:
                continue
            label = item.get("label", url.rstrip("/").split("/")[-1])
            yield url, label
        return

    # Fallback: CSV (expects header row: url,label)
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        url = (row.get("url") or row.get("URL") or "").strip()
        if not url:
            continue
        label = (
            (row.get("label") or row.get("Label") or row.get("LABEL") or "").strip()
            or url.rstrip("/").split("/")[-1]
        )
        yield url, label


def resolve_sources(args):
    """
    Return a list of (doi_or_url, label) tuples from whichever input
    arguments the user supplied.

    * --doi       → single DOI  → Sci-Hub URL is constructed automatically
    * --doi-file  → file of DOIs
    * --url       → single URL (optionally "url,label")
    * --url-file  → CSV / JSON file of URLs
    """
    items = []

    if args.doi:
        doi = args.doi.strip()
        url = f"https://sci-hub.st/{doi}"
        label = doi.split("/")[-1]
        items.append((url, label))

    if args.doi_file:
        for doi, label in _parse_doi_file(args.doi_file):
            url = f"https://sci-hub.st/{doi}"
            items.append((url, label))

    if args.url:
        # Support "url,label" inline syntax
        parts = args.url.split(",", 1)
        url = parts[0].strip()
        label = parts[1].strip() if len(parts) > 1 else url.rstrip("/").split("/")[-1]
        items.append((url, label))

    if args.url_file:
        for url, label in _parse_url_file(args.url_file):
            items.append((url, label))

    return items


# ---------------------------------------------------------------------------
# Core logic per-URL
# ---------------------------------------------------------------------------
async def process_one(ctx, url, label, output_dir, mode):
    """
    Open a page, extract the PDF URL, and either print the URL
    (mode='extract') or download the PDF to output_dir (mode='download').

    Returns True on success, False on failure.
    """
    print(f"  [{label}]", end=" ", flush=True)
    page = await ctx.new_page()

    try:
        # Navigate to the target page
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(4)  # let lazy-loaded objects/iframes appear

        # ---- Extract PDF URL ----
        pdf_url = await extract_pdf_url(page, url)
        if not pdf_url:
            print("FAIL (no PDF element found)")
            return False

        if mode == "extract":
            # Just print the URL — user can pipe through curl/wget
            print(pdf_url)
            return True

        # ---- mode == "download" ----
        resp = await page.goto(pdf_url, wait_until="domcontentloaded", timeout=30_000)
        if not resp:
            print("FAIL (no HTTP response on PDF URL)")
            return False

        body = await resp.body()
        if not body or len(body) < 5000:
            print(f"FAIL ({len(body or b'')} bytes — too small to be a PDF)")
            return False

        # Write the file
        safe_label = label.lower().replace(" ", "_").replace("/", "_")
        fpath = output_dir / f"{safe_label}.pdf"
        fpath.write_bytes(body)
        print(f"OK [{len(body) // 1024} KB] -> {fpath.stem}.pdf")
        return True

    except Exception as e:
        print(f"ERR: {e}")
        return False
    finally:
        await page.close()


# ---------------------------------------------------------------------------
# Main async entry-point
# ---------------------------------------------------------------------------
async def main_async(args):
    # Lazy-import cloakbrowser so --help and import-time don't depend on it
    from cloakbrowser import launch_context_async

    items = resolve_sources(args)
    if not items:
        print("ERROR: no DOIs or URLs provided.  Use --doi, --doi-file, --url, or --url-file.")
        sys.exit(1)

    # Ensure output directory exists (for download mode or any side-effects)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Mode:    {args.mode}")
    print(f"Sources: {len(items)} item(s)")
    print(f"Output:  {output_dir.resolve()}")
    print()

    ctx = await launch_context_async(headless=True, humanize=True)
    try:
        ok = 0
        for i, (url, label) in enumerate(items, 1):
            success = await process_one(ctx, url, label, output_dir, args.mode)
            if success:
                ok += 1
            # Brief pause between requests to avoid rate-limiting
            if i < len(items):
                await asyncio.sleep(2)
    finally:
        await ctx.close()

    print(f"\nDone: {ok}/{len(items)} succeeded")


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract or download PDFs from Sci-Hub / PMC via CloakBrowser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["extract", "download"],
        default="download",
        help="'extract' prints PDF URLs to stdout; 'download' saves PDFs (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        default="./downloads",
        help="Directory to save downloaded PDFs (default: %(default)s)",
    )

    # DOI sources
    parser.add_argument("--doi", help="Single DOI (e.g. 10.1063/1.3499317)")
    parser.add_argument(
        "--doi-file",
        type=str,
        help="File with one DOI per line (# for comments, blank lines ignored)",
    )

    # URL sources
    parser.add_argument(
        "--url",
        help=(
            "Single URL, optionally followed by a comma and a label, e.g.\n"
            '"https://sci-hub.st/10.1063/1.3499317,MyLabel"'
        ),
    )
    parser.add_argument(
        "--url-file",
        type=str,
        help=(
            "CSV or JSON file with URL+label pairs.\n"
            "CSV:  url,label\n"
            "JSON: [{\"url\": \"...\", \"label\": \"...\"}]"
        ),
    )

    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
