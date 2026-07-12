"""Deposit service — single entry point for all pipeline file placement.

PDF  → <MINERU_PDF_LIBRARY>/ + Zotero (via cookjohn MCP)
MD   → <MINERU_RAW_DIR>/<dataset>/ + _manifest.json recording

Manifest is path-insensitive — lookup by SHA256, path key auto-updates
if file moves between datasets.

PDF source priority (deposit.py does NOT download — it receives already-downloaded PDFs):
  1. Zotero attachment (already in library) → fastest
  2. <MINERU_PDF_LIBRARY>/ scan by author+year
  3. arXiv direct download: curl -L -o paper.pdf https://arxiv.org/pdf/<id>.pdf
  4. Unpaywall OA check: GET https://api.unpaywall.org/v2/{doi}?email=...
  5. Sci-Hub via CloakBrowser: extract <object data="...pdf"> → curl
  6. Manual download

Grilling Decisions (2026-07-08):
- PDF validation: warning if <50KB + magic bytes %PDF-
- Manifest: SHA256 scan + lazy path-key update, no migration needed
- Lookup: O(1) via in-memory index built at first access
"""

import hashlib
import json
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ── Configuration (overridable via environment variables) ─────────

# Directory where PDFs are stored.
# Override with MINERU_PDF_LIBRARY env var. Defaults to "./pdfs/".
PDF_LIBRARY = Path(os.environ.get("MINERU_PDF_LIBRARY", "./pdfs/"))

# Directory where processed Markdown output is deposited.
# Override with MINERU_RAW_DIR env var. Defaults to "./_raw/".
RAW_DIR = Path(os.environ.get("MINERU_RAW_DIR", "./_raw/"))

# Path to the manifest JSON file (SHA256-indexed, path-insensitive).
# Defaults to "./_manifest.json" in the current working directory.
MANIFEST_PATH = Path(os.environ.get("MINERU_MANIFEST_PATH", "./_manifest.json"))

# URL of the local cookjohn MCP server (Zotero integration).
COOKJOHN_URL = "http://127.0.0.1:23120/mcp"

PDF_MIN_SIZE = 50 * 1024  # 50KB — warn below this
PDF_MAGIC = b"%PDF-"


# ── Manifest (path-insensitive, SHA256-indexed) ──────────────────


class Manifest:
    """Path-insensitive manifest of OCR'd papers. SHA256 is the primary key;
    path in the JSON key is just metadata, auto-updated on lookup if stale.
    """

    def __init__(self, path: Path = MANIFEST_PATH):
        self._path = path
        self._data: dict = {"version": 1, "files": {}}
        self._by_sha256: dict[str, str] = {}     # md_sha256 → JSON key
        self._by_pdf_sha256: dict[str, str] = {}  # pdf_sha256 → JSON key
        self._loaded = False

    def _ensure_loaded(self):
        if self._loaded:
            return
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._data = {"version": 1, "files": {}}
        # Build SHA256 indices
        self._by_sha256.clear()
        self._by_pdf_sha256.clear()
        for key, entry in self._data.get("files", {}).items():
            sha = entry.get("sha256", "")
            if sha:
                self._by_sha256[sha] = key
            pdf_sha = entry.get("pdf_sha256", "")
            if pdf_sha:
                self._by_pdf_sha256[pdf_sha] = key
        self._loaded = True

    def lookup(self, sha256: str) -> dict | None:
        """Find entry by SHA256. Returns the entry + its current JSON key.

        If the path key has changed (file moved), auto-updates the key.
        Returns None if not found.
        """
        self._ensure_loaded()
        key = self._by_sha256.get(sha256)
        if key is None:
            return None
        entry = self._data["files"].get(key)
        if entry is None:
            return None
        return {"_key": key, **entry}

    def lookup_pdf(self, pdf_sha256: str) -> dict | None:
        """Find entry by PDF SHA256. Returns the MD path + metadata, or None."""
        self._ensure_loaded()
        key = self._by_pdf_sha256.get(pdf_sha256)
        if key is None:
            return None
        entry = self._data["files"].get(key)
        if entry is None:
            return None
        return {"_key": key, **entry}

    def record(self, dataset: str, rel_path: str, sha256: str,
               title: str = "", authors: str = "", source_pdf: str = "",
               zotero_key: str = "", pdf_sha256: str = ""):
        """Add or update a manifest entry. Auto-updates key if same SHA256
        appears under a different path (file moved across datasets)."""
        self._ensure_loaded()
        files = self._data.setdefault("files", {})

        # If this SHA256 already exists under a different key → update key
        old_key = self._by_sha256.get(sha256)
        if old_key and old_key != rel_path:
            entry = files.pop(old_key, {})
            # Merge old metadata into new
            if not title and entry.get("title"):
                title = entry["title"]
            if not source_pdf and entry.get("source_pdf"):
                source_pdf = entry["source_pdf"]

        entry = {
            "sha256": sha256,
            "pdf_sha256": pdf_sha256,
            "title": title,
            "authors": authors,
            "source_pdf": source_pdf,
            "zotero_key": zotero_key,
            "ocr_date": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "ragflow": None,
        }
        files[rel_path] = entry
        self._by_sha256[sha256] = rel_path
        if pdf_sha256:
            self._by_pdf_sha256[pdf_sha256] = rel_path
        self._save()

    def _save(self):
        tmp = self._path.with_suffix(".tmp")
        self._data.setdefault("version", 1)
        self._data.setdefault("created", self._data.get("created",
                              time.strftime("%Y-%m-%d")))
        tmp.write_text(json.dumps(self._data, indent=2, ensure_ascii=False),
                       encoding="utf-8")
        tmp.replace(self._path)

    def count(self) -> int:
        self._ensure_loaded()
        return len(self._data.get("files", {}))


# Module-level singleton
_manifest = Manifest()


# ── Exceptions ──────────────────────────────────────────────────


class DepositError(Exception):
    """Deposit operation failed (path validation, write error, etc.)."""
    pass


@dataclass
class DepositResult:
    canonical_path: Path
    zotero_key: str = ""
    error: str = ""
    warnings: list[str] = field(default_factory=list)


# ── Helpers ─────────────────────────────────────────────────────


def _sha256_file(path: Path) -> str:
    """Compute truncated SHA256 (16 chars) of file content."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _extract_arxiv_id(name: str) -> str:
    """Extract arXiv ID from filename like '2205.01121' or 'quant-ph_9707021'."""
    m = re.search(r"(\d{4}\.\d{4,5})", name)
    if m:
        return m.group(1)
    m = re.search(r"([a-z\-]+[./_]\d{7})", name)
    if m:
        return m.group(1).replace("/", "_")
    return ""


def _validate_pdf(path: Path) -> list[str]:
    """Validate PDF file. Returns list of warnings (empty = OK).
    Raises DepositError on fatal issues (wrong magic bytes)."""
    warnings = []
    size = path.stat().st_size

    # Magic bytes
    with open(path, "rb") as f:
        header = f.read(5)
    if not header.startswith(PDF_MAGIC):
        raise DepositError(
            f"Not a valid PDF (magic bytes missing): {path}\n"
            f"  First bytes: {header!r}"
        )

    # Size warning
    if size < PDF_MIN_SIZE:
        warnings.append(
            f"PDF is only {size:,} bytes (threshold: {PDF_MIN_SIZE:,} bytes). "
            "It may be corrupted or incomplete."
        )

    return warnings


def _canonical_pdf_name(src: Path, metadata: dict | None) -> str:
    """Derive canonical filename from metadata or source."""
    if metadata:
        if arxiv_id := metadata.get("arxiv_id"):
            return f"{arxiv_id.replace('/', '_')}.pdf"
        author = metadata.get("first_author", "")
        year = metadata.get("year", "")
        if author:
            return f"{author}_{year}.pdf" if year else f"{author}.pdf"
    return src.name.replace(" ", "_")


# ── Public API ──────────────────────────────────────────────────


def deposit_pdf(src: Path, metadata: dict | None = None) -> DepositResult:
    """Copy PDF to <MINERU_PDF_LIBRARY>/ with canonical naming.

    Validates PDF (magic bytes + size warning), then copies to library.
    Returns canonical path.
    """
    src = Path(src)
    if not src.exists():
        raise DepositError(f"Source PDF not found: {src}")

    # Validate
    warnings = _validate_pdf(src)

    if not PDF_LIBRARY.exists():
        raise DepositError(f"PDF_LIBRARY not found: {PDF_LIBRARY}")

    name = _canonical_pdf_name(src, metadata)
    dest = PDF_LIBRARY / name

    if dest.exists():
        existing_sha = _sha256_file(dest)
        new_sha = _sha256_file(src)
        if existing_sha == new_sha:
            return DepositResult(canonical_path=dest, warnings=warnings)
        stem, ext = os.path.splitext(name)
        dest = PDF_LIBRARY / f"{stem}_1{ext}"

    shutil.copy2(src, dest)
    return DepositResult(canonical_path=dest, warnings=warnings)


# ── Zotero Integration (cookjohn MCP) ───────────────────────────


def _zotero_create_parent(metadata: dict) -> str | None:
    """Create a minimal Zotero item from metadata. Returns item key or None."""
    title = metadata.get("title", "")
    if not title:
        return None

    import requests as _requests
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "write_item",
            "arguments": {
                "action": "create",
                "itemType": "journalArticle",
                "title": title,
                "fields": {
                    "url": metadata.get("url", ""),
                    "DOI": metadata.get("doi", ""),
                    "date": metadata.get("year", ""),
                },
            },
        },
        "id": 1,
    }
    try:
        r = _requests.post(COOKJOHN_URL, json=payload, timeout=15)
        data = r.json()
        result = data.get("result", {}).get("content", [{}])[0].get("text", "{}")
        parsed = json.loads(result)
        return parsed.get("key", "")
    except Exception:
        return None


def _zotero_find_by_arxiv(arxiv_id: str) -> str | None:
    """Search Zotero for existing item by arXiv ID. Returns item key or None."""
    if not arxiv_id:
        return None

    import requests as _requests
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "search_library",
            "arguments": {"q": arxiv_id, "limit": 3},
        },
        "id": 1,
    }
    try:
        r = _requests.post(COOKJOHN_URL, json=payload, timeout=15)
        data = r.json()
        result = data.get("result", {}).get("content", [{}])[0].get("text", "{}")
        parsed = json.loads(result)
        for item in parsed.get("results", []):
            url = item.get("url", "")
            if arxiv_id in url:
                return item.get("key")
            doi = item.get("DOI", "")
            if f"arXiv.{arxiv_id}" in doi:
                return item.get("key")
        # Fallback: return first result if any
        if parsed.get("results"):
            return parsed["results"][0].get("key")
    except Exception:
        pass
    return None


def _zotero_import_pdf(pdf_path: Path, parent_key: str, title: str = "") -> str | None:
    """Import PDF as attachment to Zotero item. Returns attachment key."""
    import requests as _requests
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "write_item",
            "arguments": {
                "action": "import",
                "filePath": str(pdf_path.absolute()),
                "parentItemKey": parent_key,
                "title": title or pdf_path.stem,
            },
        },
        "id": 1,
    }
    try:
        r = _requests.post(COOKJOHN_URL, json=payload, timeout=30)
        data = r.json()
        result = data.get("result", {}).get("content", [{}])[0].get("text", "{}")
        parsed = json.loads(result)
        return parsed.get("key", "")
    except Exception:
        return None


def deposit_pdf_zotero(src: Path, metadata: dict | None = None,
                       parent_key: str = "") -> DepositResult:
    """Copy PDF to library AND attach to Zotero.

    On success, returns canonical_path + zotero_key.
    On Zotero failure, still copies PDF to library and returns error in result.
    """
    meta = metadata or {}

    # Step 1: copy PDF to library (always succeeds unless validation fails)
    result = deposit_pdf(src, meta)

    if result.error and result.error != "":
        return result  # validation failure

    # Step 2: resolve Zotero parent item
    pk = parent_key
    if not pk:
        arxiv_id = meta.get("arxiv_id", "")
        if arxiv_id:
            pk = _zotero_find_by_arxiv(arxiv_id) or ""
        if not pk:
            pk = _zotero_create_parent(meta) or ""

    if not pk:
        result.error = "zotero: no parent item found or created"
        return result

    # Step 3: import PDF as attachment
    title = meta.get("title", result.canonical_path.stem)
    zkey = _zotero_import_pdf(result.canonical_path, pk, title)
    if zkey:
        result.zotero_key = zkey
    else:
        result.error = f"zotero: import failed (parent={pk})"

    return result


def deposit_md(src: Path, dataset: str, metadata: dict | None = None) -> DepositResult:
    """Move MD file to <MINERU_RAW_DIR>/<dataset>/<arxiv_id>/full.md.

    Also records the entry in _manifest.json for dedup.
    The source file is MOVED (consumed), not copied.
    """
    src = Path(src)
    if not src.exists():
        raise DepositError(f"Source MD not found: {src}")

    if not dataset or not dataset.strip():
        raise DepositError("dataset name is required (cannot be empty)")

    dataset = dataset.strip().lower().replace(" ", "-")
    dataset_dir = RAW_DIR / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Extract arXiv ID for subdirectory naming
    arxiv_id = _extract_arxiv_id(src.stem)
    subdir_name = arxiv_id or src.stem
    subdir = dataset_dir / subdir_name
    subdir.mkdir(exist_ok=True)

    dest = subdir / "full.md"

    # Pre-compute SHA256 for dedup checks
    new_sha = _sha256_file(src)

    # Cross-stem dedup within same dataset: check manifest for same
    # content already deposited under a different subdirectory name.
    # Only dedup within the same dataset — a paper can belong to multiple datasets.
    existing_entry = _manifest.lookup(new_sha)
    if existing_entry:
        existing_key = existing_entry["_key"]  # e.g., "_raw/theory/2205.01121"
        existing_dataset = existing_key.split("/")[1] if "/" in existing_key else ""
        if existing_dataset == dataset:
            existing_path = RAW_DIR.parent / existing_key / "full.md"
            if existing_path.exists():
                src.unlink(missing_ok=True)
                return DepositResult(canonical_path=existing_path, error="")

    # Same-path dedup: destination already exists with matching content
    if dest.exists():
        existing_sha = _sha256_file(dest)
        if existing_sha == new_sha:
            src.unlink(missing_ok=True)
            return DepositResult(canonical_path=dest, error="")

    shutil.move(str(src), str(dest))

    # Record in manifest (path-insensitive)
    rel_path = f"_raw/{dataset}/{subdir_name}"
    _manifest.record(
        dataset=dataset,
        rel_path=rel_path,
        sha256=new_sha,
        title=metadata.get("title", "") if metadata else "",
        authors=metadata.get("authors", "") if metadata else "",
        source_pdf=metadata.get("source_pdf", "") if metadata else "",
        zotero_key=metadata.get("zotero_key", "") if metadata else "",
        pdf_sha256=metadata.get("pdf_sha256", "") if metadata else "",
    )

    return DepositResult(canonical_path=dest)


def deposit_validate(dataset: str) -> dict:
    """Count artifacts in their canonical locations for a dataset."""
    result = {
        "pdfs_in_library": 0,
        "mds_in_raw": 0,
        "manifest_entries": 0,
        "dataset_dir": str(RAW_DIR / dataset),
    }
    dataset_dir = RAW_DIR / dataset
    if dataset_dir.exists():
        result["mds_in_raw"] = len(list(dataset_dir.rglob("full.md")))
    result["manifest_entries"] = _manifest.count()
    return result


def manifest_lookup_by_sha256(sha256: str) -> dict | None:
    """Path-insensitive manifest lookup. Returns entry or None."""
    return _manifest.lookup(sha256)


def manifest_lookup_by_file(path: Path) -> dict | None:
    """Look up a file in the manifest by computing its SHA256."""
    if not path.exists():
        return None
    sha = _sha256_file(path)
    return _manifest.lookup(sha)


# ── CLI (for standalone testing) ────────────────────────────────

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Deposit service CLI")
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("pdf", help="Deposit a PDF")
    p.add_argument("src", help="Source PDF path")
    p.add_argument("--arxiv-id", help="arXiv ID")

    p = sub.add_parser("md", help="Deposit a Markdown file")
    p.add_argument("src", help="Source MD path")
    p.add_argument("--dataset", required=True, help="Dataset name")

    p = sub.add_parser("validate", help="Validate dataset")
    p.add_argument("--dataset", required=True)

    p = sub.add_parser("manifest-lookup", help="Look up file in manifest")
    p.add_argument("path", help="File path to look up by SHA256")

    args = ap.parse_args()

    if args.cmd == "pdf":
        meta = {"arxiv_id": args.arxiv_id} if args.arxiv_id else {}
        r = deposit_pdf(Path(args.src), meta)
        print(f"→ {r.canonical_path}")
        for w in r.warnings:
            print(f"⚠  {w}")
        if r.error:
            print(f"✗ {r.error}")

    elif args.cmd == "md":
        r = deposit_md(Path(args.src), args.dataset)
        print(f"→ {r.canonical_path}")
        print(f"  manifest entries: {_manifest.count()}")

    elif args.cmd == "validate":
        r = deposit_validate(args.dataset)
        for k, v in r.items():
            print(f"  {k}: {v}")

    elif args.cmd == "manifest-lookup":
        entry = manifest_lookup_by_file(Path(args.path))
        if entry:
            print(json.dumps(entry, indent=2, ensure_ascii=False))
        else:
            print("Not found in manifest")


if __name__ == "__main__":
    main()
