#!/usr/bin/env python3
"""arXiv ID → Zotero entry + PDF → OCR → RAGFlow in one shot.

Single orchestration script that wraps the existing tools into an atomic pipeline:

  1. Download PDFs (wraps pdf_downloader)
  2. Create Zotero items + attach PDFs (via cookjohn MCP)
  3. Add items to a Zotero collection
  4. OCR + upload to RAGFlow (wraps pipeline_async)
  5. Clean up intermediate files

Usage:
  # Minimum — just download + Zotero entry
  python collect_papers.py 2501.07231 2403.08868 --collection SQD_COLL_KEY

  # Full pipeline — Zotero → OCR → RAGFlow
  python collect_papers.py 2501.07231 --collection XNPAFHMB --dataset-id <id>

  # Dry run — show what would happen
  python collect_papers.py 2501.07231 --dry-run

Path convention (point 4 — no MSYS /d/ path pitfalls):
  All internal paths use Win32 native (D:/...) to avoid the double-nesting
  problem where pdf_downloader --out /d/foo → actually writes to /d/d/foo.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

# ── MCP endpoint ──────────────────────────────────────────────────────────
MCP_URL = "http://127.0.0.1:23120/mcp"

# Ensure sibling scripts are importable
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


# ═══════════════════════════════════════════════════════════════════════════
# MCP helpers
# ═══════════════════════════════════════════════════════════════════════════

def mcp_call(name: str, arguments: dict) -> dict:
    """Call a cookjohn MCP tool via JSON-RPC over HTTP."""
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
    result = resp.get("result", {})
    content = result.get("content", [])
    for item in content:
        if item.get("type") == "text":
            return json.loads(item["text"])
    return result


def mcp_raw(name: str, arguments: dict) -> dict:
    """Call MCP and return raw result dict (create_item returns nested keys)."""
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
    return resp.get("result", {}).get("content", [{}])[0].get("text", "")


# ═══════════════════════════════════════════════════════════════════════════
# arXiv metadata
# ═══════════════════════════════════════════════════════════════════════════

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


def fetch_arxiv_meta(arxiv_id: str) -> dict:
    """Fetch title, summary, authors, year from arXiv API.

    Returns dict or raises on failure.
    """
    url = f"https://export.arxiv.org/api/query?id_list={arxiv_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "collect-papers/1.0"})
    resp = urllib.request.urlopen(req, timeout=30).read().decode()
    root = ET.fromstring(resp)
    entry = root.find("a:entry", ARXIV_NS)
    if entry is None:
        raise RuntimeError(f"arXiv API returned no entry for {arxiv_id}")

    title = entry.find("a:title", ARXIV_NS)
    title = title.text.strip().replace("\n", " ").replace("  ", " ") if title is not None else arxiv_id

    summary = entry.find("a:summary", ARXIV_NS)
    summary = summary.text.strip().replace("\n", " ")[:500] if summary is not None else ""

    authors = []
    for author in entry.findall("a:author", ARXIV_NS):
        name_el = author.find("a:name", ARXIV_NS)
        if name_el is not None:
            parts = name_el.text.strip().rsplit(" ", 1)
            if len(parts) == 2:
                authors.append({"creatorType": "author", "firstName": parts[0], "lastName": parts[1]})
            else:
                authors.append({"creatorType": "author", "lastName": parts[0]})

    published = entry.find("a:published", ARXIV_NS)
    year = published.text[:4] if published is not None else ""

    return {
        "title": title,
        "summary": summary,
        "authors": authors,
        "year": year,
        "arxiv_url": f"https://arxiv.org/abs/{arxiv_id}",
        "doi": f"10.48550/arXiv.{arxiv_id}",
    }


# ═══════════════════════════════════════════════════════════════════════════
# Canonical download path (point 4 — no MSYS path ambiguity)
# ═══════════════════════════════════════════════════════════════════════════

def download_dir(name: str = "sqd") -> Path:
    """Return a Win32-native Path under downloads/<name>/.

    Use this instead of MSYS /d/ paths to avoid double-nesting.
    """
    d = Path(os.environ.get("MINERU_DOWNLOAD_DIR", "./downloads"))
    return d.resolve() / name


# ═══════════════════════════════════════════════════════════════════════════
# Step 1: Download PDFs (via pdf_downloader or direct curl)
# ═══════════════════════════════════════════════════════════════════════════

def download_pdfs(arxiv_ids: list[str], dest: Path, concurrency: int = 4) -> dict[str, Path]:
    """Download PDFs for all arXiv IDs.

    Returns {arxiv_id: Path} mapping. Uses pdf_downloader script if available,
    falls back to direct curl.
    """
    pdf_downloader = _SCRIPTS_DIR / "pdf_downloader.py"
    dest.mkdir(parents=True, exist_ok=True)

    if pdf_downloader.exists():
        cmd = [
            sys.executable, str(pdf_downloader),
            *arxiv_ids,
            "--out", str(dest),
            "--concurrency", str(concurrency),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"  ⚠ pdf_downloader stderr: {result.stderr[:300]}", file=sys.stderr)
    else:
        # Fallback: direct curl
        for aid in arxiv_ids:
            url = f"https://arxiv.org/pdf/{aid}.pdf"
            out = dest / f"{aid}.pdf"
            subprocess.run(
                ["curl", "-sSL", "-o", str(out), url],
                timeout=120,
            )

    # Collect results: support pdf_downloader's double-nested path quirk
    # It may write to D:\d\... when given --out D:\... (MSYS double-nesting).
    # Check standard location first, then fallback to double-nested.
    mapping = {}
    for aid in arxiv_ids:
        candidates = [
            dest / f"{aid}.pdf",
            dest.parent / f"{aid}.pdf",
            # Double-nested fallback (pdf_downloader MSYS path bug)
            Path(str(dest).replace("D:\\", "D:\\d\\")) / f"{aid}.pdf",
            Path(str(dest).replace("D:/", "D:/d/")) / f"{aid}.pdf",
        ]
        for c in candidates:
            if c.exists():
                mapping[aid] = c
                break

    # Print status
    for aid in arxiv_ids:
        if aid in mapping:
            sz = mapping[aid].stat().st_size / 1024
            print(f"  ✓ [{aid}] ({sz:.0f} KB)")
        else:
            print(f"  ✗ [{aid}] NOT FOUND — check {dest}")

    return mapping


# ═══════════════════════════════════════════════════════════════════════════
# Step 2: Create Zotero entry + attach PDF (atomic — point 1)
# ═══════════════════════════════════════════════════════════════════════════

def create_zotero_entry(arxiv_id: str, pdf_path: Path, collection_key: str,
                        meta: dict | None = None) -> str:
    """Create Zotero item and attach PDF in one flow.

    Returns Zotero item key.
    """
    if meta is None:
        meta = fetch_arxiv_meta(arxiv_id)

    # 2a. Create bibliographic entry
    result_json = mcp_raw("write_item", {
        "action": "create",
        "itemType": "journalArticle",
        "fields": {
            "title": meta["title"],
            "DOI": meta["doi"],
            "url": meta["arxiv_url"],
            "abstractNote": meta["summary"],
            "date": meta["year"],
            "publicationTitle": "arXiv preprint",
        },
        "creators": meta["authors"],
        "tags": ["hermes-20260714", "SQD", "from-collect"],
    })

    # Extract item key from response
    try:
        resp_data = json.loads(result_json) if isinstance(result_json, str) else result_json
    except (json.JSONDecodeError, TypeError):
        resp_data = {}

    item_key = (
        resp_data.get("itemKey")
        or resp_data.get("data", {}).get("itemKey")
        or ""
    )
    if not item_key:
        raise RuntimeError(f"write_item(create) returned no itemKey for {arxiv_id}: {result_json[:200]}")

    # write_item(create) silently drops title — fix with write_metadata
    mcp_call("write_metadata", {
        "itemKey": item_key,
        "fields": {"title": meta["title"]},
    })

    # 2b. Attach PDF
    pdf_win = str(pdf_path)  # Win32 native path from resolved Path
    mcp_call("write_item", {
        "action": "import",
        "parentItemKey": item_key,
        "filePath": pdf_win,
        "title": f"{arxiv_id}.pdf",
    })

    # 2c. Add to collection
    if collection_key:
        mcp_call("add_items_to_collection", {
            "collectionKey": collection_key,
            "itemKeys": [item_key],
        })

    return item_key


# ═══════════════════════════════════════════════════════════════════════════
# Step 3: Chain to pipeline_async (OCR → RAGFlow)
# ═══════════════════════════════════════════════════════════════════════════

def run_ocr_pipeline(pdf_paths: list[Path], dataset_id: str | None,
                     dataset_name: str, lang: str = "en",
                     concurrency: int = 4) -> int:
    """Run pipeline_async process on the PDFs.

    Returns exit code.
    """
    pipeline = _SCRIPTS_DIR / "pipeline_async.py"
    if not pipeline.exists():
        print("pipeline_async.py not found, skipping OCR")
        return 0

    cmd = [
        sys.executable, str(pipeline), "process",
        *[str(p) for p in pdf_paths],
        "--lang", lang,
        "--concurrency", str(concurrency),
        "--dataset-name", dataset_name,
    ]
    if dataset_id:
        cmd += ["--dataset-id", dataset_id]

    print(f"  Running: {' '.join(cmd)}")
    return subprocess.run(cmd).returncode


# ═══════════════════════════════════════════════════════════════════════════
# Step 4: Clean up download dir (point 3 — cleanup at end)
# ═══════════════════════════════════════════════════════════════════════════

def cleanup(download_dir: Path):
    """Remove downloaded PDFs and OCR intermediates."""
    ocr_dir = download_dir / "_ocr"
    if ocr_dir.exists():
        shutil.rmtree(ocr_dir, ignore_errors=True)
        print(f"  cleaned: {ocr_dir}")
    # Remove PDFs only, keep directory structure
    for f in download_dir.glob("*.pdf"):
        f.unlink()
        print(f"  removed: {f.name}")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="arXiv ID → Zotero entry + PDF → OCR → RAGFlow"
    )
    p.add_argument("arxiv_ids", nargs="+", help="arXiv IDs to collect")
    p.add_argument("--collection", "-c", required=True,
                   help="Zotero collection key (8 chars) to add items to")
    p.add_argument("--dataset-id", "-d", help="RAGFlow dataset ID for OCR upload")
    p.add_argument("--dataset-name", default="sqd",
                   help="Subdirectory name under _raw/ (default: sqd)")
    p.add_argument("--download-dir", help="Download directory (default: downloads/<dataset-name>/)")
    p.add_argument("--lang", default="en", help="Document language")
    p.add_argument("--concurrency", type=int, default=4, help="Download/OCR concurrency")
    p.add_argument("--dry-run", action="store_true", help="Show what would happen")
    p.add_argument("--no-cover", action="store_true",
                   help="Skip zotero coverage check (checking each ID against library is slow in batch)")
    args = p.parse_args()

    # Resolve download directory (point 4 — Win32 native path)
    if args.download_dir:
        dl_dir = Path(args.download_dir).resolve()
    else:
        dl_dir = download_dir(args.dataset_name)
    print(f"Download dir: {dl_dir}")

    # ── Dry run ────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"\nWould process {len(args.arxiv_ids)} papers:")
        for aid in args.arxiv_ids:
            print(f"  {aid}")
        print(f"  → Zotero collection: {args.collection}")
        print(f"  → RAGFlow dataset:  {args.dataset_id or '(skip)'}")
        print(f"  → Download dir:      {dl_dir}")
        return 0

    # ── Step 1: Download PDFs ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Step 1: Download {len(args.arxiv_ids)} PDFs")
    print(f"{'='*60}")
    pdfs = download_pdfs(args.arxiv_ids, dl_dir, args.concurrency)
    found = [p for p in pdfs.values()]
    if not found:
        print("No PDFs downloaded. Aborting.")
        return 1

    # ── Step 2: Create Zotero entries + attach PDFs ────────────────────
    print(f"\n{'='*60}")
    print(f"Step 2: Zotero entries → attach PDFs → collection")
    print(f"{'='*60}")
    zotero_keys = {}
    for aid in args.arxiv_ids:
        if aid not in pdfs:
            print(f"  ✗ [{aid}] no PDF, skipping Zotero entry")
            continue
        try:
            meta = fetch_arxiv_meta(aid)
            item_key = create_zotero_entry(aid, pdfs[aid], args.collection, meta)
            zotero_keys[aid] = item_key
            sz = pdfs[aid].stat().st_size / (1024 * 1024)
            print(f"  ✓ [{aid}] → [{item_key}] ({sz:.1f} MB)")
        except Exception as e:
            print(f"  ✗ [{aid}] Zotero error: {e}")

    if not zotero_keys:
        print("No Zotero entries created. Aborting.")
        return 1

    print(f"\n  Created {len(zotero_keys)} Zotero items in collection {args.collection}")

    # ── Step 3: OCR + RAGFlow ──────────────────────────────────────────
    if args.dataset_id:
        print(f"\n{'='*60}")
        print(f"Step 3: OCR → RAGFlow (dataset: {args.dataset_id})")
        print(f"{'='*60}")
        exit_code = run_ocr_pipeline(found, args.dataset_id, args.dataset_name,
                                     args.lang, args.concurrency)
        if exit_code != 0:
            print(f"  ⚠ OCR pipeline exited with code {exit_code}")
    else:
        print(f"\n  (skipping OCR+RAGFlow — no --dataset-id)")

    # ── Step 4: Cleanup (point 3) ──────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Step 4: Cleanup")
    print(f"{'='*60}")
    cleanup(dl_dir)

    print(f"\n{'='*60}")
    print(f"Done. {len(zotero_keys)} papers collected.")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
