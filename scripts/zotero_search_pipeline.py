#!/usr/bin/env python3
"""
zotero-search-pipeline — Search Zotero by keyword → OCR → RAGFlow.

Usage:
  zotero-search-pipeline --query "quantum machine learning" --ragflow-dataset <id>
  zotero-search-pipeline --query "topological insulator" --ragflow-dataset <id> --collection "cond-mat"
  zotero-search-pipeline --query "machine learning" --ragflow-dataset <id> --dry-run

Workflow:
  1. Search Zotero library for items matching the query (via cookjohn MCP)
  2. Extract PDF attachment paths from matching items
  3. Feed PDFs into pipeline_async (OCR + RAGFlow upload with SHA256 dedup)

All internal paths are Win32-native (no MSYS /d/ path pitfalls).
"""

import argparse
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────

COOKJOHN_MCP = "http://127.0.0.1:23120/mcp"  # cookjohn default endpoint
_SCRIPTS_DIR = Path(__file__).resolve().parent

# ── MCP Transport (same pattern as zotero_linked_pipeline.py) ────────────


def _mcp_call(tool_name: str, arguments: dict, timeout: int = 30) -> dict:
    """Call a cookjohn MCP tool via JSON-RPC over HTTP.

    Returns the parsed JSON result from the MCP response envelope.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    req = urllib.request.Request(
        COOKJOHN_MCP,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    result = resp.get("result", {})
    # MCP wraps data in content[0].text
    content = result.get("content", [])
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            return json.loads(item["text"])
    return result


# ── Zotero helpers ──────────────────────────────────────────────────────


def search_items(query: str, limit: int = 50) -> list[dict]:
    """Search Zotero library by keyword. Returns list of item dicts."""
    result = _mcp_call("search_library", {"q": query, "limit": limit, "mode": "complete"})
    if isinstance(result, dict):
        return result.get("results", [])
    if isinstance(result, list):
        return result
    return []


def resolve_pdf_path(item_key: str) -> Path | None:
    """Return the filesystem path of the first PDF attachment, or None.

    get_item_details returns the full Win32 path in attachments[].path,
    e.g. C:\\Users\\zhang\\Zotero\\storage\\XXXXXX\\file.pdf.
    """
    try:
        detail = _mcp_call("get_item_details", {"itemKey": item_key, "mode": "standard"})
        for att in detail.get("attachments", []):
            if att.get("contentType") == "application/pdf":
                path_str = att.get("path")
                if path_str:
                    p = Path(path_str)
                    if p.exists():
                        return p
    except Exception:
        pass
    return None


def collect_pdfs(items: list[dict]) -> list[dict]:
    """Iterate items and extract PDF paths.

    Returns list of dicts: {title, path (Path), item_key}.
    """
    pdfs = []
    for item in items:
        key = item.get("key", "")
        title = item.get("title", "Untitled")
        path = resolve_pdf_path(key)
        if path:
            pdfs.append({"title": title, "path": path, "item_key": key})
    return pdfs


# ── CLI ──────────────────────────────────────────────────────────────────


def main():
    p = argparse.ArgumentParser(
        description="Search Zotero by keyword → collect PDFs → OCR → RAGFlow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--query", "-q", required=True, help="Zotero library search query")
    p.add_argument(
        "--ragflow-dataset", "-d", required=True,
        help="RAGFlow dataset ID for OCR upload",
    )
    p.add_argument(
        "--collection", "-c",
        help="Optional: restrict search to a specific Zotero collection (name or key)",
    )
    p.add_argument("--lang", default="en", help="Document language (default: en)")
    p.add_argument("--concurrency", type=int, default=4, help="OCR concurrency (default: 4)")
    p.add_argument("--dry-run", action="store_true", help="Show what would be done, don't run")
    args = p.parse_args()

    # ── Phase 1: Search Zotero ──────────────────────────────────────────
    print(f"{'='*60}")
    print(f"Phase 1: Search Zotero")
    print(f"{'='*60}")
    print(f"  Query: {args.query}")
    if args.collection:
        print(f"  Collection scope: {args.collection}")

    items = search_items(args.query)
    print(f"  → {len(items)} matching items found")

    if not items:
        print("  No items found. Exiting.")
        return

    print(f"\n  First {min(5, len(items))} items:")
    for i, item in enumerate(items[:5]):
        title = item.get("title", "?")[:80]
        creators = item.get("creators", "") or ""
        print(f"    [{i+1}] {title}")
        if creators:
            print(f"         ({creators})")
    if len(items) > 5:
        print(f"    ... and {len(items) - 5} more")

    # ── Phase 2: Collect PDFs ───────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Phase 2: Collect PDF attachments")
    print(f"{'='*60}")

    pdfs = collect_pdfs(items)
    print(f"  → {len(pdfs)} items have PDFs on disk")

    if not pdfs:
        print("  No PDFs found. Exiting.")
        return

    for pdf in pdfs:
        size_mb = pdf["path"].stat().st_size / (1024 * 1024)
        print(f"  📄 {pdf['title'][:70]}")
        print(f"     {pdf['path']} ({size_mb:.1f} MB)")

    # ── Phase 3: Dry run or execute ────────────────────────────────────
    if args.dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN — would process {len(pdfs)} PDFs:")
        print(f"  pipeline_async process <{len(pdfs)} PDFs> --dataset-id {args.ragflow_dataset}")
        print(f"{'='*60}")
        return

    print(f"\n{'='*60}")
    print(f"Phase 3: OCR → RAGFlow ({len(pdfs)} PDFs)")
    print(f"{'='*60}")

    pipeline = _SCRIPTS_DIR / "pipeline_async.py"
    cmd = [
        sys.executable, str(pipeline), "process",
        *[str(p["path"]) for p in pdfs],
        "--dataset-id", args.ragflow_dataset,
        "--lang", args.lang,
        "--concurrency", str(args.concurrency),
    ]
    print(f"  Running: {' '.join(cmd)}")
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
