#!/usr/bin/env python3
"""zotero-ocr — Find OCR'd markdown for a Zotero item.

Usage:
  zotero-ocr A5CFQQQ7           # single item
  zotero-ocr A5CFQQQ7 AI6UCCVR  # batch
  zotero-ocr --pipe < keys.txt   # from stdin
  zotero-ocr A5CFQQQ7 --cat      # print full markdown

Lookup strategy:
  1. If zotero_key recorded in manifest -> O(1) hit
  2. Otherwise, compute PDF SHA256 -> manifest lookup
  3. Read and print the full.md content
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from _zotero_mcp import mcp_call

# Honor MINERU_MANIFEST_PATH env var, fall back to project-root _manifest.json
MANIFEST_PATH = Path(
    os.environ.get(
        "MINERU_MANIFEST_PATH",
        Path(__file__).resolve().parent.parent / "_manifest.json",
    )
)
# Base directory for resolving manifest-relative paths (_raw/sqd/xxx)
_BASE_DIR = MANIFEST_PATH.parent


def _load_manifest() -> dict:
    """Load manifest and build zotero_key + pdf_sha256 indices."""
    if not MANIFEST_PATH.exists():
        return {"by_zotero_key": {}, "by_pdf_sha256": {}}

    data = json.loads(MANIFEST_PATH.read_text("utf-8"))
    files = data.get("files", {})

    by_zotero = {}
    by_pdf = {}
    for path_key, entry in files.items():
        zk = entry.get("zotero_key", "")
        if zk:
            by_zotero[zk] = path_key
        psha = entry.get("pdf_sha256", "")
        if psha:
            by_pdf[psha] = path_key

    return {"by_zotero_key": by_zotero, "by_pdf_sha256": by_pdf}


def find_ocr(item_key: str) -> dict | None:
    """Find OCR markdown path for a Zotero item key.

    Returns {"path": Path, "source": "zotero_key"|"pdf_sha256"} or None.
    """
    manifest = _load_manifest()

    # Level 1: Zotero key lookup (O(1))
    path_key = manifest["by_zotero_key"].get(item_key)
    if path_key:
        md_path = _BASE_DIR / path_key / "full.md"
        if md_path.exists():
            return {"path": md_path, "source": "zotero_key"}

    # Level 2: PDF SHA256 fallback
    try:
        detail = mcp_call("get_item_details", {"itemKey": item_key, "mode": "standard"})
        for att in detail.get("attachments", []):
            if att.get("contentType") == "application/pdf":
                pdf_path = att.get("path")
                if pdf_path:
                    p = Path(pdf_path)
                    if p.exists():
                        sha = hashlib.sha256(p.read_bytes()).hexdigest()[:16]
                        path_key = manifest["by_pdf_sha256"].get(sha)
                        if path_key:
                            md_path = _BASE_DIR / path_key / "full.md"
                            if md_path.exists():
                                return {"path": md_path, "source": "pdf_sha256"}
    except Exception:
        pass

    return None


def main():
    p = argparse.ArgumentParser(description="Find OCR markdown for Zotero items")
    p.add_argument("items", nargs="*", help="Zotero item keys")
    p.add_argument("--pipe", action="store_true", help="Read keys from stdin")
    p.add_argument("--cat", action="store_true", help="Print full markdown content")
    args = p.parse_args()

    keys = list(args.items)
    if args.pipe:
        keys.extend(line.strip() for line in sys.stdin if line.strip())

    if not keys:
        p.print_help()
        sys.exit(1)

    for item_key in keys:
        result = find_ocr(item_key)
        if result:
            md = result["path"]
            print(f"[{item_key}] {md} ({md.stat().st_size} bytes, via {result['source']})")
            if args.cat:
                print(md.read_text("utf-8"))
        else:
            print(f"[{item_key}] NOT FOUND")


if __name__ == "__main__":
    main()
