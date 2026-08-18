#!/usr/bin/env python3
"""Resolve Zotero item keys to local PDF attachment paths.

Usage:
    python zotero_pdfpath.py <KEY> [KEY...]
    python zotero_pdfpath.py --collection <KEY> [--json]
    python zotero_pdfpath.py <KEY> [KEY...] --json

Prints one line per key in text mode:  KEY<TAB>path  or  KEY<TAB>NOT_FOUND.
Exit code 0 if every key resolved to a PDF, 1 if any did not
(missing keys are also printed to stderr).

Reuses the transport in _zotero_mcp.py (cookjohn MCP :23120 + local API
:23119) — no transport logic is duplicated here.
"""

import argparse
import json
import sys

from _zotero_mcp import local_api, resolve_pdf_path, resolve_pdf_path_local


def collection_keys(collection_key: str) -> list[str]:
    """List item keys in a collection via the read-only local API."""
    raw = local_api(f"/collections/{collection_key}/items")
    if isinstance(raw, dict):
        raw = raw.get("items", [])
    keys = []
    for item in raw if isinstance(raw, list) else []:
        k = item.get("key")
        if k:
            keys.append(k)
    return keys


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Resolve Zotero item keys to local PDF attachment paths."
    )
    parser.add_argument("keys", nargs="*", metavar="KEY",
                        help="8-char Zotero item key(s)")
    parser.add_argument("--collection", metavar="KEY",
                        help="resolve every item in the given collection instead")
    parser.add_argument("--json", action="store_true",
                        help="machine-readable JSON output "
                             "[{\"key\": ..., \"path\": path-or-null, \"exists\": bool}]")
    args = parser.parse_args()

    keys = list(args.keys)
    if args.collection:
        keys = collection_keys(args.collection)
    if not keys:
        parser.error("no item keys given (positional args or --collection)")

    results = []
    missing = []
    for key in keys:
        # local API first (MCP-free: children endpoint + prefs.js base dirs),
        # cookjohn MCP as fallback
        path = resolve_pdf_path_local(key) or resolve_pdf_path(key)
        results.append({"key": key, "path": path, "exists": path is not None})
        if path is None:
            missing.append(key)

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        for r in results:
            print(f"{r['key']}\t{r['path'] if r['path'] else 'NOT_FOUND'}")

    for k in missing:
        print(f"MISSING: {k}", file=sys.stderr)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
