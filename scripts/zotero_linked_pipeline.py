#!/usr/bin/env python3
"""
Zotero Linked-File → MinerU Pipeline Bridge
=============================================

Finds all linked-file PDFs (linkMode=2) in a Zotero collection,
verifies they exist on disk, and feeds them into **pipeline_async.py**
for OCR / RAGFlow ingestion.

**Prerequisites**
-----------------
1. **Zotero desktop** must be running (the cookjohn plugin talks to the
   local Zotero application via its built-in HTTP server).
2. **cookjohn** — a Zotero MCP plugin — must be installed and serving on
   ``http://127.0.0.1:23120/mcp``.
   - Repository: https://github.com/cookjohn/ZoteroMCP
   - Install via Zotero's "Install Add-on From File…" (Tools → Add-ons gear
     menu). After installation a local MCP endpoint becomes available.
3. **Requests** library: ``pip install requests`` (standard with MinerU env).

**Usage**
---------
.. code-block:: bash

    python zotero_linked_pipeline.py \\
        --collection "<key_or_name>" \\
        --dataset-id "<ragflow_dataset_id>" \\
        [--lang en] [--concurrency 4] [--dry-run]

**Arguments**
-------------
- ``--collection`` / ``-c`` (required) — 8-char Zotero collection key, or
  human-readable collection name (case-insensitive exact match).
- ``--dataset-id`` / ``-d`` (required) — RAGFlow dataset ID to upload to.
- ``--lang`` — Document language (default: ``en``).
- ``--concurrency`` — Parallel upload count (default: ``4``).
- ``--dataset-name`` — Subdirectory name under ``_raw/`` (optional).
- ``--pages`` — Page range e.g. ``"1-20"`` (optional).
- ``--dry-run`` — List linked PDFs without running the pipeline.

**Workflow**
------------
1. Resolve the collection key (accepts key or name).
2. Query Zotero via cookjohn MCP for all items in the collection.
3. Filter to **linked-file PDFs** only (linkMode=2, not stored-attachment
   copies).
4. Verify each PDF exists on the local filesystem; warn for missing files.
5. Pass surviving PDF paths to ``pipeline_async.py process …``.

**Architecture Note**
---------------------
This script avoids hardcoding any personal data. All paths, collection
identifiers, and dataset IDs come from CLI arguments, making it safe to
commit to a shared repository.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import requests  # pip install requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The cookjohn MCP plugin serves JSON-RPC on this standard localhost URL.
# Zotero desktop MUST be running with the plugin installed and active.
COOKJOHN_URL = "http://127.0.0.1:23120/mcp"

# Resolve the directory this script lives in, then insert it at the front of
# sys.path so that sibling modules (notably pipeline_async.py) can be imported
# or invoked by relative path without a separate pip install.
_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)


# ---------------------------------------------------------------------------
# MCP transport helpers
# ---------------------------------------------------------------------------


def _rpc(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Call the cookjohn MCP plugin via JSON-RPC 2.0.

    Parameters
    ----------
    method : str
        The MCP method to invoke, e.g. ``"tools/call"``.
    params : dict
        Parameters passed to the method. For tool calls this contains
        ``{"name": "<tool_name>", "arguments": {…}}``.

    Returns
    -------
    dict
        The ``result`` field of the JSON-RPC response (already unwrapped
        from the outer envelope). Raises ``RuntimeError`` on error.

    Raises
    ------
    requests.RequestException
        On HTTP-level failures (connection refused, timeout, etc.).
    RuntimeError
        If the JSON-RPC response contains an ``"error"`` field.

    Notes
    -----
    The cookjohn MCP plugin is not a fully standard MCP server — it speaks
    JSON-RPC 2.0 over HTTP, *not* the official MCP transport (stdio or SSE).
    This is a lightweight adapter that matches the plugin's actual wire format.
    """
    resp = requests.post(
        COOKJOHN_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    resp.raise_for_status()  # raise on HTTP 4xx/5xx
    body: dict[str, Any] = resp.json()

    if "error" in body:
        raise RuntimeError(f"MCP error: {body['error']}")

    # Return the result payload already unwrapped from the JSON-RPC envelope.
    return body["result"]


def _parse_result(result: Any) -> list[dict[str, Any]]:
    """Unpack the actual data from a cookjohn MCP tool-call response.

    cookjohn wraps its result payload inside a ``content`` array structured
    like the OpenAI/Anthropic tool-call format:

    .. code-block:: json

        [
            {"type": "text", "text": "[{…}, {…}]"}
        ]

    The inner ``text`` value is a **JSON-encoded string** of the real data.
    This function unwraps that nesting, falling back through several common
    shapes so callers always get a plain Python list of dicts.

    Resolution order
    ----------------
    1. **Standard MCP wrapping** — ``result`` is a ``list[dict]`` whose
       first element has ``type == "text"``.  Load ``text`` as JSON.
    2. **Raw JSON string** — ``result`` is itself a JSON string.
    3. **Already a list** — return as-is.
    4. **Anything else** — return empty list.

    Returns
    -------
    list[dict[str, Any]]
        The unwrapped payload, always a list of dicts.
    """
    # Shape 1: OpenAI/MCP-style content list, e.g.
    #   [{"type": "text", "text": "[{...}]"}]
    if isinstance(result, list) and len(result) > 0:
        item = result[0]
        if isinstance(item, dict) and item.get("type") == "text":
            return json.loads(item["text"])

    # Shape 2: result is a bare JSON string
    if isinstance(result, str):
        return json.loads(result)

    # Shape 3: result is already a parsed list
    if isinstance(result, list):
        return result

    # Fallback: nothing to unwrap
    return []


# ---------------------------------------------------------------------------
# Zotero collection helpers
# ---------------------------------------------------------------------------


def resolve_collection_key(name_or_key: str) -> str:
    """Resolve a Zotero collection identifier to its 8-character key.

    Accepts either:
    - An **8-character alphanumeric key** (fast path — verified via a single
      ``get_collection_details`` call).
    - A **human-readable collection name** (case-insensitive exact match
      against all collections matching the search term).

    Parameters
    ----------
    name_or_key : str
        Collection key or name to resolve.

    Returns
    -------
    str
        The 8-character Zotero collection key.

    Raises
    ------
    ValueError
        If the collection cannot be found by key or name.
    """
    # --- Fast path: treat as a raw key first ---
    # Zotero collection keys are always 8 alphanumeric characters.
    if len(name_or_key) == 8 and name_or_key.isalnum():
        try:
            _rpc(
                "tools/call",
                {
                    "name": "get_collection_details",
                    "arguments": {"collectionKey": name_or_key},
                },
            )
            # No exception → key exists and is valid.
            return name_or_key
        except Exception:
            # Fall through to name-search below.
            pass

    # --- Slow path: search by name ---
    result: dict[str, Any] = _rpc(
        "tools/call",
        {
            "name": "search_collections",
            "arguments": {"q": name_or_key},
        },
    )
    parsed: list[dict[str, Any]] = _parse_result(result.get("content", result))

    # Case-insensitive exact match on the collection name.
    for col in parsed:
        if col.get("name", "").lower() == name_or_key.lower():
            return col["key"]

    raise ValueError(f"Collection not found: {name_or_key}")


def get_linked_pdfs(collection_key: str) -> list[dict[str, Any]]:
    """Retrieve all linked-file PDFs from a Zotero collection.

    A "linked file" in Zotero (``linkMode == 2``) is a reference to a PDF
    that lives on the local filesystem *outside* Zotero's internal storage.
    Zotero stores only the path to the original file.

    This function:
    1. Fetches all items in the collection (up to 200).
    2. Iterates over each item's attachments.
    3. Filters to PDFs with ``linkMode == 2``.
    4. Skips PDFs whose file no longer exists on disk (with a warning).

    Parameters
    ----------
    collection_key : str
        The 8-character Zotero collection key.

    Returns
    -------
    list[dict[str, Any]]
        Each dict has keys:
        - ``item_key`` (str): Zotero item key.
        - ``title`` (str): Item title (falls back to ``"Untitled"``).
        - ``path`` (str): Absolute filesystem path to the PDF.
        - ``filename`` (str): Attachment filename.

    Notes
    -----
    Missing files are printed to stderr and silently excluded from the
    returned list. This is intentional — the downstream pipeline will fail
    if given a non-existent path, so we pre-filter for robustness.
    """
    result: dict[str, Any] = _rpc(
        "tools/call",
        {
            "name": "get_collection_items",
            "arguments": {"collectionKey": collection_key, "limit": 200},
        },
    )
    items: list[dict[str, Any]] = _parse_result(result.get("content", result))

    linked: list[dict[str, Any]] = []

    for item in items:
        # Walk all attachments on this item.
        for att in item.get("attachments", []):
            # linkMode=2 means "linked file" (stored by path on local FS).
            is_linked_pdf = (
                att.get("linkMode") == 2
                and att.get("contentType") == "application/pdf"
            )
            if not is_linked_pdf:
                continue

            path: Optional[str] = att.get("path")

            if path and Path(path).exists():
                linked.append(
                    {
                        "item_key": item["key"],
                        "title": item.get("title", "Untitled"),
                        "path": path,
                        "filename": att.get("filename", ""),
                    }
                )
            elif path:
                # Warn but don't fail — other PDFs in the collection may
                # still be valid.
                print(f"  ⚠ Missing on disk: {path}", file=sys.stderr)

    return linked


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse CLI arguments, resolve collection, gather PDFs, run pipeline.

    Flow
    ----
    1. Parse ``argparse`` arguments.
    2. Resolve the Zotero collection key (key or name).
    3. Fetch all linked-file PDFs from that collection.
    4. Display a summary of found files (including size).
    5. On ``--dry-run``, exit without processing.
    6. Otherwise, invoke ``pipeline_async.py process …`` with the PDF paths
       and forward RAGFlow-related arguments.
    """
    p = argparse.ArgumentParser(
        description="Zotero linked-file PDFs → MinerU pipeline"
    )
    p.add_argument(
        "--collection", "-c",
        required=True,
        help="Zotero collection key (8 chars) or name",
    )
    p.add_argument(
        "--dataset-id", "-d",
        required=True,
        help="RAGFlow dataset ID",
    )
    p.add_argument("--lang", default="en", help="Document language (default: en)")
    p.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Parallel upload count (default: 4)",
    )
    p.add_argument(
        "--dataset-name",
        default="",
        help="Dataset subdirectory name under _raw/",
    )
    p.add_argument(
        "--pages",
        default=None,
        help="Page range, e.g. 1-20",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List linked PDFs without running the pipeline",
    )
    args = p.parse_args()

    # ------------------------------------------------------------------
    # Step 1 — Resolve collection
    # ------------------------------------------------------------------
    print(f"Resolving collection: {args.collection}")
    collection_key: str = resolve_collection_key(args.collection)
    print(f"  → key={collection_key}")

    # ------------------------------------------------------------------
    # Step 2 — Fetch linked-file PDFs from Zotero
    # ------------------------------------------------------------------
    print("Fetching linked PDFs...")
    pdfs: list[dict[str, Any]] = get_linked_pdfs(collection_key)
    print(f"  → {len(pdfs)} linked PDF(s) found")

    if not pdfs:
        print("No linked PDFs to process.")
        return

    # Print a human-readable summary of each PDF found.
    for pdf in pdfs:
        size_mb: float = Path(pdf["path"]).stat().st_size / (1024 * 1024)
        print(f"  📄 {pdf['title'][:80]}")
        print(f"     {pdf['path']} ({size_mb:.1f} MB)")

    # ------------------------------------------------------------------
    # Step 3 — Dry-run?  Stop here.
    # ------------------------------------------------------------------
    if args.dry_run:
        print("\nDry run — no pipeline execution.")
        return

    # ------------------------------------------------------------------
    # Step 4 — Invoke pipeline_async.py
    # ------------------------------------------------------------------
    # pipeline_async.py lives in the same directory as this script (resolved
    # via _SCRIPTS_DIR at module load time), so we reference it by relative
    # path rather than assuming it is on $PATH.
    pdf_paths: list[str] = [pdf["path"] for pdf in pdfs]

    cmd: list[str] = [
        sys.executable,
        str(Path(_SCRIPTS_DIR) / "pipeline_async.py"),
        "process",
        *pdf_paths,
        "--dataset-id", args.dataset_id,
        "--lang", args.lang,
        "--concurrency", str(args.concurrency),
    ]
    if args.dataset_name:
        cmd += ["--dataset-name", args.dataset_name]
    if args.pages:
        cmd += ["--pages", args.pages]

    print(f"\nRunning: {' '.join(cmd)}")
    # Use check=False to let the user see pipeline_async.py's own stderr
    # output without a hard crash on non-zero exit.
    subprocess.run(cmd, check=False)


if __name__ == "__main__":
    main()
