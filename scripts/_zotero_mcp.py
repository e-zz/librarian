"""Shared Zotero transport — cookjohn MCP (:23120) + local API (:23119).

Single stdlib dependency (urllib.request). No pip install needed.

Usage:
    from _zotero_mcp import mcp_call, local_api

    # cookjohn MCP (write)
    item = mcp_call("get_item_details", {"itemKey": "A5CFQQQ7"})

    # Local API (read — collection membership, etc.)
    data = local_api("/items/A5CFQQQ7")
"""

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

# ── Endpoints ────────────────────────────────────────────────────────────

COOKJOHN_URL = "http://127.0.0.1:23120/mcp"
LOCAL_API = "http://127.0.0.1:23119/api/users/0"

# ── Layer 1: Raw transport ───────────────────────────────────────────────


def _json_rpc(method: str, params: dict, timeout: int) -> dict:
    """Send a JSON-RPC 2.0 request to the cookjohn MCP endpoint.

    Returns the full result dict (with "content" key).
    Raises RuntimeError on JSON-RPC error, URLError on connection failure.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        COOKJOHN_URL,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    if "error" in resp:
        raise RuntimeError(f"JSON-RPC error: {resp['error']}")
    return resp.get("result", {})


def _extract_text_content(result: dict) -> Any:
    """Extract the 'text' field from MCP content[0] (the common case).

    cookjohn wraps real data in result['content'][0]['text'] as a
    JSON-encoded string. This unwraps it.
    """
    content = result.get("content", [])
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            return json.loads(item["text"])
    return result


def mcp_call(tool_name: str, arguments: dict, timeout: int = 60) -> Any:
    """Call a cookjohn MCP tool and return the parsed result.

    Automatically unwraps MCP's content[0].text nesting.
    For write_item(create) which returns non-standard JSON, use mcp_call_raw.
    """
    result = _json_rpc("tools/call", {"name": tool_name, "arguments": arguments}, timeout)
    return _extract_text_content(result)


def mcp_call_raw(tool_name: str, arguments: dict, timeout: int = 60) -> str:
    """Call a cookjohn MCP tool and return the raw text string.

    Use this for write_item(create) and other calls whose response
    shape doesn't match the standard MCP content array.
    """
    result = _json_rpc("tools/call", {"name": tool_name, "arguments": arguments}, timeout)
    content = result.get("content", [{}])
    return content[0].get("text", "") if isinstance(content[0], dict) else ""


def local_api(path: str, timeout: int = 10) -> Any:
    """GET the Zotero local API and return parsed JSON.

    Path is relative to LOCAL_API, e.g. '/items/A5CFQQQ7'.
    The local API is read-only (Zotero built-in, no plugin needed).
    Requires 'Allow other applications to communicate' in Zotero prefs.
    """
    url = f"{LOCAL_API}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read().decode())


# ── Layer 2: Zotero helpers ──────────────────────────────────────────────


def resolve_collections(item_key: str) -> list[str]:
    """Return full collection paths for an item via local API.

    The cookjohn MCP doesn't expose data.collections (collection
    membership). The local API at :23119 does — use it here.

    Returns ['Parent / Child', ...] or empty list.
    """
    raw = local_api(f"/items/{item_key}")
    data = raw if isinstance(raw, dict) else raw[0] if isinstance(raw, list) else {}
    d = data.get("data", data)
    col_keys = d.get("collections", [])
    if not col_keys:
        return []

    # Resolve each collection key to its full path
    def _path(key: str, cache: dict) -> str:
        if key in cache:
            return cache[key]
        raw_c = local_api(f"/collections/{key}")
        c_data = raw_c if isinstance(raw_c, dict) else raw_c[0] if isinstance(raw_c, list) else {}
        cd = c_data.get("data", c_data)
        name = cd.get("name", key)
        parent = cd.get("parentCollection", False)
        if parent and parent is not False:
            parent_path = _path(parent, cache)
            path = f"{parent_path} / {name}"
        else:
            path = name
        cache[key] = path
        return path

    cache: dict[str, str] = {}
    return [_path(ck, cache) for ck in col_keys]


def resolve_pdf_path(item_key: str) -> str | None:
    """Find first PDF attachment path via cookjohn MCP.

    Returns Win32 absolute path string, or None if no PDF attachment found.
    """
    try:
        detail = mcp_call("get_item_details", {"itemKey": item_key, "mode": "standard"})
        for att in detail.get("attachments", []):
            if att.get("contentType") == "application/pdf":
                path_str = att.get("path")
                if path_str:
                    from pathlib import Path
                    if Path(path_str).exists():
                        return path_str
    except Exception:
        pass
    return None


# ── Local-API-only PDF path resolution (no MCP) ───────────────────────────


def _zotero_dirs() -> tuple[str | None, str | None]:
    """(linked-base-attachment-dir, data-dir) parsed from Zotero prefs.js.

    The local API returns RELATIVE attachment paths ('attachments:qn/...' or
    'storage:NAME'); the absolute path needs Zotero's own config:
    extensions.zotero.baseAttachmentPath (linked base) and dataDir (storage).
    Cached after first parse.
    """
    global _ZOTERO_DIRS_CACHE
    if _ZOTERO_DIRS_CACHE is not None:
        return _ZOTERO_DIRS_CACHE
    base = data = None
    import glob
    from pathlib import Path as _P
    pattern = os.path.join(os.environ.get("APPDATA", ""), "Zotero", "Zotero",
                           "Profiles", "*", "prefs.js")
    for prefs in glob.glob(pattern):
        try:
            txt = _P(prefs).read_text(encoding="utf-8")
        except OSError:
            continue
        m = re.search(r'user_pref\("extensions\.zotero\.baseAttachmentPath",\s*"([^"]*)"\)', txt)
        if m:
            base = m.group(1).replace("\\\\", "\\")
        m = re.search(r'user_pref\("extensions\.zotero\.dataDir",\s*"([^"]*)"\)', txt)
        if m:
            data = m.group(1).replace("\\\\", "\\")
        if base and data:
            break
    _ZOTERO_DIRS_CACHE = (base, data)
    return _ZOTERO_DIRS_CACHE


_ZOTERO_DIRS_CACHE: tuple[str | None, str | None] | None = None


def resolve_pdf_path_local(item_key: str) -> str | None:
    """First PDF attachment path via the LOCAL API ONLY (no MCP).

    children endpoint (:23119) -> data.path ('attachments:REL' / 'storage:NAME')
    -> absolute path from prefs.js base dirs. Falls back to None on any
    failure; callers may then fall back to the MCP channel.
    """
    from pathlib import Path
    try:
        kids = local_api(f"/items/{item_key}/children")
        if not isinstance(kids, list):
            return None
        base, data = _zotero_dirs()
        for kid in kids:
            d = kid.get("data", kid)
            if d.get("contentType") != "application/pdf":
                continue
            rel = d.get("path", "").replace("/", os.sep)
            if rel.startswith("attachments:"):
                if not base:
                    return None
                abs_path = os.path.join(base, rel[len("attachments:"):])
            elif rel.startswith("storage:"):
                if not data:
                    return None
                abs_path = os.path.join(data, "storage", rel[len("storage:"):])
            else:
                abs_path = rel
            if Path(abs_path).exists():
                return abs_path
    except Exception:
        return None
    return None


def search_items(query: str, limit: int = 50) -> list[dict]:
    """Search Zotero library by keyword via cookjohn MCP.

    Returns list of item dicts with keys: key, title, creators, date, attachments.
    """
    result = mcp_call("search_library", {"q": query, "limit": limit, "mode": "complete"})
    if isinstance(result, dict):
        return result.get("results", [])
    if isinstance(result, list):
        return result
    return []


def get_collection_items(collection_key: str, limit: int = 200) -> list[dict]:
    """Get all items in a Zotero collection via cookjohn MCP.

    Returns list of item dicts (with attachments).
    """
    result = _json_rpc(
        "tools/call",
        {
            "name": "get_collection_items",
            "arguments": {"collectionKey": collection_key, "limit": limit},
        },
        timeout=30,
    )
    raw = _extract_text_content(result)
    if isinstance(raw, dict):
        return raw.get("results", [])
    if isinstance(raw, list):
        return raw
    return []


def linked_pdfs_from_items(items: list[dict]) -> list[dict]:
    """Filter items to linked-file PDFs (linkMode=2) that exist on disk.

    Returns [{"item_key", "title", "path", "filename"}, ...].
    Missing files are printed to stderr and excluded.
    """
    from pathlib import Path
    import sys

    linked: list[dict] = []
    for item in items:
        for att in item.get("attachments", []):
            if att.get("linkMode") != 2 or att.get("contentType") != "application/pdf":
                continue
            path_str = att.get("path")
            if path_str and Path(path_str).exists():
                linked.append({
                    "item_key": item["key"],
                    "title": item.get("title", "Untitled"),
                    "path": path_str,
                    "filename": att.get("filename", ""),
                })
            elif path_str:
                print(f"  ⚠ Missing on disk: {path_str}", file=sys.stderr)
    return linked


# ── Layer 3: Pipeline invocation ─────────────────────────────────────────


def _resolve_scripts_dir() -> str:
    """Return the absolute path to the scripts/ directory containing this module."""
    from pathlib import Path
    return str(Path(__file__).resolve().parent)


def collect_pipeline_cmd(
    pdf_paths: list[str],
    dataset_id: str | None = None,
    dataset_name: str = "",
    lang: str = "en",
    concurrency: int = 4,
    pages: str | None = None,
) -> list[str]:
    """Build the subprocess command list for pipeline_async.py.

    Returns ['python', '/path/to/pipeline_async.py', 'process', '<pdfs>', ...]
    """
    import sys
    from pathlib import Path

    pipeline = Path(_resolve_scripts_dir()) / "pipeline_async.py"
    cmd = [
        sys.executable,
        str(pipeline),
        "process",
        *pdf_paths,
        "--lang", lang,
        "--concurrency", str(concurrency),
    ]
    if dataset_id:
        cmd += ["--dataset-id", dataset_id]
    if dataset_name:
        cmd += ["--dataset-name", dataset_name]
    if pages:
        cmd += ["--pages", pages]
    return cmd


def run_pipeline(
    pdf_paths: list[str],
    dataset_id: str | None = None,
    dataset_name: str = "",
    lang: str = "en",
    concurrency: int = 4,
    pages: str | None = None,
) -> int:
    """Run pipeline_async.py process on given PDFs and return exit code."""
    import subprocess
    cmd = collect_pipeline_cmd(
        pdf_paths, dataset_id, dataset_name, lang, concurrency, pages
    )
    print(f"  Running: {' '.join(cmd)}")
    return subprocess.run(cmd).returncode
