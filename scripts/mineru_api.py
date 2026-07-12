#!/usr/bin/env python3
"""
MinerU Cloud API CLI — parse PDF/Office/Images via mineru.net API.

Setup
-----
  pip install requests

  # Token (required for v4 Precision API):
  #   1. Go to https://mineru.net/apiManage
  #   2. Create a token (API Key management)
  #   3. Set env:  set MINERU_TOKEN=your_token_here
  #   4. Or pass with:  --token your_token_here

Usage
-----
  # Quick parse (Agent API, no token, lightweight)
  python mineru_api.py parse https://example.com/doc.pdf

  # Precision parse (needs token, better quality)
  python mineru_api.py parse input.pdf --api v4 --model vlm

  # Submit only
  python mineru_api.py submit input.pdf
  # Check status
  python mineru_api.py status <task_id>
  # Download result
  python mineru_api.py download <task_id> -o ./output/

  # Batch upload (v4 only)
  python mineru_api.py batch file1.pdf file2.pdf file3.pdf

API Comparison
--------------
             Precision (v4)          Agent (v1)
  Token      Required                 None
  Limit      ≤200MB / ≤200 pages      ≤10MB / ≤20 pages
  Models     pipeline/vlm/HTML        pipeline (fixed)
  Output     Zip (MD+JSON+...)        Markdown only
"""

import argparse
import json
import os
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests

# Fix encoding for Windows consoles (cp1252 can't handle unicode chars)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Configuration ────────────────────────────────────────────────

BASE_URL = "https://mineru.net"
DEFAULT_POLL_INTERVAL = 3  # seconds
DEFAULT_TIMEOUT = 600      # seconds (10 min)

# ── Helpers ──────────────────────────────────────────────────────


def _env_token() -> str | None:
    """Resolve MINERU_TOKEN from environment or project-local .env.

    Checks in order:
      1. MINERU_TOKEN environment variable
      2. Project-local .env file (same directory as this script's parent)

    Does NOT check user home directories — those are deployment-specific
    paths that don't belong in open-source code.
    """
    t = os.environ.get("MINERU_TOKEN")
    if t:
        return t
    # Fallback: read from project-local .env (parent of scripts/)
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("MINERU_TOKEN="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _resolve_token(cli_token: str | None) -> str:
    """Resolve token from CLI arg, env var, or error out."""
    token = cli_token or _env_token()
    if not token:
        sys.exit(
            "ERROR: Token required for Precision API.\n"
            "  Get one at https://mineru.net/apiManage\n"
            "  Then set MINERU_TOKEN env var or pass --token."
        )
    return token


def _headers(token: str | None = None) -> dict:
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _check_code(resp: dict, task_id: str = "") -> None:
    """Raise SystemExit on API error."""
    if resp.get("code") != 0:
        sys.exit(f"API Error (code={resp.get('code')}): {resp.get('msg')}")


def _progress_bar(current: int, total: int, width: int = 30) -> str:
    if total == 0:
        return "[          waiting          ]"
    pct = min(current / total, 1.0)
    filled = int(width * pct)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {current}/{total} pages ({pct:.0%})"


def _is_url(s: str) -> bool:
    try:
        p = urlparse(s)
        return p.scheme in ("http", "https")
    except Exception:
        return False


def _is_local_file(s: str) -> bool:
    return Path(s).is_file()


def _file_size_mb(path: str) -> float:
    return Path(path).stat().st_size / (1024 * 1024)


# ── API Calls ────────────────────────────────────────────────────


def submit_by_url(
    url: str,
    token: str | None = None,
    model_version: str = "pipeline",
    api_version: str = "v4",
    is_ocr: bool = False,
    enable_formula: bool = True,
    enable_table: bool = True,
    language: str = "ch",
    extra_formats: list[str] | None = None,
    page_ranges: str | None = None,
    callback: str | None = None,
    seed: str | None = None,
    no_cache: bool = False,
    data_id: str | None = None,
) -> dict:
    """Submit a document URL for parsing. Returns JSON response with task_id."""
    if api_version == "v4":
        endpoint = f"{BASE_URL}/api/v4/extract/task"
        body = {
            "url": url,
            "model_version": model_version,
            "is_ocr": is_ocr,
            "enable_formula": enable_formula,
            "enable_table": enable_table,
            "language": language,
            "no_cache": no_cache,
        }
        if extra_formats:
            body["extra_formats"] = extra_formats
        if page_ranges:
            body["page_ranges"] = page_ranges
        if callback:
            body["callback"] = callback
            body["seed"] = seed or ""
        if data_id:
            body["data_id"] = data_id
        headers = _headers(_resolve_token(token))
    else:
        endpoint = f"{BASE_URL}/api/v1/agent/parse/url"
        body = {
            "url": url,
            "language": language,
            "enable_table": enable_table,
            "is_ocr": is_ocr,
            "enable_formula": enable_formula,
        }
        if page_ranges:
            body["page_range"] = page_ranges
        headers = _headers(token)  # token can be None for agent

    resp = requests.post(endpoint, json=body, headers=headers)
    data = resp.json()
    _check_code(data)
    return data


def submit_by_file(
    file_path: str,
    token: str | None = None,
    api_version: str = "v4",
    model_version: str = "pipeline",
    is_ocr: bool = False,
    enable_formula: bool = True,
    enable_table: bool = True,
    language: str = "ch",
    page_ranges: str | None = None,
    no_cache: bool = False,
    data_id: str | None = None,
) -> dict:
    """Upload a local file via signed URL. Returns JSON response with batch_id (v4) or task_id (v1)."""
    file_name = Path(file_path).name

    if api_version == "v4":
        # Step 1: request signed upload URL
        endpoint = f"{BASE_URL}/api/v4/file-urls/batch"
        body = {
            "files": [{"name": file_name}],
            "model_version": model_version,
            "is_ocr": is_ocr,
        }
        if data_id:
            body["files"][0]["data_id"] = data_id
        if page_ranges:
            body["files"][0]["page_ranges"] = page_ranges
        headers = _headers(_resolve_token(token))
    else:
        endpoint = f"{BASE_URL}/api/v1/agent/parse/file"
        body = {
            "file_name": file_name,
            "language": language,
            "enable_table": enable_table,
            "is_ocr": is_ocr,
            "enable_formula": enable_formula,
        }
        if page_ranges:
            body["page_range"] = page_ranges
        headers = _headers(token)

    resp = requests.post(endpoint, json=body, headers=headers)
    data = resp.json()
    _check_code(data)

    if api_version == "v4":
        # Step 2: PUT upload to signed URL
        upload_url = data["data"]["file_urls"][0]
        batch_id = data["data"]["batch_id"]
        print(f"Uploading {file_name} ({_file_size_mb(file_path):.1f} MB) ...")
        with open(file_path, "rb") as f:
            put_resp = requests.put(upload_url, data=f)
        if put_resp.status_code not in (200, 204):
            sys.exit(f"Upload failed: HTTP {put_resp.status_code}\n{put_resp.text}")
        print("Upload complete.")
        return data
    else:
        # Agent API: get upload URL from response
        upload_url = data["data"]["file_url"]
        task_id = data["data"]["task_id"]
        print(
            f"Uploading {file_name} ({_file_size_mb(file_path):.1f} MB) "
            f"→ task={task_id} ..."
        )
        with open(file_path, "rb") as f:
            put_resp = requests.put(upload_url, data=f)
        if put_resp.status_code not in (200, 204):
            sys.exit(f"Upload failed: HTTP {put_resp.status_code}\n{put_resp.text}")
        print("Upload complete.")
        return data


def submit_batch_by_urls(
    urls: list[str],
    token: str | None = None,
    model_version: str = "pipeline",
    is_ocr: bool = False,
    enable_formula: bool = True,
    enable_table: bool = True,
    language: str = "ch",
    data_ids: list[str] | None = None,
) -> dict:
    """Submit multiple URLs at once (v4 only, max 50)."""
    endpoint = f"{BASE_URL}/api/v4/extract/task/batch"
    files = []
    for i, url in enumerate(urls):
        f = {
            "url": url,
            "is_ocr": is_ocr,
            "enable_formula": enable_formula,
            "enable_table": enable_table,
            "language": language,
        }
        if data_ids and i < len(data_ids):
            f["data_id"] = data_ids[i]
        files.append(f)

    body = {"files": files, "model_version": model_version}
    headers = _headers(_resolve_token(token))
    resp = requests.post(endpoint, json=body, headers=headers)
    data = resp.json()
    _check_code(data)
    return data


def submit_batch_by_files(
    file_paths: list[str],
    token: str | None = None,
    model_version: str = "pipeline",
    is_ocr: bool = False,
) -> dict:
    """Upload multiple local files (v4 only, max 50)."""
    endpoint = f"{BASE_URL}/api/v4/file-urls/batch"
    files = [{"name": Path(p).name} for p in file_paths]
    body = {
        "files": files,
        "model_version": model_version,
        "is_ocr": is_ocr,
    }
    headers = _headers(_resolve_token(token))
    resp = requests.post(endpoint, json=body, headers=headers)
    data = resp.json()
    _check_code(data)

    # Upload each file to its signed URL
    for file_path, upload_url in zip(file_paths, data["data"]["file_urls"]):
        name = Path(file_path).name
        print(f"Uploading {name} ({_file_size_mb(file_path):.1f} MB) ...")
        with open(file_path, "rb") as f:
            put_resp = requests.put(upload_url, data=f)
        if put_resp.status_code not in (200, 204):
            sys.exit(
                f"Upload failed for {name}: HTTP {put_resp.status_code}"
            )
    print(f"All {len(file_paths)} files uploaded.")
    return data


def query_task(task_id: str, token: str | None = None, api_version: str = "v4") -> dict:
    """Poll task status. Returns full JSON response."""
    if api_version == "v4":
        endpoint = f"{BASE_URL}/api/v4/extract/task/{task_id}"
        headers = _headers(_resolve_token(token))
    else:
        endpoint = f"{BASE_URL}/api/v1/agent/parse/{task_id}"
        headers = _headers(token)

    resp = requests.get(endpoint, headers=headers)
    data = resp.json()
    _check_code(data, task_id)
    return data


def query_batch(
    batch_id: str, token: str | None = None, api_version: str = "v4"
) -> dict:
    """Query batch results (v4 only)."""
    endpoint = f"{BASE_URL}/api/v4/extract-results/batch/{batch_id}"
    headers = _headers(_resolve_token(token))
    resp = requests.get(endpoint, headers=headers)
    data = resp.json()
    _check_code(data, batch_id)
    return data


# ── Polling logic ────────────────────────────────────────────────


def poll_and_wait(
    task_id: str,
    token: str | None = None,
    api_version: str = "v4",
    timeout: int = DEFAULT_TIMEOUT,
    interval: int = DEFAULT_POLL_INTERVAL,
    quiet: bool = False,
) -> dict:
    """Poll until task completes. Returns the final response data."""
    start = time.time()
    last_progress = ""

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            sys.exit(f"Timeout after {timeout}s — task {task_id} still not done.")

        data = query_task(task_id, token, api_version)
        state = data["data"].get("state", "unknown")

        # Show progress
        progress = data["data"].get("extract_progress", {})
        if progress:
            current = progress.get("extracted_pages", 0)
            total = progress.get("total_pages", 0)
            bar = _progress_bar(current, total)
            if bar != last_progress:
                print(f"\r  {bar}", end="", flush=True)
                last_progress = bar
        elif not quiet:
            elapsed_str = f"{int(elapsed)}s"
            print(f"\r  state={state} ({elapsed_str})", end="", flush=True)

        if state == "done":
            if progress:
                print()  # newline after progress bar
            elif not quiet:
                print()
            return data
        elif state == "failed":
            err = data["data"].get("err_msg", "Unknown error")
            sys.exit(f"\nTask {task_id} failed: {err}")
        elif state in ("waiting-file", "uploading"):
            pass  # still uploading
        elif state in ("pending", "running", "converting"):
            pass  # normal intermediate states
        else:
            if not quiet:
                print(f"\n  Unknown state: {state}")

        time.sleep(interval)


def poll_batch_and_wait(
    batch_id: str,
    token: str | None = None,
    timeout: int = DEFAULT_TIMEOUT * 3,
    interval: int = DEFAULT_POLL_INTERVAL,
) -> dict:
    """Poll batch until all files complete."""
    start = time.time()

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            sys.exit(f"Batch {batch_id} timeout after {timeout}s.")

        data = query_batch(batch_id, token)

        # Count states
        states = {}
        for r in data["data"].get("extract_result", []):
            s = r.get("state", "unknown")
            states[s] = states.get(s, 0) + 1

        status_line = ", ".join(f"{k}: {v}" for k, v in sorted(states.items()))
        print(f"\r  [{int(elapsed)}s] {status_line}", end="", flush=True)

        # All done?
        all_done = all(
            r.get("state") == "done"
            for r in data["data"].get("extract_result", [])
        )
        any_failed = any(
            r.get("state") == "failed"
            for r in data["data"].get("extract_result", [])
        )

        if all_done:
            print()
            return data
        if any_failed:
            for r in data["data"].get("extract_result", []):
                if r.get("state") == "failed":
                    print(f"\n  FAILED: {r.get('file_name')} - {r.get('err_msg')}")

        time.sleep(interval)


# ── Download & extract ──────────────────────────────────────────


def download_result(
    task_data: dict,
    output_dir: str,
    api_version: str = "v4",
    token: str | None = None,
) -> list[Path]:
    """Download and optionally extract the result. Returns list of output files."""
    d = task_data["data"]
    os.makedirs(output_dir, exist_ok=True)
    saved: list[Path] = []

    if api_version == "v4":
        zip_url = d.get("full_zip_url")
        if not zip_url:
            sys.exit("No full_zip_url in response — task may not be 'done' yet.")
        # Download zip
        zip_path = Path(output_dir) / f"{d['task_id']}.zip"
        print(f"Downloading {zip_url}")
        _download_file(zip_url, zip_path)
        saved.append(zip_path)
        # Extract
        extract_dir = Path(output_dir) / d["task_id"]
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        saved.append(extract_dir)
        print(f"Extracted to {extract_dir}")
        # Show contents
        md_files = list(extract_dir.rglob("*.md"))
        print(f"  Markdown: {[f.name for f in md_files]}")
        json_files = list(extract_dir.rglob("*.json"))
        print(f"  JSON:     {[f.name for f in json_files]}")
    else:
        md_url = d.get("markdown_url")
        if not md_url:
            sys.exit("No markdown_url in response — task may not be done yet.")
        md_path = Path(output_dir) / f"{d['task_id']}.md"
        print(f"Downloading {md_url}")
        _download_file(md_url, md_path)
        saved.append(md_path)
        print(f"Saved to {md_path}")

    return saved


def _download_file(url: str, dest: Path):
    """Stream download with progress."""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f:
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = min(downloaded / total, 1.0)
                bar = _progress_bar(int(pct * 100), 100, 25)
                mb = downloaded / (1024 * 1024)
                total_mb = total / (1024 * 1024)
                print(
                    f"\r  {bar}  {mb:.1f}/{total_mb:.1f} MB", end="", flush=True
                )
    if total:
        print()


def download_batch_results(
    batch_data: dict,
    output_dir: str,
    token: str | None = None,
) -> list[Path]:
    """Download all results in a batch."""
    os.makedirs(output_dir, exist_ok=True)
    saved: list[Path] = []

    for r in batch_data["data"].get("extract_result", []):
        if r.get("state") != "done":
            print(f"  Skipping {r.get('file_name')} (state={r.get('state')})")
            continue
        zip_url = r.get("full_zip_url")
        if not zip_url:
            continue
        name = Path(r.get("file_name", "output")).stem
        zip_path = Path(output_dir) / f"{name}.zip"
        print(f"Downloading {name}.zip ...")
        _download_file(zip_url, zip_path)
        saved.append(zip_path)
        # Extract
        extract_dir = Path(output_dir) / name
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        print(f"  Extracted to {extract_dir}")

    return saved


# ── Commands ─────────────────────────────────────────────────────


def cmd_submit(args):
    """Submit a document for parsing."""
    Input = args.input
    api = args.api

    if api == "v4" and not args.token and not _env_token():
        sys.exit(
            "Precision API (v4) requires a token.\n"
            "  Get one: https://mineru.net/apiManage\n"
            "  Then: set MINERU_TOKEN=xxx  or  --token xxx"
        )

    # Determine: URL or local file
    if _is_url(Input):
        print(f"Submitting URL ({api} api): {Input}")
        data = submit_by_url(
            url=Input,
            token=args.token,
            model_version=args.model,
            api_version=api,
            is_ocr=args.ocr,
            enable_formula=not args.no_formula,
            enable_table=not args.no_table,
            language=args.lang,
            extra_formats=args.extra_formats.split(",") if args.extra_formats else None,
            page_ranges=args.pages,
            no_cache=args.no_cache,
            data_id=args.data_id,
        )
    elif _is_local_file(Input):
        print(f"Submitting file ({api} api): {Input}")
        data = submit_by_file(
            file_path=Input,
            token=args.token,
            api_version=api,
            model_version=args.model,
            is_ocr=args.ocr,
            enable_formula=not args.no_formula,
            enable_table=not args.no_table,
            language=args.lang,
            page_ranges=args.pages,
            no_cache=args.no_cache,
            data_id=args.data_id,
        )
    else:
        sys.exit(f"Input is not a URL or local file: {Input}")

    if api == "v4":
        batch_id = data["data"].get("batch_id")
        task_id = data["data"].get("task_id")
        if batch_id:
            print(f"batch_id = {batch_id}")
            return batch_id
        else:
            print(f"task_id  = {task_id}")
            return task_id
    else:
        task_id = data["data"]["task_id"]
        print(f"task_id  = {task_id}")
        return task_id


def cmd_status(args):
    """Check task/batch status."""
    api = args.api
    task_data = query_task(args.task_id, args.token, api)
    d = task_data["data"]
    state = d.get("state", "unknown")
    print(f"task_id  = {d.get('task_id', args.task_id)}")
    print(f"state    = {state}")

    progress = d.get("extract_progress")
    if progress:
        print(
            f"pages    = {progress.get('extracted_pages', '?')}"
            f"/{progress.get('total_pages', '?')}"
        )
        if progress.get("start_time"):
            print(f"started  = {progress.get('start_time')}")

    if state == "done":
        if api == "v4":
            print(f"zip_url  = {d.get('full_zip_url', 'N/A')}")
        else:
            print(f"md_url   = {d.get('markdown_url', 'N/A')}")
    elif state == "failed":
        print(f"error    = {d.get('err_msg', 'Unknown')}")


def cmd_download(args):
    """Download task result."""
    api = args.api
    print(f"Checking status for {args.task_id} ...")
    task_data = query_task(args.task_id, args.token, api)
    state = task_data["data"].get("state", "unknown")

    if state != "done":
        sys.exit(
            f"Task is not done (state={state}). "
            f"Use 'status' to check, or 'parse' to wait."
        )

    saved = download_result(task_data, args.output or ".", api, args.token)
    print(f"\nDone — {len(saved)} file(s) saved.")


def cmd_parse(args):
    """Submit + wait + download (full pipeline)."""
    Input = args.input
    output = args.output or "."
    api = args.api

    if api == "v4" and not args.token and not _env_token():
        sys.exit(
            "Precision API (v4) requires a token.\n"
            "  Get one: https://mineru.net/apiManage\n"
            "  Then: set MINERU_TOKEN=xxx  or  --token xxx"
        )

    # Submit
    if _is_url(Input):
        print(f"=== Submit URL ({api} api) ===")
        data = submit_by_url(
            url=Input,
            token=args.token,
            model_version=args.model,
            api_version=api,
            is_ocr=args.ocr,
            enable_formula=not args.no_formula,
            enable_table=not args.no_table,
            language=args.lang,
            extra_formats=args.extra_formats.split(",") if args.extra_formats else None,
            page_ranges=args.pages,
            no_cache=args.no_cache,
        )
        task_id = data["data"]["task_id"]
    elif _is_local_file(Input):
        print(f"=== Submit file ({api} api) ===")
        data = submit_by_file(
            file_path=Input,
            token=args.token,
            api_version=api,
            model_version=args.model,
            is_ocr=args.ocr,
            enable_formula=not args.no_formula,
            enable_table=not args.no_table,
            language=args.lang,
            page_ranges=args.pages,
            no_cache=args.no_cache,
            data_id=args.data_id,
        )
        task_id = data["data"].get("task_id") or data["data"]["batch_id"]
        is_batch = "batch_id" in data["data"] and "task_id" not in data["data"]
    else:
        sys.exit(f"Input is not a URL or local file: {Input}")

    print(f"task_id = {task_id}")

    # Wait
    print(f"\n=== Polling (timeout={args.timeout}s) ===")
    if is_batch:
        # v4 batch — use query_batch, extract single file result
        import time as _time
        start = _time.time()
        while True:
            if _time.time() - start > args.timeout:
                sys.exit(f"Timeout after {args.timeout}s")
            bdata = query_batch(task_id, args.token, api)
            results = bdata["data"].get("extract_result", [])
            if not results:
                _time.sleep(args.interval); continue
            state = results[0].get("state", "?")
            if state == "done":
                task_data = bdata; break
            elif state == "failed":
                sys.exit(f"Batch failed: {results[0].get('err_msg','?')}")
            _time.sleep(args.interval)
    else:
        task_data = poll_and_wait(
            task_id, args.token, api, timeout=args.timeout, interval=args.interval
        )

    # Download
    print(f"\n=== Download ===")
    if is_batch:
        # v4 batch: extract first result's download URL
        results = task_data["data"].get("extract_result", [])
        if not results:
            sys.exit("No results in batch")
        md_url = results[0].get("markdown_url") or results[0].get("full_md_url")
        zip_url = results[0].get("full_zip_url")
        if md_url:
            md_name = results[0].get("file_name", "output").rsplit(".", 1)[0]
            md_path = Path(output) / f"{md_name}.md"
            print(f"Downloading {md_url}")
            import requests as _r
            resp = _r.get(md_url, timeout=60)
            resp.raise_for_status()
            md_path.write_text(resp.text, encoding="utf-8")
            saved = [md_path]
            print(f"Saved to {md_path}")
        elif zip_url:
            # Download zip and extract markdown
            import requests as _r, zipfile, tempfile
            zip_path = Path(output) / f"{task_id}.zip"
            print(f"Downloading {zip_url}")
            resp = _r.get(zip_url, timeout=120)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)
            extract_dir = Path(output) / task_id
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
            md_files = list(extract_dir.rglob("*.md"))
            saved = md_files
            print(f"Extracted to {extract_dir} ({len(md_files)} md files)")
        else:
            sys.exit(f"No markdown URL in result: {list(results[0].keys())}")
    else:
        saved = download_result(task_data, output, api, args.token)
    print(f"\nDone — {len(saved)} file(s) saved.")


def cmd_batch_v1(args):
    """Submit multiple files in parallel via Agent API (v1), poll all, download all."""
    files = args.files
    if len(files) > 50:
        sys.exit("Max 50 files per batch.")
    for f in files:
        if not _is_local_file(f):
            sys.exit(f"Not a local file: {f} — batch-v1 only supports local files.")

    concurrency = min(args.concurrency, 8)
    print(f"Submitting {len(files)} files in parallel (concurrency={concurrency}) ...")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    tasks = {}  # task_id -> file_path

    def _submit_one(fp):
        try:
            data = submit_by_file(file_path=fp, api_version="v1",
                                  language=args.lang, page_ranges=args.pages)
            tid = data["data"]["task_id"]
            print(f"  [{tid[:8]}...] {Path(fp).name}")
            return tid, fp, None
        except Exception as e:
            return None, fp, str(e)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_submit_one, f): f for f in files}
        for fut in as_completed(futures):
            tid, fp, err = fut.result()
            if err:
                print(f"  FAIL: {Path(fp).name} — {err}")
            else:
                tasks[tid] = fp
    print()

    if not tasks:
        sys.exit("All submissions failed.")
    if args.no_wait:
        print("Task IDs:")
        for tid in tasks:
            print(f"  {tid}")
        print(f"\nPoll later: python mineru_api.py download <task_id> -o {args.output}")
        return

    # Poll all
    print(f"Polling {len(tasks)} tasks (timeout={args.timeout}s) ...")
    start = time.time()
    done = set()
    api = "v1"
    while len(done) < len(tasks):
        elapsed = int(time.time() - start)
        if elapsed > args.timeout:
            print(f"\nTimeout. {len(done)}/{len(tasks)} done, {len(tasks)-len(done)} pending.")
            for tid in tasks:
                if tid not in done:
                    print(f"  pending: {tid}")
            return
        for tid in list(tasks):
            if tid in done:
                continue
            try:
                resp = requests.get(
                    f"{BASE_URL}/api/v1/agent/parse/{tid}",
                    timeout=10,
                )
                d = resp.json()
                state = d.get("data", {}).get("state", "?")
                if state == "done":
                    done.add(tid)
                elif state == "failed":
                    err = d.get("data", {}).get("err_msg", "?")
                    print(f"\n  [{tid[:8]}...] FAILED: {err}")
                    done.add(tid)  # count as done to stop polling
            except Exception:
                pass
        msg = " ".join(f"{Path(tasks[t]).name[:30]}={('DONE' if t in done else '...')}" for t in list(tasks)[:4])
        print(f"\r  [{elapsed}s] {msg}", end="", flush=True)
        time.sleep(args.interval)
    print()

    # Download
    print(f"\nDownloading {len(done)} results ...")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    saved = 0
    for tid in tasks:
        if tid not in done:
            continue
        try:
            resp = requests.get(
                f"{BASE_URL}/api/v1/agent/parse/{tid}",
                timeout=10,
            )
            d = resp.json()
            md_url = d.get("data", {}).get("markdown_url", "")
            if not md_url:
                print(f"  [{tid[:8]}...] no download URL")
                continue
            fname = Path(tasks[tid]).stem + ".md"
            dest = out / fname
            with requests.get(md_url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(65536):
                        fh.write(chunk)
            print(f"  [{tid[:8]}...] → {dest}")
            saved += 1
        except Exception as e:
            print(f"  [{tid[:8]}...] download failed: {e}")
    print(f"\nDone — {saved}/{len(tasks)} file(s) saved.")


def cmd_batch(args):
    """Submit multiple files (v4 only)."""
    api = "v4"
    token = _resolve_token(args.token)

    is_urls = all(_is_url(f) for f in args.files)
    is_local = all(_is_local_file(f) for f in args.files)

    if is_urls:
        print(f"Submitting {len(args.files)} URLs ...")
        data = submit_batch_by_urls(
            urls=args.files,
            token=args.token,
            model_version=args.model,
            is_ocr=args.ocr,
            language=args.lang,
        )
    elif is_local:
        print(f"Submitting {len(args.files)} files ...")
        data = submit_batch_by_files(
            file_paths=args.files,
            token=args.token,
            model_version=args.model,
            is_ocr=args.ocr,
        )
    else:
        sys.exit("All inputs must be either URLs or local files (cannot mix).")

    batch_id = data["data"]["batch_id"]
    print(f"batch_id = {batch_id}")

    if not args.no_wait:
        print(f"\n=== Polling batch ===")
        batch_data = poll_batch_and_wait(
            batch_id, token, timeout=args.timeout, interval=args.interval
        )
        print(f"\n=== Downloading results ===")
        saved = download_batch_results(
            batch_data, args.output or ".", token
        )
        print(f"\nDone — {len(saved)} archives saved.")


def cmd_test(args):
    """Run a quick connectivity test."""
    print("=== MinerU API Connectivity Test ===\n")

    # Test Agent API (no token needed)
    print("[1/3] Testing Agent API (v1, no token) ...")
    try:
        resp = requests.post(
            f"{BASE_URL}/api/v1/agent/parse/url",
            json={
                "url": "https://cdn-mineru.openxlab.org.cn/demo/example.pdf",
                "language": "en",
                "page_range": "1-1",
            },
            timeout=15,
        )
        data = resp.json()
        if data.get("code") == 0:
            task_id = data["data"]["task_id"]
            print(f"  [OK] Agent API OK -- task_id={task_id}")
        else:
            print(f"  [WARN] Agent API returned code={data.get('code')}: {data.get('msg')}")
    except Exception as e:
        print(f"  [FAIL] Agent API unreachable: {e}")

    # Test Precision API
    token = args.token or _env_token()
    if token:
        print("\n[2/3] Testing Precision API (v4) ...")
        try:
            resp = requests.post(
                f"{BASE_URL}/api/v4/extract/task",
                json={
                    "url": "https://cdn-mineru.openxlab.org.cn/demo/example.pdf",
                    "model_version": "pipeline",
                    "language": "en",
                },
                headers=_headers(token),
                timeout=15,
            )
            data = resp.json()
            if data.get("code") == 0:
                task_id = data["data"]["task_id"]
                print(f"  [OK] Precision API OK -- task_id={task_id}")
            else:
                print(
                    f"  [WARN] Precision API returned code={data.get('code')}: "
                    f"{data.get('msg')}"
                )
        except Exception as e:
            print(f"  [FAIL] Precision API error: {e}")
    else:
        print("\n[2/3] Skipping Precision API test (no token configured).")
        print("  Set MINERU_TOKEN env var or pass --token.")

    print("\n[3/3] Checking demo file accessibility ...")
    try:
        resp = requests.head(
            "https://cdn-mineru.openxlab.org.cn/demo/example.pdf", timeout=10
        )
        print(f"  [OK] Demo file accessible (HTTP {resp.status_code})")
    except Exception as e:
        print(f"  [WARN] Demo file: {e}")

    print("\n=== Test complete ===")


# ── CLI Setup ────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="MinerU Cloud API CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s test                          Quick connectivity test
  %(prog)s parse input.pdf               Agent API parse (no token)
  %(prog)s parse input.pdf --api v4      Precision API parse (needs token)
  %(prog)s parse https://.../doc.pdf --api v4 --model vlm
  %(prog)s batch *.pdf --api v4          Batch upload
  %(prog)s status <task_id>              Check task status
  %(prog)s download <task_id> -o ./out   Download results

Token: https://mineru.net/apiManage
""",
    )

    sub = parser.add_subparsers(dest="command", help="Commands")

    # --- test ---
    p_test = sub.add_parser("test", help="API connectivity test")
    p_test.add_argument("--token", default=None, help="Bearer token")
    p_test.set_defaults(func=cmd_test)

    # --- submit ---
    p_sub = sub.add_parser("submit", help="Submit document for parsing")
    p_sub.add_argument("input", help="File path or URL")
    p_sub.add_argument("--api", default="v1", choices=["v1", "v4"],
                       help="API version: v1=Agent (no token), v4=Precision (default: v1)")
    p_sub.add_argument("--token", default=None, help="Bearer token (or set MINERU_TOKEN)")
    p_sub.add_argument("--model", default="pipeline",
                       choices=["pipeline", "vlm", "MinerU-HTML"],
                       help="Model version (v4 only, default: pipeline)")
    p_sub.add_argument("--ocr", action="store_true", help="Enable OCR")
    p_sub.add_argument("--no-formula", action="store_true", help="Disable formula recognition")
    p_sub.add_argument("--no-table", action="store_true", help="Disable table recognition")
    p_sub.add_argument("--lang", default="ch", help="Document language (default: ch)")
    p_sub.add_argument("--pages", default=None, help="Page range, e.g. '1-10'")
    p_sub.add_argument("--no-cache", action="store_true", help="Bypass cache (v4)")
    p_sub.add_argument("--extra-formats", default=None,
                       help="Extra output formats: docx,html,latex (comma-sep, v4 only)")
    p_sub.add_argument("--data-id", default=None, help="Business data ID")
    p_sub.set_defaults(func=cmd_submit)

    # --- status ---
    p_st = sub.add_parser("status", help="Check task status")
    p_st.add_argument("task_id", help="Task ID")
    p_st.add_argument("--api", default="v1", choices=["v1", "v4"],
                      help="API version (default: v1)")
    p_st.add_argument("--token", default=None, help="Bearer token")
    p_st.set_defaults(func=cmd_status)

    # --- download ---
    p_dl = sub.add_parser("download", help="Download task result")
    p_dl.add_argument("task_id", help="Task ID")
    p_dl.add_argument("-o", "--output", default=".", help="Output directory")
    p_dl.add_argument("--api", default="v1", choices=["v1", "v4"],
                      help="API version (default: v1)")
    p_dl.add_argument("--token", default=None, help="Bearer token")
    p_dl.set_defaults(func=cmd_download)

    # --- parse (submit + wait + download) ---
    p_parse = sub.add_parser("parse", help="Full pipeline: submit + wait + download")
    p_parse.add_argument("input", help="File path or URL")
    p_parse.add_argument("-o", "--output", default=".", help="Output directory")
    p_parse.add_argument("--api", default="v1", choices=["v1", "v4"],
                         help="API version: v1=Agent (no token), v4=Precision (default: v1)")
    p_parse.add_argument("--token", default=None, help="Bearer token (or set MINERU_TOKEN)")
    p_parse.add_argument("--model", default="pipeline",
                         choices=["pipeline", "vlm", "MinerU-HTML"],
                         help="Model version (v4 only)")
    p_parse.add_argument("--ocr", action="store_true", help="Enable OCR")
    p_parse.add_argument("--no-formula", action="store_true", help="Disable formula recognition")
    p_parse.add_argument("--no-table", action="store_true", help="Disable table recognition")
    p_parse.add_argument("--lang", default="ch", help="Document language (default: ch)")
    p_parse.add_argument("--pages", default=None, help="Page range, e.g. '1-10'")
    p_parse.add_argument("--no-cache", action="store_true", help="Bypass cache (v4)")
    p_parse.add_argument("--extra-formats", default=None,
                         help="Extra output formats: docx,html,latex (comma-sep, v4 only)")
    p_parse.add_argument("--data-id", default=None, help="Business data ID")
    p_parse.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                         help=f"Poll timeout in seconds (default: {DEFAULT_TIMEOUT})")
    p_parse.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                         help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})")
    p_parse.set_defaults(func=cmd_parse)

    # --- batch ---
    p_batch = sub.add_parser("batch", help="Submit multiple files (v4 only)")
    p_batch.add_argument("files", nargs="+", help="File paths or URLs (max 50)")
    p_batch.add_argument("-o", "--output", default=".", help="Output directory")
    p_batch.add_argument("--token", default=None, help="Bearer token")
    p_batch.add_argument("--model", default="pipeline",
                         choices=["pipeline", "vlm"],
                         help="Model version")
    p_batch.add_argument("--ocr", action="store_true", help="Enable OCR")
    p_batch.add_argument("--lang", default="ch", help="Document language")
    p_batch.add_argument("--no-wait", action="store_true",
                         help="Submit only, don't wait for completion")
    p_batch.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT * 3,
                         help=f"Poll timeout in seconds (default: {DEFAULT_TIMEOUT * 3})")
    p_batch.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                         help=f"Poll interval in seconds")
    p_batch.set_defaults(func=cmd_batch)

    # --- batch-v1 ---
    p_bv1 = sub.add_parser("batch-v1", help="Submit multiple files via Agent API (v1, parallel)")
    p_bv1.add_argument("files", nargs="+", help="File paths (max 50)")
    p_bv1.add_argument("-o", "--output", default=".", help="Output directory")
    p_bv1.add_argument("--lang", default="en", help="Document language (default: en)")
    p_bv1.add_argument("--pages", default=None, help="Page range, e.g. '1-20'")
    p_bv1.add_argument("--concurrency", type=int, default=4,
                       help="Parallel submit threads (default: 4, max: 8)")
    p_bv1.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                       help=f"Poll timeout in seconds (default: {DEFAULT_TIMEOUT})")
    p_bv1.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                       help=f"Poll interval in seconds")
    p_bv1.add_argument("--no-wait", action="store_true",
                       help="Submit only, print task IDs, don't wait")
    p_bv1.set_defaults(func=cmd_batch_v1)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
