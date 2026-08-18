#!/usr/bin/env python3
"""
Async PDF → OCR pipeline (optional RAGFlow integration).

Two-phase processing:
  Phase 1 — Parallel OCR all PDFs via MinerU API (v1 or v4)
  Phase 2 — Per-file upload → parse to RAGFlow (skipped if --dataset-id omitted)

The pipeline is resume-safe: state is persisted after every file operation so
interruptions can be recovered with the 'resume' subcommand.

RAGFlow integration is OPTIONAL. If you only need OCR output, run without
--dataset-id. You can upload the markdown files to RAGFlow later using the
standalone ragflow_uploader.py script.

Usage examples:
  # OCR only (no RAGFlow)
  python pipeline_async.py process *.pdf

  # OCR + upload to RAGFlow
  python pipeline_async.py process *.pdf --dataset-id <ragflow-dataset-id>

  # With custom state file and higher concurrency
  python pipeline_async.py process *.pdf --dataset-id <id> --state-file my_state.json --concurrency 8

  # Resume interrupted run
  python pipeline_async.py resume --state-file my_state.json

  # Check status of a run
  python pipeline_async.py status --state-file my_state.json

  # Collect parse results (fire-and-forget mode)
  python pipeline_async.py reap --state-file my_state.json

Requirements:
  pip install requests pypdf
  # For MinerU v4 API: set MINERU_TOKEN env var
  # For RAGFlow: set RAGFLOW_URL and RAGFLOW_API_KEY env vars
"""

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# ── Make sibling modules importable ──────────────────────────────────────
# This ensures _ragflow_client.py and mineru_api.py in the same directory
# are importable regardless of how this script is launched.
_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import requests as req_lib
from _ragflow_client import RAGFlowClient
import mineru_api
from mineru_api import submit_by_file, poll_and_wait, download_result, BASE_URL

# ── Load .env (stdlib, no external deps) ──────────────────────────────────

def _load_dotenv(*paths):
    """Load KEY=VALUE pairs from .env files into os.environ (simple parser)."""
    for p in paths:
        try:
            for line in Path(p).read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip("'\"")
                if key and key not in os.environ:  # don't override existing
                    os.environ[key] = val
        except (FileNotFoundError, PermissionError):
            pass

# Load from project .env and user home fallback
_load_dotenv(
    Path(__file__).resolve().parent.parent / ".env",  # mineru_pipeline/.env
    Path.home() / "Mineru" / ".env",                   # ~/Mineru/.env
)

# ── Configurable defaults ────────────────────────────────────────────────
# These can be overridden via CLI arguments or environment variables.

# State file: stores per-file progress for resume support.
# Default is a local file in the current working directory.
DEFAULT_STATE_FILE = Path(".pipeline_state.json")

# Lock file: prevents concurrent pipeline instances on the same state file.
LOCK_FILE = Path(".pipeline_async.lock")

# Maximum number of PDFs to OCR in parallel.
DEFAULT_CONCURRENCY = 4

# Polling interval (seconds) between MinerU / RAGFlow API status checks.
DEFAULT_POLL_INTERVAL = 3
# OCR chunk polling: a 200-page textbook chunk takes 5-15 min on v4 VLM, and
# jobs may sit in the server queue ("pending") long before processing starts.
# Do NOT kill on wall-clock alone — see _poll_ocr_state().
CHUNK_POLL_TIMEOUT = 1800   # seconds (30 min) — legacy hard cap, kept for compat
POLL_HARD_CAP = 7200        # seconds (2h) — absolute last-resort safety net
# States observed from MinerU API (see mineru_api.poll_and_wait):
#   done → success; failed → terminal error with err_msg;
#   pending/running/converting/waiting-file/uploading → alive, keep waiting
_ALIVE_STATES = {"pending", "running", "converting", "waiting-file", "uploading",
                 "queued", "processing", "extracting", "waiting"}
# Terminal-bad states: abort immediately with diagnostic, never wait these out
_DEAD_STATES = {"failed", "canceled", "cancelled", "error", "timeout", "expired",
                "deleted", "rejected"}


def _poll_ocr_state(query_fn, label: str, interval: int = DEFAULT_POLL_INTERVAL,
                    hard_cap: int = POLL_HARD_CAP) -> dict:
    """State-aware OCR poll. Aborts ONLY on terminal failure states; waits
    through queue/processing states. Returns the 'done' data dict.

    query_fn() -> (state, err_msg, data). Prints state transitions so queued
    vs processing is visible. Unknown states warn but keep polling (the API
    may add states); only the hard cap can kill a job that never reaches a
    terminal state.
    """
    start = time.time()
    last = None
    while True:
        state, err, data = query_fn()
        if state != last:
            print(f"    [{label}] state={state} ({int(time.time()-start)}s)")
            last = state
        if state == "done":
            return data
        if state in _DEAD_STATES:
            raise RuntimeError(f"[{label}] {state}: {err or state}")
        if state not in _ALIVE_STATES:
            print(f"    [{label}] WARNING unknown state={state!r} — keep polling")
        if time.time() - start > hard_cap:
            raise RuntimeError(f"[{label}] hard cap {hard_cap}s reached, last state {state}")
        time.sleep(interval)


def _batch_state(batch_id: str, token: str | None, chunk_no: int = 0,
                 chunk_total: int = 0):
    """Query helper for _poll_ocr_state: v4 batch → (state, err_msg, data)."""
    import mineru_api as _ma
    d = _ma.query_batch(batch_id, token=token)
    results = d["data"].get("extract_result", [])
    if not results:
        return "waiting", "", d
    label = f"chunk {chunk_no}/{chunk_total}" if chunk_total else ""
    st = results[0].get("state", "?")
    err = results[0].get("err_msg", "")
    if st == "done" and not results[0].get("full_zip_url"):
        # done but no download URL yet — treat as still-finalizing
        return "running", "", d
    return st, err, d


def _task_state(task_id: str, chunk_no: int = 0, chunk_total: int = 0):
    """Query helper for _poll_ocr_state: v1 task → (state, err_msg, data)."""
    import mineru_api as _ma
    d = _ma.query_task(task_id, api_version="v1")
    st = d.get("data", {}).get("state", "?")
    err = d.get("data", {}).get("err_msg", "")
    return st, err, d


def _chunk_cache_model(chunk_dir, n: int):
    """Read the model marker of cached chunk n (pipeline/vlm).

    Returns the recorded model, or None for legacy chunks created before this
    feature. Legacy chunks are treated as 'pipeline' by callers via the
    (cache_model or 'legacy') convention — they are re-OCR'd when the requested
    model is vlm, reused when it's the default pipeline.
    """
    mf = chunk_dir / f"full_{n}.model"
    if mf.exists():
        return mf.read_text("utf-8").strip().lower()
    return None

# Directories to search when resuming and a PDF's stored path is missing.
# Override by setting the PIPELINE_PDF_SEARCH_DIRS env var (semicolon-separated).
_DEFAULT_SEARCH_DIRS = [
    str(Path.home() / "Downloads"),
    str(Path.home() / "Desktop"),
]
_PDF_SEARCH_DIRS_ENV = os.environ.get("PIPELINE_PDF_SEARCH_DIRS", "")
PDF_SEARCH_DIRS = (
    [Path(d.strip()) for d in _PDF_SEARCH_DIRS_ENV.split(";") if d.strip()]
    if _PDF_SEARCH_DIRS_ENV
    else [Path(d) for d in _DEFAULT_SEARCH_DIRS]
)

# OCR output directory.
# Set MINERU_OCR_DIR in .env to use a centralized dir (e.g. ~/MinerU/_raw/qc).
# When unset, OCR output goes next to each PDF in _ocr/ subdirectory.
_OCR_DIR_ENV = os.environ.get("MINERU_OCR_DIR", "")
OCR_OUTPUT_DIR = Path(_OCR_DIR_ENV) if _OCR_DIR_ENV else Path("_ocr")


def _ocr_dir(pdf_path: Path, pdf_key: str = "") -> Path:
    """Resolve OCR output directory for a PDF.

    When MINERU_OCR_DIR is set, all OCR output goes to that centralized
    location. Otherwise output goes next to the PDF in _ocr/ subdirectory.
    """
    base = OCR_OUTPUT_DIR if OCR_OUTPUT_DIR.is_absolute() else pdf_path.parent / OCR_OUTPUT_DIR
    return base / pdf_key if pdf_key else base


# ── Lock (cross-platform with Windows ctypes support) ────────────────────

def acquire_lock() -> bool:
    """
    Acquire a file-based lock to prevent concurrent pipeline instances.

    On Windows, uses ctypes to check if the owning process is still alive
    (stale lock detection). On other platforms, simply checks file existence.
    """
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            # Check if process is still alive (Windows)
            if sys.platform == "win32":
                import ctypes
                h = ctypes.windll.kernel32.OpenProcess(0x0400, False, pid)
                if h:
                    ctypes.windll.kernel32.CloseHandle(h)
                    return False  # Process still running
            else:
                # Unix: check /proc (Linux) or use kill(0)
                try:
                    import signal
                    os.kill(pid, 0)  # signal 0 = existence check
                    return False  # Process still running
                except (OSError, ImportError):
                    pass  # Process not found or /proc unavailable
        except Exception:
            pass  # Stale lock file — will be overwritten
    LOCK_FILE.write_text(str(os.getpid()))
    return True


def release_lock():
    """Release the file lock."""
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# ── State persistence ────────────────────────────────────────────────────

def load_state(path):
    """Load pipeline state from a JSON file. Returns default state if missing."""
    path = Path(path)
    if not path.exists():
        return {"files": {}, "dataset_id": "", "updated_at": ""}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path, state):
    """Atomically save pipeline state to a JSON file (atomic via .tmp + replace)."""
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _load_meta(pdf_path: Path) -> dict:
    """Load metadata sidecar (.meta.json) if it exists alongside the PDF."""
    meta_path = pdf_path.with_suffix('.meta.json')
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding='utf-8'))
        except Exception:
            pass
    return {}


# ── OCR helpers ──────────────────────────────────────────────────────────

def _ocr_chunked(pdf_path, page_count, token, lang, key, api_version="v1", model=None):
    """OCR a large PDF by splitting into chunks.

    v1: 20-page chunks (≤10MB limit, no token needed)
    v4: 200-page chunks (≤200MB limit, needs MINERU_TOKEN)

    Each chunk saved to <pdf_parent>/_ocr/<key>/full_N.md for resumability.
    Returns (merged_path, merged_text).
    """
    chunk_size = 20 if api_version == "v1" else 200
    chunks = [(i * chunk_size + 1, min((i + 1) * chunk_size, page_count))
              for i in range((page_count + chunk_size - 1) // chunk_size)]
    print(f"  [{key[:40]}] {page_count}p → {len(chunks)} {api_version} chunks")
    chunk_dir = _ocr_dir(pdf_path, key)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    parts = []
    import mineru_api as _ma
    req_model = (model or "pipeline").strip().lower()
    for i, (s, e) in enumerate(chunks):
        out_md = chunk_dir / f"full_{i+1}.md"
        cache_model = _chunk_cache_model(chunk_dir, i + 1)
        # Incremental cache: reuse chunk iff it matches the requested model.
        # Legacy chunks (pre-marker, cache_model None) were created by the
        # default pipeline model → reused for pipeline, re-OCR'd for vlm.
        if out_md.exists() and (cache_model == req_model
                                or (cache_model is None and req_model == "pipeline")):
            text = out_md.read_text("utf-8")
            parts.append(text)
            print(f"    chunk {i+1}/{len(chunks)} cached ({len(text)} chars, model={cache_model or 'pipeline'})")
            continue
        if out_md.exists():
            # stale cache (model mismatch or legacy chunk needing vlm) → re-OCR this chunk only
            print(f"    chunk {i+1}/{len(chunks)} re-OCR (cached model={cache_model or 'legacy'} != {req_model})")
        data = submit_by_file(str(pdf_path), api_version=api_version,
                              language=lang, page_ranges=f"{s}-{e}", token=token,
                              model_version=model)
        if api_version == "v4":
            tid = data["data"]["batch_id"]
            d = _poll_ocr_state(
                lambda: _batch_state(tid, token, i + 1, len(chunks)),
                f"chunk {i+1}/{len(chunks)}")
            results = d["data"].get("extract_result", [])
            if not results or "full_zip_url" not in results[0]:
                raise RuntimeError(f"chunk {i+1}: no full_zip_url in done response")
            zip_url = results[0]["full_zip_url"]
            resp = req_lib.get(zip_url, timeout=120)
            import zipfile, tempfile
            with tempfile.TemporaryDirectory() as tmp:
                zpath = Path(tmp) / "c.zip"; zpath.write_bytes(resp.content)
                with zipfile.ZipFile(zpath, "r") as zf:
                    zf.extractall(tmp)
                mds = list(Path(tmp).rglob("*.md"))
                if not mds:
                    raise RuntimeError(f"chunk {i+1}: no md in zip")
                text = mds[0].read_text("utf-8")
                # Preserve images from this chunk
                chunk_images = Path(tmp) / "images"
                if chunk_images.is_dir():
                    import shutil
                    target = chunk_dir / "images"
                    target.mkdir(exist_ok=True)
                    for img in chunk_images.iterdir():
                        shutil.copy2(str(img), str(target / img.name))
        else:
            # v1: simple task polling + markdown URL
            tid = data["data"]["task_id"]
            d = _poll_ocr_state(
                lambda: _task_state(tid, i + 1, len(chunks)),
                f"chunk {i+1}/{len(chunks)}")
            md_url = d["data"]["markdown_url"]
            resp = req_lib.get(md_url, timeout=120)
            resp.encoding = "utf-8"
            text = resp.text
        out_md.write_text(text, "utf-8")
        (chunk_dir / f"full_{i+1}.model").write_text(req_model, "utf-8")
        parts.append(text)
        print(f"    chunk {i+1}/{len(chunks)} done ({len(text)} chars, model={req_model})")
    merged = "\n\n".join(parts)
    merged_path = chunk_dir / "full_merged.md"
    merged_path.write_text(merged, "utf-8")
    return merged_path, merged


def _deposit_md(md_path, dataset_name, pdf_sha, content_sha, key, _update_fn,
                zotero_key=""):
    """
    Optionally deposit OCR output to a structured archive.

    This function loads the optional 'deposit' module (a custom feature).
    If unavailable, it's silently skipped.
    Override or replace this function to implement your own archival logic.
    """
    if not dataset_name:
        return md_path
    try:
        from deposit import deposit_md
        metadata = {"pdf_sha256": pdf_sha} if pdf_sha else {}
        if zotero_key:
            metadata["zotero_key"] = zotero_key
        dep_result = deposit_md(
            Path(md_path), dataset=dataset_name,
            metadata=metadata or None)
        if dep_result.error == "":
            _update_fn("ocr_done",
                       ocr_file=str(dep_result.canonical_path),
                       sha256=content_sha)
            print(f"  [{key[:40]}] deposited → {dep_result.canonical_path}")
            return dep_result.canonical_path
    except ImportError:
        pass
    return md_path


# ── Per-file processing ──────────────────────────────────────────────────

def process_one(pdf_path, dataset_id, state, state_file, client, lang, pages,
                dataset_name: str = "", fire_and_forget: bool = True,
                model: str = None, force: bool = False) -> dict:
    """
    OCR → upload → parse for a single PDF. Independent per-file processing.

    Pipeline steps:
      1. OCR via MinerU API (v1 or v4, auto-detected by file size)
      2. (optional) Upload markdown to RAGFlow
      3. (optional) Start / wait for RAGFlow parsing

    With fire_and_forget=True: uploads + starts parse, marks 'parsing',
    returns immediately. Worker released for next OCR. Use 'reap' subcommand
    to collect parse results afterwards.

    If dataset_id is empty/falsy, RAGFlow steps (2 and 3) are skipped entirely.
    The pipeline will only perform OCR and save the markdown output.
    """
    key = pdf_path.name
    ragflow_name = key.rsplit(".", 1)[0] + ".md"  # .md for RAGFlow
    info = state["files"].get(key, {"status": "pending"})
    result = {"file": key}
    # Load paper metadata (e.g., title, authors, DOI) from sidecar file
    paper_meta = _load_meta(pdf_path)
    zotero_key = paper_meta.get("zotero_key", "") if paper_meta else ""

    has_ragflow = bool(dataset_id) and client is not None

    def _update(s, **kw):
        info["status"] = s
        info.update(kw)
        state["files"][key] = info
        save_state(state_file, state)

    # ── Step 1: OCR (submit → poll → download) ──
    if info["status"] in ("pending",):
        # Check if OCR output is already cached via the optional 'deposit' module
        cached = False
        pdf_sha = ""
        try:
            from deposit import _sha256_file, _manifest
            pdf_sha = _sha256_file(pdf_path)
            existing = _manifest.lookup_pdf(pdf_sha)
            if existing:
                # Cached OCR path — you can customize this to your own cache dir
                cache_base = Path(os.environ.get("MINERU_CACHE_DIR", str(Path.home() / ".mineru_cache")))
                canonical = cache_base / existing["_key"] / "full.md"
                if canonical.exists():
                    content = canonical.read_text(encoding="utf-8")
                    content_sha = hashlib.sha256(content.encode()).hexdigest()
                    _update("ocr_done", ocr_file=str(canonical), sha256=content_sha)
                    print(f"  [{key[:40]}] OCR cached (pdf_sha={pdf_sha[:8]})")
                    cached = True
        except (ImportError, FileNotFoundError):
            pass

        if cached:
            pass  # Skip OCR, state already updated
        else:
            print(f"  [{key[:40]}] OCR ...")
            try:
                # Get page count for chunking decisions
                page_count = None
                try:
                    from pypdf import PdfReader
                    reader = PdfReader(str(pdf_path))
                    page_count = len(reader.pages)
                except Exception:
                    pass

                # Let mineru_api auto-detect v1 vs v4
                from mineru_api import _auto_detect_api_version, _env_token
                api_version = _auto_detect_api_version(str(pdf_path))
                token = _env_token()
                file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
                print(f"  [{key[:40]}] {file_size_mb:.0f}MB, {page_count or '?'}pp → {api_version}")

                # Chunked path for large PDFs
                CHUNK_LIMIT = 20 if api_version == "v1" else 200
                if page_count and page_count > CHUNK_LIMIT:
                    md_path, content = _ocr_chunked(pdf_path, page_count, token, lang, key, api_version, model=model)
                    content_sha = hashlib.sha256(content.encode()).hexdigest()
                    _update("ocr_done", ocr_file=str(md_path), sha256=content_sha)
                    print(f"  [{key[:40]}] merged → {len(content)} chars")
                    _deposit_md(md_path, dataset_name, pdf_sha, content_sha, key, _update,
                                zotero_key=zotero_key)
                else:
                    data = submit_by_file(str(pdf_path),
                                          language=lang, token=token,
                                          model_version=model)
                    # v1 returns task_id, v4 batch returns batch_id
                    tid = data["data"].get("task_id") or data["data"]["batch_id"]
                    is_batch = "batch_id" in data["data"] and "task_id" not in data["data"]

                    # Poll until OCR complete (state-aware: only abort on
                    # terminal failures; queue/processing states keep waiting)
                    import mineru_api as _ma
                    if is_batch:
                        d = _poll_ocr_state(
                            lambda: _batch_state(tid, token, 0, 0),
                            key[:40])
                    else:
                        d = _poll_ocr_state(
                            lambda: _task_state(tid, 0, 0),
                            key[:40])
                    task_data = d

                    # Download OCR result
                    out_dir = _ocr_dir(pdf_path)
                    out_dir.mkdir(exist_ok=True)
                    if is_batch:
                        # v4 batch: download zip, extract markdown
                        results = task_data["data"].get("extract_result", [])
                        if not results:
                            raise RuntimeError("No results in batch")
                        zip_url = results[0].get("full_zip_url")
                        if not zip_url:
                            raise RuntimeError(f"No zip URL: {list(results[0].keys())}")
                        resp = req_lib.get(zip_url, timeout=120)
                        import zipfile, tempfile
                        with tempfile.TemporaryDirectory() as tmp:
                            zpath = Path(tmp) / "result.zip"
                            zpath.write_bytes(resp.content)
                            with zipfile.ZipFile(zpath, "r") as zf:
                                zf.extractall(tmp)
                            mds = list(Path(tmp).rglob("*.md"))
                            if mds:
                                # Copy to stable _ocr/ dir before temp dir is cleaned
                                md_name = pdf_path.stem + ".md"
                                md_path = out_dir / md_name
                                md_path.write_text(mds[0].read_text("utf-8"), "utf-8")
                                # Also preserve images/ directory if present (vlm model)
                                images_dir = Path(tmp) / "images"
                                if images_dir.is_dir():
                                    import shutil
                                    target_images = out_dir / "images"
                                    if target_images.exists():
                                        shutil.rmtree(target_images)
                                    shutil.copytree(str(images_dir), str(target_images))
                            else:
                                md_path = None
                    else:
                        saved = download_result(task_data, str(out_dir), api_version=api_version)
                        md_path = saved[0] if saved else None
                    if not md_path or not md_path.exists():
                        raise RuntimeError("No markdown output")
                    content = md_path.read_text(encoding="utf-8")
                    content_sha = hashlib.sha256(content.encode()).hexdigest()
                    _update("ocr_done", ocr_file=str(md_path), sha256=content_sha)
                    print(f"  [{key[:40]}] OCR done ({len(content)} chars)")
                    _deposit_md(md_path, dataset_name, pdf_sha, content_sha, key, _update,
                                zotero_key=zotero_key)
            except Exception as e:
                _update("failed", error=f"OCR: {e}")
                print(f"  [{key[:40]}] OCR FAIL: {e}")
                return result

    # ── Step 2: Upload to RAGFlow (skipped if no dataset_id) ──
    if has_ragflow and info["status"] in ("ocr_done",):
        md_path = Path(info.get("ocr_file", ""))
        stored_sha = info.get("sha256", "")
        # Re-verify file integrity: recompute SHA from file bytes
        if not md_path.exists():
            _update("failed", error="OCR file missing")
            print(f"  [{key[:40]}] FAIL: file missing"); return result
        content_sha = hashlib.sha256(md_path.read_bytes()).hexdigest()
        if stored_sha and content_sha != stored_sha:
            print(f"  [{key[:40]}] sha256 updated (file modified since OCR)")
        _update("ocr_done", sha256=content_sha)
        # Dedup check: by PDF SHA256 (source identity) + name.
        # PDF SHA is more reliable than content SHA — OCR minor variations
        # between runs don't change source identity.
        # PITFALL: using content_sha for dedup creates duplicates on re-OCR
        # because markdown content varies slightly across OCR runs.
        if not pdf_sha:
            try:
                from deposit import _sha256_file
                pdf_sha = _sha256_file(pdf_path)
            except ImportError:
                pdf_sha = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
        existing = client.get_existing_docs(dataset_id)
        if ragflow_name in existing and existing[ragflow_name].get("pdf_sha") == pdf_sha:
            if force:
                _update("ocr_done")  # allow re-upload
                print(f"  [{key[:40]}] force-updating (pdf_sha match, --force)")
            else:
                _update("done")
                print(f"  [{key[:40]}] already in RAGFlow (pdf_sha match)"); return result
        # Cross-name dedup: same PDF under different name
        matched_name = None
        for ename, emeta in existing.items():
            if isinstance(emeta, dict) and emeta.get("pdf_sha") == pdf_sha:
                matched_name = ename; break
        if matched_name:
            if force:
                print(f"  [{key[:40]}] force-updating (replaces '{matched_name}')")
            else:
                _update("done")
                print(f"  [{key[:40]}] already in RAGFlow as '{matched_name}'"); return result
        print(f"  [{key[:40]}] uploading ...")
        try:
            content = md_path.read_text(encoding="utf-8")
            # Build meta_fields: dedup hash + paper metadata
            meta_fields = {"sha256": content_sha, "pdf_sha": pdf_sha}
            if paper_meta:
                if paper_meta.get("title"):
                    meta_fields["title"] = paper_meta["title"]
                if paper_meta.get("year"):
                    meta_fields["year"] = paper_meta["year"]
                if paper_meta.get("doi"):
                    meta_fields["doi"] = paper_meta["doi"]
                if paper_meta.get("journal"):
                    meta_fields["journal"] = paper_meta["journal"]
                authors = paper_meta.get("authors", [])
                if authors:
                    meta_fields["authors"] = ", ".join(
                        a.get("lastName", a.get("name", "")) for a in authors[:5])
            resp = client.upload_content(dataset_id, ragflow_name, content,
                                         meta_fields=meta_fields)
            doc_id = resp.get("data", [{}])[0].get("id")
            if not doc_id:
                raise RuntimeError("No doc_id")
            _update("uploaded", doc_id=doc_id)
            print(f"  [{key[:40]}] uploaded")
        except Exception as e:
            _update("failed", error=f"Upload: {e}")
            print(f"  [{key[:40]}] upload FAIL: {e}"); return result

    # ── Step 3: Parse (RAGFlow) — skipped if no dataset_id ──
    if has_ragflow and info["status"] in ("uploaded",):
        doc_id = info.get("doc_id", "")
        try:
            client.start_parse(dataset_id, [doc_id])
        except Exception:
            pass

        if fire_and_forget:
            _update("parsing")
            print(f"  [{key[:40]}] parse started (async)")
            return result

        # Legacy: block until parse complete
        start = time.time()
        while time.time() - start < 300:
            try:
                resp = client.list_documents(dataset_id, page_size=50)
                for doc in resp.get("data", {}).get("docs", []):
                    if doc.get("id") == doc_id:
                        run = doc.get("run", "")
                        if run == "DONE":
                            _update("done")
                            print(f"  [{key[:40]}] parse done "
                                  f"({int(time.time()-start)}s)")
                            return result
                        elif run in ("FAIL", "CANCEL"):
                            _update("failed", error=f"Parse {run}")
                            print(f"  [{key[:40]}] parse {run}")
                            return result
                time.sleep(2)
            except Exception:
                time.sleep(2)
        _update("failed", error="Parse timeout")
        print(f"  [{key[:40]}] parse timeout")

    # If no RAGFlow, mark OCR-only as done
    if not has_ragflow and info["status"] in ("ocr_done",):
        _update("done")
        print(f"  [{key[:40]}] OCR complete (RAGFlow skipped)")

    return result


# ── Subcommands ──────────────────────────────────────────────────────────

def cmd_process(args):
    if not acquire_lock():
        sys.exit(f"Another instance running. Delete {LOCK_FILE} if stale.")

    # Create RAGFlow client only if dataset_id is provided
    client = None
    if args.dataset_id:
        client = RAGFlowClient()

    state = load_state(args.state_file)
    state["dataset_id"] = args.dataset_id or ""

    pdfs = [Path(p) for p in args.files]
    for p in pdfs:
        if not p.is_file() or p.suffix.lower() != ".pdf":
            sys.exit(f"Not a PDF: {p}")
    state.setdefault("files", {})
    for p in pdfs:
        if p.name not in state["files"]:
            state["files"][p.name] = {"status": "pending", "path": str(p.resolve())}
    save_state(args.state_file, state)

    total = len(pdfs)
    done = 0
    failed = 0
    fire_and_forget = args.fire_and_forget
    mode = "fire-and-forget" if fire_and_forget else "blocking"
    ragflow_mode = "OCR+RAGFlow" if args.dataset_id else "OCR only"
    print(f"Pipeline: {total} PDFs → {ragflow_mode} "
          f"(concurrency={args.concurrency}, {mode})\n")

    dataset_name = getattr(args, "dataset_name", "") or ""

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {ex.submit(process_one, p, args.dataset_id, state,
                             args.state_file, client, args.lang, args.pages,
                             dataset_name, fire_and_forget,
                             args.model, args.force): p.name
                   for p in pdfs}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
                st = state["files"].get(name, {}).get("status", "?")
                if st == "done": done += 1
                elif st == "failed": failed += 1
            except Exception as e:
                print(f"  [{name[:40]}] CRASH: {e}")
                failed += 1

    if args.dataset_id and fire_and_forget:
        parsing = sum(1 for i in state["files"].values() if i.get("status") == "parsing")
        print(f"\nOCR+upload done. {parsing} files parsing in background.")
        print(f"Run 'python pipeline_async.py reap --state-file {args.state_file}' to collect results.")
    else:
        print(f"\nDone: {done}/{total} processed, {failed} failed")
    release_lock()


def cmd_status(args):
    state = load_state(args.state_file)
    files = state.get("files", {})
    if not files:
        print("No state found."); return
    from collections import Counter
    counts = Counter(info.get("status", "?") for info in files.values())
    total = len(files)

    # Progress bar
    done = counts.get("done", 0)
    failed = counts.get("failed", 0)
    parsing = counts.get("parsing", 0)
    uploaded = counts.get("uploaded", 0)
    pending = counts.get("pending", 0)
    processed = done + failed + parsing  # OCR+upload complete

    w = 40
    bar = ("█" * (done * w // total) +
           "▓" * (parsing * w // total) +
           "░" * (pending * w // total) +
           "✗" * ((w - (done + parsing + pending) * w // total) if failed else 0))

    print(f"Dataset: {state.get('dataset_id', '?')}")
    print(f"Updated: {state.get('updated_at', '?')}")
    print(f"\n  {processed}/{total} OCR'd  {done} done  {parsing} parsing  {pending} pending  {failed} failed")
    print(f"  {bar}")
    print(f"  █ done  ▓ parsing  ░ pending  ✗ failed\n")

    for name, info in files.items():
        err = info.get("error", "")
        print(f"  [{info.get('status','?'):<10}] {name[:60]}{' — '+err[:60] if err else ''}")


def cmd_reap(args):
    """Poll all files with status 'parsing' and collect RAGFlow parse results."""
    client = RAGFlowClient()
    state = load_state(args.state_file)
    files = state.get("files", {})
    dataset_id = state.get("dataset_id")
    watch = getattr(args, "watch", False)

    if not dataset_id:
        sys.exit("No dataset_id in state file. Run 'process' with --dataset-id first.")

    def _reap_once(quiet=False):
        nonlocal state
        parsing = {n: i for n, i in files.items() if i.get("status") == "parsing"}
        if not parsing:
            return 0, 0, 0  # done, failed, still

        try:
            resp = client.list_documents(dataset_id, page_size=50)
            docs = resp.get("data", {}).get("docs", [])
        except Exception as e:
            if not quiet:
                print(f"Failed to list documents: {e}")
            return 0, 0, len(parsing)

        doc_map = {d["id"]: d for d in docs}
        done, failed, still = 0, 0, 0

        for name, info in parsing.items():
            doc_id = info.get("doc_id", "")
            if not doc_id or doc_id not in doc_map:
                still += 1
                continue
            doc = doc_map[doc_id]
            run = doc.get("run", "")
            if run == "DONE":
                info["status"] = "done"
                info["parsed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                done += 1
                if not quiet:
                    print(f"  [{name[:40]}] DONE")
            elif run in ("FAIL", "CANCEL"):
                info["status"] = "failed"
                info["last_error"] = f"Parse {run}: {doc.get('progress_msg', '')}"
                failed += 1
                if not quiet:
                    print(f"  [{name[:40]}] {run}")
            else:
                still += 1

        state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        save_state(args.state_file, state)
        return done, failed, still

    parsing = {n: i for n, i in files.items() if i.get("status") == "parsing"}
    if not parsing:
        print("No files in 'parsing' state.")
        return

    if not watch:
        print(f"Reaping {len(parsing)} files ...")
        done, failed, still = _reap_once()
        print(f"\nReap complete: {done} done, {failed} failed, {still} still parsing")
        if still:
            print("Re-run 'reap' later to collect remaining.")
        return

    # ── Watch mode: live progress bar ──
    print(f"Watching {len(parsing)} parsing files (Ctrl+C to stop)...\n")
    inter = getattr(args, "interval", 5)
    try:
        while True:
            parsing = {n: i for n, i in files.items() if i.get("status") == "parsing"}
            if not parsing:
                break

            # Fetch statuses
            try:
                resp = client.list_documents(dataset_id, page_size=50)
                docs = resp.get("data", {}).get("docs", [])
                doc_map = {d["id"]: d for d in docs}
            except Exception:
                time.sleep(inter)
                continue

            # Count states
            states = {"RUNNING": 0, "UNSTART": 0, "DONE": 0, "FAIL": 0, "CANCEL": 0, "?": 0}
            for name, info in parsing.items():
                doc = doc_map.get(info.get("doc_id", ""), {})
                run = doc.get("run", "?")
                states[run if run in states else "?"] += 1

            done_now = states["DONE"]
            running = states["RUNNING"] + states["UNSTART"]
            failed_now = states["FAIL"] + states["CANCEL"]
            total_parsing = len(parsing)
            resolved = done_now + failed_now

            # Progress bar
            w = 40
            done_w = done_now * w // total_parsing
            run_w = running * w // total_parsing
            fail_w = failed_now * w // total_parsing
            bar = "█" * done_w + "▓" * run_w + "✗" * fail_w + "░" * (w - done_w - run_w - fail_w)

            elapsed = int(time.time() - (getattr(args, "_start", time.time())))
            print(f"\r  [{elapsed:>4}s] {bar}  {resolved}/{total_parsing} resolved  "
                  f"RUN={running} DONE={done_now} FAIL={failed_now}  ", end="", flush=True)

            # Actually reap the DONE/FAIL ones
            _reap_once(quiet=True)

            time.sleep(inter)
            if not getattr(args, "_start", None):
                args._start = time.time()
    except KeyboardInterrupt:
        pass

    # Final reap + summary
    done, failed, still = _reap_once()
    final = sum(1 for i in files.values() if i.get("status") == "done")
    final_fail = sum(1 for i in files.values() if i.get("status") == "failed")
    print(f"\n\nDone: {final} parsed, {final_fail} failed"
          f"{', ' + str(still) + ' still parsing' if still else ''}")


def cmd_resume(args):
    """
    Resume an interrupted pipeline run from a state file.

    PDFs that were in 'parsing' status are skipped (use 'reap' instead).
    PDFs whose stored paths are missing are searched for in PDF_SEARCH_DIRS.
    """
    if not acquire_lock():
        sys.exit(f"Another instance running.")
    client = None
    state = load_state(args.state_file)
    dataset_id = state.get("dataset_id", "")
    if dataset_id:
        client = RAGFlowClient()

    # Skip 'parsing' files — they need reaping, not re-processing
    pending = {n: i for n, i in state.get("files", {}).items()
               if i.get("status") not in ("done", "failed", "parsing")}
    if not pending:
        # Check if there are parsing files needing reap
        parsing = {n: i for n, i in state.get("files", {}).items()
                   if i.get("status") == "parsing"}
        if parsing:
            print(f"{len(parsing)} files in 'parsing' state — run 'reap' instead.")
        else:
            print("Nothing to resume.")
        release_lock(); return

    print(f"Resuming {len(pending)} files ...")
    dataset_name = getattr(args, "dataset_name", "") or ""
    fire_and_forget = args.fire_and_forget
    done, failed = 0, 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {}
        for name, info in pending.items():
            # Find PDF: first check stored path, then search fallback dirs
            pdf_path = None
            stored = info.get("path", "")
            if stored and Path(stored).exists():
                pdf_path = Path(stored)
            else:
                for d in PDF_SEARCH_DIRS:
                    candidate = d / name
                    if candidate.exists():
                        pdf_path = candidate
                        break
            if pdf_path is None:
                print(f"  [{name[:40]}] PDF not found, skip"); continue
            futures[ex.submit(process_one, pdf_path, dataset_id,
                              state, args.state_file, client,
                              args.lang, args.pages, dataset_name,
                              fire_and_forget, args.model, args.force)] = name
        for fut in as_completed(futures):
            try:
                fut.result()
                s = state["files"].get(futures[fut], {}).get("status", "?")
                if s == "done": done += 1
                elif s == "failed": failed += 1
            except Exception as e:
                failed += 1
    if dataset_id and fire_and_forget:
        parsing = sum(1 for i in state["files"].values() if i.get("status") == "parsing")
        print(f"\nOCR+upload done. {parsing} files parsing in background.")
        print(f"Run 'python pipeline_async.py reap --state-file {args.state_file}' to collect results.")
    else:
        print(f"\nDone: {done} processed, {failed} failed")
    release_lock()


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PDF → OCR pipeline with optional RAGFlow integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # OCR only (no RAGFlow upload)
  %(prog)s process *.pdf

  # OCR + RAGFlow
  %(prog)s process *.pdf --dataset-id <ragflow-dataset-id>

  # Higher concurrency + custom state file
  %(prog)s process *.pdf --dataset-id <id> --concurrency 8 --state-file ./my_state.json

  # Resume interrupted run
  %(prog)s resume --state-file ./my_state.json

Environment:
  MINERU_TOKEN          MinerU API token (required for v4/Precision API on files >10MB)
  RAGFLOW_URL           RAGFlow server URL (default: http://localhost:9380)
  RAGFLOW_API_KEY       RAGFlow API key (required if --dataset-id is used)
  PIPELINE_PDF_SEARCH_DIRS  Semicolon-separated dirs for resume PDF search (default: Downloads, Desktop)
  MINERU_CACHE_DIR      Directory for cached OCR results (default: ~/.mineru_cache)
""",
    )
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    sub = parser.add_subparsers(dest="command")

    # ── process ──
    p = sub.add_parser("process", help="OCR + optional upload to RAGFlow")
    p.add_argument("files", nargs="+", help="PDF files to process")
    p.add_argument("--dataset-id", default=None,
                   help="RAGFlow dataset ID (omit for OCR-only mode)")
    p.add_argument("--dataset-name", default="",
                   help="Dataset name for optional deposit feature")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help="Parallel OCR workers (default: %(default)s)")
    p.add_argument("--lang", default="en",
                   help="OCR language (default: %(default)s)")
    p.add_argument("--pages", default=None,
                   help="Page range, e.g. '1-10' (default: auto-detect, limited to 20 for v1)")
    p.add_argument("--model", default=None,
                   help="Model version: pipeline (default) or vlm")
    p.add_argument("--force", action="store_true",
                   help="Force re-upload even if PDF SHA256 matches existing doc")
    p.add_argument("--fire-and-forget", action="store_true",
                   help="Upload + start parse, return immediately (use 'reap' to collect)")
    p.add_argument("--blocking", action="store_false", dest="fire_and_forget",
                   help="Block until parse completes (legacy behavior)")
    p.set_defaults(func=cmd_process, fire_and_forget=True)

    # ── resume ──
    p2 = sub.add_parser("resume", help="Resume from state file")
    p2.add_argument("--dataset-name", default="",
                    help="Dataset name for optional deposit feature")
    p2.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help="Parallel OCR workers (default: %(default)s)")
    p2.add_argument("--lang", default="en",
                    help="OCR language (default: %(default)s)")
    p2.add_argument("--pages", default=None,
                    help="Page range, e.g. '1-10'")
    p2.add_argument("--model", default=None,
                    help="Model version: pipeline (default) or vlm")
    p2.add_argument("--force", action="store_true",
                    help="Force re-upload even if PDF SHA256 matches existing doc")
    p2.add_argument("--fire-and-forget", action="store_true",
                    help="Upload + start parse, return immediately (use 'reap' to collect)")
    p2.add_argument("--blocking", action="store_false", dest="fire_and_forget",
                    help="Block until parse completes")
    p2.set_defaults(func=cmd_resume, fire_and_forget=True)

    # ── status ──
    p3 = sub.add_parser("status", help="Show pipeline state")
    p3.set_defaults(func=cmd_status)

    # ── reap ──
    p4 = sub.add_parser("reap", help="Collect RAGFlow parse results for 'parsing' files")
    p4.add_argument("--watch", action="store_true",
                    help="Live progress bar, poll until all done")
    p4.add_argument("--interval", type=int, default=5,
                    help="Poll interval in seconds (default: %(default)s)")
    p4.set_defaults(func=cmd_reap)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
