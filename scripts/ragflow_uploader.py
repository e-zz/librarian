#!/usr/bin/env python3
"""
RAGFlow Bulk Uploader — upload markdown files (e.g., MinerU OCR output)
into a RAGFlow dataset.

Features:
  - Uploads files to a RAGFlow dataset via HTTP API
  - Manages a queue: upload → start parse → poll until done
  - State persistence (resume interrupted batches)
  - Concurrent upload with configurable thread count
  - SHA256-based dedup to avoid re-uploading duplicates
  - Dry-run mode to preview what would be uploaded

Setup:
  pip install requests pyyaml

  # Set RAGFlow credentials (or use --url / --api-key arguments)
  export RAGFLOW_URL=http://localhost:9380        # Linux/Mac
  set RAGFLOW_URL=http://localhost:9380           # Windows
  export RAGFLOW_API_KEY=ragflow-xxxxxxxxxxxxxxxx

Usage:
  # List all datasets
  python ragflow_uploader.py datasets

  # Create a new dataset
  python ragflow_uploader.py create-dataset "My Papers"

  # Upload markdown files from a directory
  python ragflow_uploader.py upload ./_raw --dataset-id <id>

  # Upload + wait for parsing to complete
  python ragflow_uploader.py upload ./_raw --dataset-id <id> --wait

  # Upload with parallel uploads
  python ragflow_uploader.py upload ./_raw --dataset-id <id> --concurrency 8

  # Resume an interrupted batch
  python ragflow_uploader.py resume

  # Check status of a dataset's documents
  python ragflow_uploader.py status --dataset-id <id>

  # Dry run: show what would be uploaded
  python ragflow_uploader.py upload ./_raw --dataset-id <id> --dry-run
"""

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
import yaml

# Fix encoding for Windows consoles (cp1252 can't handle unicode chars)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Make sibling modules importable ──────────────────────────────────────
# This ensures _ragflow_client.py in the same directory is importable
# regardless of how this script is launched.
_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

# ── Shared client from _ragflow_client.py ────────────────────
from _ragflow_client import RAGFlowClient, batch_upload_parallel, ragflow_retrieval_check as retrieval_check

# State file path: can be overridden via RAGFLOW_STATE env var.
# Default is a local file in the current working directory.
STATE_FILE = Path(os.environ.get("RAGFLOW_STATE", "ragflow_upload_state.json"))
DEFAULT_POLL_INTERVAL = 5  # seconds between status checks
DEFAULT_TIMEOUT = 3600      # 1 hour max wait for parsing


# ── Upload Queue Manager ─────────────────────────────────────────────────


class UploadManager:
    """
    Manages the upload + parse pipeline with state persistence.

    State is stored as JSON in a state file for resume support:

    State file structure:
      {
        "dataset_id": "xxx",
        "files": {
          "display_name.md": {
            "path": "/abs/path/to/file.md",
            "display_name": "display_name.md",
            "sha256": "abc123...",
            "status": "pending|uploaded|parsing|done|failed",
            "doc_id": "xxx",        # assigned by RAGFlow after upload
            "attempts": 0,
            "last_error": null,
            "uploaded_at": null,
            "parsed_at": null
          }
        },
        "batch_size": 5,
        "created_at": "2026-06-25T..."
      }
    """

    def __init__(self, client: RAGFlowClient, state_file: Path = STATE_FILE):
        self.client = client
        self.state_file = state_file
        self.state: dict = {}
        self._lock = threading.Lock()

    def _load_state(self) -> dict:
        """Load state from the JSON state file."""
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {}

    def _save_state(self):
        """Persist current state to the JSON state file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self.state, indent=2, ensure_ascii=False))

    def discover_files(self, raw_dir: Path) -> list[Path]:
        """Find all .md files in the given directory (non-recursive)."""
        return sorted(raw_dir.glob("*.md"))

    def init_batch(self, dataset_id: str, files: list[Path], batch_size: int = 5):
        """
        Initialize a new upload batch.

        For files in subdirectories (recursive scan), uses the parent directory name
        as the RAGFlow document name (since MinerU output is always named full.md).
        Also computes SHA256 of each file for hash-based dedup.
        """
        self.state = {
            "dataset_id": dataset_id,
            "files": {},
            "batch_size": batch_size,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        for f in files:
            # If file is "full.md" in a subdirectory, use parent dir as display name
            if f.parent.name and f.name == "full.md":
                display_name = f.parent.name + ".md"
            else:
                display_name = f.name
            # Compute content SHA256 for dedup
            sha256_hash = hashlib.sha256()
            try:
                with open(f, "rb") as fh:
                    for chunk in iter(lambda: fh.read(65536), b""):
                        sha256_hash.update(chunk)
                content_sha256 = sha256_hash.hexdigest()
            except Exception:
                content_sha256 = None
            key = display_name  # Used as state key and RAGFlow document name
            self.state["files"][key] = {
                "path": str(f.resolve()),
                "display_name": display_name,
                "sha256": content_sha256,
                "status": "pending",
                "doc_id": None,
                "attempts": 0,
                "last_error": None,
                "uploaded_at": None,
                "parsed_at": None,
            }
        self._save_state()

    def resume(self) -> dict:
        """Load existing state from file (for resuming interrupted batches)."""
        self.state = self._load_state()
        if not self.state:
            raise RuntimeError("No state file found to resume from.")
        return self.state

    def _upload_one(self, name: str, info: dict, dataset_id: str,
                    dry_run: bool, results: dict) -> None:
        """
        Upload a single file to RAGFlow. Thread-safe via self._lock.

        Skips files that are already uploaded/parsing/done, or failed after
        3 attempts.
        """
        if info["status"] in ("uploaded", "parsing", "done"):
            return
        if info["status"] == "failed" and info["attempts"] >= 3:
            return

        file_path = Path(info["path"])
        if not file_path.exists():
            with self._lock:
                info["status"] = "failed"
                info["last_error"] = "File not found"
            print(f"  SKIP (missing): {name}")
            with self._lock:
                results["failed"] += 1
            return

        if dry_run:
            with self._lock:
                info["status"] = "uploaded"
                info["doc_id"] = "dry-run-id"
                results["uploaded"] += 1
            print(f"  [DRY RUN] Would upload: {name}")
            return

        print(f"  Uploading: {name} ({file_path.stat().st_size / 1024:.0f} KB)")
        with self._lock:
            info["attempts"] += 1
        try:
            display_name = info.get("display_name", name)
            meta = {}
            if info.get("sha256"):
                meta["sha256"] = info["sha256"]
            resp = self.client.upload_document(dataset_id, file_path,
                                               display_name=display_name,
                                               meta_fields=meta if meta else None)
            docs = resp.get("data", [])
            if docs:
                with self._lock:
                    info["doc_id"] = docs[0].get("id")
                    info["status"] = "uploaded"
                    info["uploaded_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                    results["uploaded"] += 1
                print(f"    -> doc_id: {info['doc_id']}")
            else:
                raise RuntimeError(f"No document ID in response: {resp}")
        except Exception as e:
            with self._lock:
                info["status"] = "failed"
                info["last_error"] = str(e)
                results["failed"] += 1
            print(f"    FAILED: {e}")

    def upload_all(self, wait: bool = True, dry_run: bool = False,
                   max_workers: int = 1) -> dict:
        """
        Three-phase upload pipeline.

        Phase 1: Upload all pending files (parallel when max_workers > 1).
        Phase 2: Start parse on all uploaded files.
        Phase 3 (if wait): Poll until all done.

        Args:
            wait: If True, poll until parsing completes for all files.
            dry_run: If True, preview what would be uploaded without doing it.
            max_workers: Concurrent upload threads (1 = sequential).
                         Recommended: 4 for local network, 8 max for remote.
        """
        if not self.state:
            raise RuntimeError("No batch initialized. Run init_batch first.")

        dataset_id = self.state["dataset_id"]
        results = {"uploaded": 0, "failed": 0, "parsed": 0}
        seq = max_workers <= 1

        # ── Phase 1: Upload ─────────────────────────────────
        print(f"\n--- Phase 1: Upload ({'sequential' if seq else f'parallel={max_workers}'}) ---")
        pending = [
            (name, info)
            for name, info in sorted(self.state["files"].items())
            if info["status"] not in ("uploaded", "parsing", "done")
            and not (info["status"] == "failed" and info["attempts"] >= 3)
        ]

        if seq:
            # Sequential upload (backward compatible)
            for name, info in pending:
                self._upload_one(name, info, dataset_id, dry_run, results)
                self._save_state()
                if not dry_run:
                    time.sleep(0.5)
        else:
            # Parallel upload via ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {
                    ex.submit(self._upload_one, name, info, dataset_id,
                              dry_run, results): name
                    for name, info in pending
                }
                for fut in as_completed(futures):
                    name = futures[fut]
                    try:
                        fut.result()
                    except Exception as e:
                        print(f"  UNEXPECTED error for {name}: {e}")
            # Save state once after all parallel uploads complete
            self._save_state()

        # ── Phase 2: Start Parse ────────────────────────────
        print("\n--- Phase 2: Start Parsing ---")
        uploaded_ids = [
            info["doc_id"]
            for name, info in sorted(self.state["files"].items())
            if info["status"] == "uploaded" and info["doc_id"]
        ]

        if dry_run:
            print(f"  [DRY RUN] Would start parse on {len(uploaded_ids)} documents")
            for name in self.state["files"]:
                if self.state["files"][name]["status"] == "uploaded":
                    self.state["files"][name]["status"] = "done"
            return results

        # Start parse on all uploaded documents at once — RAGFlow queues internally
        if uploaded_ids:
            try:
                self.client.start_parse(dataset_id, uploaded_ids)
                for doc_id in uploaded_ids:
                    for name, info in self.state["files"].items():
                        if info.get("doc_id") == doc_id:
                            info["status"] = "parsing"
                print(f"  Parse started for all {len(uploaded_ids)} docs")
            except Exception as e:
                print(f"  FAILED to start parse: {e}")
            self._save_state()

        if not wait:
            print("\nUpload + parse start complete. Use 'status' to check progress.")
            return results

        # ── Phase 3: Poll until all documents are parsed ────
        print("\n--- Phase 3: Polling (Ctrl+C to stop, can resume later) ---")
        return self._poll_until_done(results)

    def _poll_until_done(self, results: dict) -> dict:
        """
        Poll RAGFlow document statuses until all are done or failed.

        Shows a live progress line. State is saved after each poll cycle
        so the batch can be resumed if interrupted.
        """
        dataset_id = self.state["dataset_id"]
        start = time.time()

        while True:
            elapsed = time.time() - start
            if elapsed > DEFAULT_TIMEOUT:
                print(f"\nTimeout after {DEFAULT_TIMEOUT}s — progress saved to state file.")
                return results

            # Fetch all docs in the dataset
            try:
                resp = self.client.list_documents(dataset_id, page_size=100)
            except Exception as e:
                print(f"\n  API error: {e} — retrying...")
                time.sleep(DEFAULT_POLL_INTERVAL)
                continue

            docs = resp.get("data", {}).get("docs", [])
            statuses = {}
            for doc in docs:
                doc_id = doc["id"]
                run = doc.get("run", "UNKNOWN")
                progress = doc.get("progress", 0)
                progress_msg = doc.get("progress_msg", "")
                statuses[doc_id] = (run, progress, progress_msg)

            # Update state based on current statuses
            done_count = 0
            failed_count = 0
            running_count = 0
            for name, info in self.state["files"].items():
                doc_id = info.get("doc_id")
                # Files skipped as duplicates have no doc_id — treat as done
                if not doc_id and info.get("status") in ("uploaded", "done"):
                    done_count += 1
                    continue
                if not doc_id or doc_id not in statuses:
                    continue
                run, progress, msg = statuses[doc_id]
                if run == "DONE":
                    if info["status"] != "done":
                        info["status"] = "done"
                        info["parsed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                        results["parsed"] += 1
                    done_count += 1
                elif run == "FAIL":
                    info["status"] = "failed"
                    info["last_error"] = msg
                    failed_count += 1
                elif run in ("RUNNING", "UNSTART"):
                    running_count += 1
                elif run == "CANCEL":
                    info["status"] = "failed"
                    info["last_error"] = "Cancelled"
                    failed_count += 1

            total = len(self.state["files"])
            status_line = (
                f"\r  [{int(elapsed)}s] DONE={done_count} "
                f"RUNNING={running_count} FAILED={failed_count} "
                f"of {total}"
            )
            print(status_line, end="", flush=True)

            if done_count + failed_count >= total:
                print()
                self._save_state()
                return results

            self._save_state()
            time.sleep(DEFAULT_POLL_INTERVAL)


# ── CLI helpers ─────────────────────────────────────────────────────────


def resolve_client(args) -> RAGFlowClient:
    """
    Create a RAGFlowClient from CLI args or environment variables.

    Precedence: CLI args > env vars > defaults.
    """
    from _ragflow_client import DEFAULT_RAGFLOW_URL, DEFAULT_API_KEY
    url = args.url or DEFAULT_RAGFLOW_URL
    key = args.api_key or DEFAULT_API_KEY
    if not key:
        sys.exit(
            "ERROR: RAGFlow API key required.\n"
            "  Set RAGFLOW_API_KEY env var or pass --api-key."
        )
    return RAGFlowClient(url, key)


# ── Subcommands ─────────────────────────────────────────────────────────


def cmd_datasets(args):
    """List all available RAGFlow datasets."""
    client = resolve_client(args)
    resp = client.list_datasets()
    datasets = resp.get("data", [])
    if not datasets:
        print("No datasets found.")
        return

    if args.json:
        import datetime, tempfile
        items = []
        for ds in datasets:
            items.append({
                'name': ds['name'],
                'id': ds['id'],
                'doc_count': ds.get('doc_num', 0),
                'chunk_count': ds.get('chunk_count', 0),
                'chunk_method': ds.get('chunk_method', ''),
                'status': ds.get('status', ''),
            })
        output = {
            'updated': datetime.datetime.now().isoformat(),
            'datasets': sorted(items, key=lambda x: x['name']),
        }
        if args.output:
            fd, tmp_path = tempfile.mkstemp(suffix='.json',
                                            dir=os.path.dirname(os.path.abspath(args.output)) or '.')
            with os.fdopen(fd, 'w', encoding='utf-8') as tf:
                json.dump(output, tf, indent=2, ensure_ascii=False)
            os.replace(tmp_path, args.output)
            print(f"Written {len(items)} datasets to {args.output}")
        else:
            print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    print(f"{'ID':<36} {'Name':<40} {'Chunks':>8} {'Docs':>6}")
    print("-" * 95)
    for ds in datasets:
        print(
            f"{ds['id']:<36} {ds['name']:<40} "
            f"{ds.get('chunk_count', 0):>8} {ds.get('doc_num', 0):>6}"
        )


def cmd_create_dataset(args):
    """Create a new RAGFlow dataset with optional parser configuration."""
    client = resolve_client(args)
    parser_config = None
    if args.chunk_method == "manual":
        parser_config = {
            "chunk_token_num": args.chunk_size,
            "delimiter": args.delimiter or "\n",
            "html4excel": False,
            "layout_recognize": False,
        }
    elif args.chunk_method == "qa":
        parser_config = {
            "raptor": {"use_raptor": True},
        }
    resp = client.create_dataset(
        name=args.name,
        description=args.description or "",
        chunk_method=args.chunk_method,
        parser_config=parser_config,
    )
    ds = resp.get("data", {})
    print(f"Dataset created: id={ds.get('id')}, name={ds.get('name')}")


def cmd_upload(args):
    """
    Upload markdown files from directory or individual .md file(s) to a dataset.

    Supports:
      - Single .md file
      - Directory of .md files (recursive search for full.md or *.md)
      - Multiple paths (mix of files and directories)
      - SHA256 dedup against existing dataset documents
      - Name collision handling (--force to overwrite)
    """
    client = resolve_client(args)
    files = set()  # Use set to dedup

    for raw_path in args.raw_dir:
        rp = Path(raw_path)
        if rp.is_file() and rp.suffix == ".md":
            files.add(rp)
        elif rp.is_dir():
            # Recurse subdirectories looking for full.md or any .md
            found = set(rp.glob("**/full.md"))
            if not found:
                found = set(rp.glob("*/*.md"))
            if not found:
                found = set(rp.glob("*.md"))
            if not found:
                print(f"  Warning: No .md or full.md files in {rp}")
                continue
            files.update(found)
        else:
            sys.exit(f"Not a file or directory: {rp}")

    if not files:
        sys.exit("No .md files found in any of the specified paths")
    files = sorted(files)

    path_list = ", ".join(str(p) for p in args.raw_dir)
    print(f"Found {len(files)} markdown files in {path_list}")
    for f in files:
        print(f"  {f}")
    print()
    print(f"Target dataset: {args.dataset_id}")
    if args.dry_run:
        print("[DRY RUN — no files will be uploaded]\n")

    manager = UploadManager(client, Path(args.state_file))
    manager.init_batch(args.dataset_id, files, batch_size=args.batch_size)

    # Enrich display names from _manifest.json if available
    # (optional: reads metadata sidecar for nicer document titles)
    first_dir = next((Path(p) for p in args.raw_dir if Path(p).is_dir()), None)
    manifest_path = first_dir.parent / "_manifest.json" if first_dir else None
    if manifest_path and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            file_map = manifest.get("files", {})
            renames = {}  # old_name -> new_name, collected for safe rename
            for name in list(manager.state["files"].keys()):
                base_name = name.replace(".md", "")
                for mpath, mentry in file_map.items():
                    if base_name in mpath and mentry.get("title"):
                        title = mentry["title"]
                        authors = mentry.get("authors", "")
                        nicer = f"{authors} - {title}.md" if authors else f"{title}.md"
                        if nicer != name:
                            renames[name] = nicer
                        break
            for old_name, new_name in renames.items():
                info = manager.state["files"].pop(old_name)
                info["display_name"] = new_name
                manager.state["files"][new_name] = info
                print(f"  [manifest] {old_name[:50]} -> {new_name[:70]}")
            if renames:
                manager._save_state()
        except Exception as e:
            print(f"  (manifest skipped: {e})")

    # Check which files already exist in the dataset — by SHA256 then name
    try:
        existing = client.list_documents(args.dataset_id, page_size=100)
        existing_docs_info = {}  # {name: sha256_or_none}
        for doc in existing.get("data", {}).get("docs", []):
            existing_docs_info[doc["name"]] = doc.get("meta_fields", {}).get("sha256")

        for name, info in list(manager.state["files"].items()):
            local_sha = info.get("sha256")
            existing_sha = existing_docs_info.get(name)
            should_skip = False
            reason = ""
            if local_sha and existing_sha == local_sha:
                should_skip = True
                reason = "SHA256 match (same name)"
            elif local_sha and local_sha in existing_docs_info.values():
                should_skip = True
                reason = "SHA256 match (different name)"

            # Name collision — different hash
            elif name in existing_docs_info:
                if not args.force:
                    should_skip = True
                    reason = "name exists (different hash)"
                else:
                    reason = "FORCE re-upload (name collision, diff hash)"
            if should_skip:
                if not args.force:
                    print(f"  SKIP ({reason}): {name}")
                if not args.force:
                    info["status"] = "uploaded"  # treat as uploaded, will not re-upload
            elif args.force and name in existing_docs_info and local_sha != existing_sha:
                print(f"  FORCE re-upload (hash mismatch): {name}")

        manager._save_state()
    except Exception as e:
        print(f"  Warning: Could not check existing docs: {e}")

    results = manager.upload_all(wait=args.wait, dry_run=args.dry_run,
                                  max_workers=args.concurrency)
    print(f"\nUpload complete: {results['uploaded']} uploaded, "
          f"{results.get('parsed', 'N/A')} parsed, {results['failed']} failed")


def cmd_resume(args):
    """Resume an interrupted batch from its state file."""
    client = resolve_client(args)
    manager = UploadManager(client, Path(args.state_file))
    state = manager.resume()

    total = len(state.get("files", {}))
    pending = sum(1 for f in state["files"].values() if f["status"] == "pending")
    uploaded = sum(1 for f in state["files"].values() if f["status"] == "uploaded")
    parsing = sum(1 for f in state["files"].values() if f["status"] == "parsing")
    done = sum(1 for f in state["files"].values() if f["status"] == "done")
    failed = sum(1 for f in state["files"].values() if f["status"] == "failed")

    print(f"Resuming batch for dataset {state['dataset_id']}:")
    print(f"  Total: {total} | Pending: {pending} | Uploaded: {uploaded} | "
          f"Parsing: {parsing} | Done: {done} | Failed: {failed}")

    results = manager.upload_all(wait=args.wait, dry_run=False)
    print(f"\nDone: {results.get('parsed', 'N/A')} parsed, {results['failed']} failed")


def cmd_status(args):
    """Check document parsing status in a dataset."""
    client = resolve_client(args)
    resp = client.list_documents(
        args.dataset_id,
        page_size=100,
        run_status=args.run if args.run else None,
    )
    docs = resp.get("data", {}).get("docs", [])
    if not docs:
        print("No documents found.")
        return

    # Summarize by status
    from collections import Counter
    status_counts = Counter(doc["run"] for doc in docs)
    print(f"\nDataset {args.dataset_id}: {len(docs)} documents")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    if args.verbose:
        print(f"\n{'Name':<50} {'Status':>8} {'Progress':>8} {'Chunks':>8}")
        print("-" * 80)
        for doc in sorted(docs, key=lambda d: d.get("run", "")):
            print(
                f"{doc['name']:<50} {doc.get('run', '?'):>8} "
                f"{doc.get('progress', 0):>7.0%} {doc.get('chunk_count', 0):>8}"
            )


def cmd_delete(args):
    """Delete documents from a dataset by name, ID, or all at once."""
    client = resolve_client(args)
    resp = client.list_documents(args.dataset_id, page_size=100)
    docs = resp.get("data", {}).get("docs", [])

    to_delete = []
    if args.all:
        to_delete = [doc["id"] for doc in docs]
    elif args.names:
        to_delete = [doc["id"] for doc in docs if doc["name"] in args.names]
    elif args.ids:
        to_delete = args.ids

    if not to_delete:
        print("No documents selected for deletion.")
        return

    print(f"Deleting {len(to_delete)} documents...")
    # Delete in batches of 32 (RAGFlow API limit)
    for i in range(0, len(to_delete), 32):
        chunk = to_delete[i : i + 32]
        client.delete_documents(args.dataset_id, chunk)
        print(f"  Deleted {len(chunk)}")
    print("Done.")


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="RAGFlow Bulk Uploader for MinerU OCR results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s datasets                                   List all datasets
  %(prog)s create-dataset "My Papers"                 Create a new dataset
  %(prog)s upload ./_raw --dataset-id <id>            Upload .md files
  %(prog)s upload ./_raw --dataset-id <id> --wait     Upload + wait for parse
  %(prog)s resume                                     Resume interrupted batch
  %(prog)s status --dataset-id <id>                   Check document status
  %(prog)s delete --dataset-id <id> --all             Delete all docs

Environment:
  RAGFLOW_URL        RAGFlow server URL (default: http://localhost:9380)
  RAGFLOW_API_KEY    API key from RAGFlow settings page
  RAGFLOW_STATE      Path to state file (default: ./ragflow_upload_state.json)
""",
    )

    parser.add_argument("--url", default=None, help="RAGFlow server URL")
    parser.add_argument("--api-key", default=None, help="RAGFlow API key")
    parser.add_argument("--state-file", default=str(STATE_FILE),
                        help="State file for resume support (default: %(default)s)")

    sub = parser.add_subparsers(dest="command", help="Commands")

    # datasets
    p_ds = sub.add_parser("datasets", help="List all datasets")
    p_ds.add_argument("--json", action="store_true", help="Output as JSON")
    p_ds.add_argument("--output", default=None, help="Write JSON to file (requires --json)")
    p_ds.set_defaults(func=cmd_datasets)

    # create-dataset
    p_cd = sub.add_parser("create-dataset", help="Create a new dataset")
    p_cd.add_argument("name", help="Dataset name")
    p_cd.add_argument("--description", default="", help="Dataset description")
    p_cd.add_argument("--chunk-method", default="naive",
                      choices=["naive", "manual", "qa", "table", "paper", "book",
                               "laws", "presentation", "picture", "one", "knowledge_graph",
                               "email", "tag"],
                      help="Chunking method (default: %(default)s)")
    p_cd.add_argument("--chunk-size", type=int, default=512,
                      help="Chunk token size for manual method")
    p_cd.add_argument("--delimiter", default=None,
                      help="Chunk delimiter for manual method")
    p_cd.set_defaults(func=cmd_create_dataset)

    # upload
    p_up = sub.add_parser("upload", help="Upload markdown files to dataset")
    p_up.add_argument("raw_dir", nargs="+",
                      help="Path(s) to directory containing .md files, or individual .md file(s)")
    p_up.add_argument("--dataset-id", required=True, help="Target dataset ID")
    p_up.add_argument("--batch-size", type=int, default=5,
                      help="Documents per parse batch (default: %(default)s)")
    p_up.add_argument("--wait", action="store_true",
                      help="Wait for parsing to complete")
    p_up.add_argument("--force", action="store_true",
                      help="Force re-upload even if file name exists in dataset")
    p_up.add_argument("--dry-run", action="store_true",
                      help="Show what would be uploaded without doing it")
    p_up.add_argument("--concurrency", type=int, default=8,
                      help="Parallel upload threads (default: %(default)s, 1=sequential)")
    p_up.set_defaults(func=cmd_upload)

    # resume
    p_resume = sub.add_parser("resume", help="Resume interrupted batch from state file")
    p_resume.add_argument("--wait", action="store_true",
                          help="Wait for parsing to complete")
    p_resume.set_defaults(func=cmd_resume)

    # status
    p_st = sub.add_parser("status", help="Check document parsing status")
    p_st.add_argument("--dataset-id", required=True, help="Dataset ID")
    p_st.add_argument("--run", default=None,
                      choices=["UNSTART", "RUNNING", "CANCEL", "DONE", "FAIL",
                               "0", "1", "2", "3", "4"],
                      help="Filter by run status")
    p_st.add_argument("-v", "--verbose", action="store_true",
                      help="Show per-document details")
    p_st.set_defaults(func=cmd_status)

    # delete
    p_del = sub.add_parser("delete", help="Delete documents from dataset")
    p_del.add_argument("--dataset-id", required=True, help="Dataset ID")
    p_del.add_argument("--all", action="store_true", help="Delete all documents")
    p_del.add_argument("--names", nargs="+", help="Delete by file name(s)")
    p_del.add_argument("--ids", nargs="+", help="Delete by document ID(s)")
    p_del.set_defaults(func=cmd_delete)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
