#!/usr/bin/env python3
"""
Shared RAGFlow client — single source of truth for RAGFlow API operations.
Used by ragflow_uploader.py (batch upload) and pipeline.py (end-to-end OCR→upload).

RAGFlow is OPTIONAL. If RAGFLOW_URL or RAGFLOW_API_KEY environment variables
are not set, the client will raise RuntimeError with instructions on first use.

Usage:
    from _ragflow_client import RAGFlowClient, batch_upload_parallel

Configuration:
    Set these environment variables (or add to a project-local .env file):

      RAGFLOW_URL=http://localhost:9380
      RAGFLOW_API_KEY=your_api_key_here
      RAGFLOW_API_KEY_2=your_secondary_api_key  # optional: second tenant
"""
import hashlib
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests


# ── Lazy .env loading (project-local only) ───────────────────────

def _load_dotenv():
    """Read RAGFLOW_* vars from project-local .env if not already in environment.

    Only checks the .env file sitting one directory above this script
    (i.e., the project root). Does NOT search user home directories or
    Hermes profile directories — those are deployment-specific paths
    that don't belong in open-source code.
    """
    dotenv = Path(__file__).resolve().parent.parent / ".env"
    if not dotenv.exists():
        return
    try:
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if "=" not in line or line.startswith("#"):
                continue
            k, v = line.split("=", 1)
            k = k.strip().upper()
            v = v.strip().strip("'\" \\r")
            if k in ("RAGFLOW_URL", "RAGFLOW_API_KEY", "RAGFLOW_API_KEY_2") and not os.environ.get(k):
                os.environ[k] = v
    except Exception:
        pass


# Load .env at import time — this is safe because it does NOT fail
# if the file is missing; it's purely additive.
_load_dotenv()


# ── Lazy API key resolution (no interactive prompts) ─────────────

def _ensure_api_key():
    """Raise RuntimeError if RAGFLOW_API_KEY is not set.

    NOTE: This does NOT prompt interactively or write to disk. If the
    key is missing at API call time, the caller gets a clear error with
    setup instructions. This makes RAGFlow truly optional — the import
    itself never fails, and the error only surfaces when you try to use
    the client without configuring it.
    """
    key = os.environ.get("RAGFLOW_API_KEY", "").strip()
    url = os.environ.get("RAGFLOW_URL", "").strip()
    if not key and not url:
        raise RuntimeError(
            "RAGFlow is not configured.\n\n"
            "  Set these environment variables (or add them to a .env file\n"
            "  in the project root):\n\n"
            "    RAGFLOW_URL=http://localhost:9380\n"
            "    RAGFLOW_API_KEY=your_api_key_here\n\n"
            "  You can obtain an API key from RAGFlow > Settings > API.\n"
            "  If you don't need RAGFlow, skip this step — it's optional."
        )
    if not key:
        raise RuntimeError(
            "RAGFLOW_API_KEY is not set.\n\n"
            "  The RAGFLOW_URL is configured, but no API key is provided.\n"
            "  Set RAGFLOW_API_KEY in your environment or project .env.\n\n"
            "    RAGFLOW_API_KEY=your_api_key_here\n"
        )


# ── Defaults ────────────────────────────────────────────────────

DEFAULT_RAGFLOW_URL = os.environ.get("RAGFLOW_URL", "http://localhost:9380")

# Support multiple tenants: RAGFLOW_API_KEY (primary) and RAGFLOW_API_KEY_2 (secondary)
def _resolve_api_key(tenant: int = 1) -> str:
    """Get API key for a specific tenant from env or .env.

    tenant=1 → RAGFLOW_API_KEY  (primary)
    tenant=2 → RAGFLOW_API_KEY_2 (secondary)
    """
    key_name = "RAGFLOW_API_KEY" if tenant == 1 else "RAGFLOW_API_KEY_2"
    return os.environ.get(key_name, "")


DEFAULT_API_KEY = _resolve_api_key(tenant=1)


# ── Client ───────────────────────────────────────────────────────

class RAGFlowClient:
    """Thin wrapper around RAGFlow HTTP API.

    Usage:
        client = RAGFlowClient()
        datasets = client.list_datasets()

    The client lazily checks for credentials. Importing this class or
    even instantiating it does NOT fail if env vars are missing; only
    actual API calls will raise RuntimeError with setup instructions.
    """

    def __init__(self, base_url: str = DEFAULT_RAGFLOW_URL,
                 api_key: str = DEFAULT_API_KEY):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        # NOTE: We do NOT call _ensure_api_key() here. The check is
        # deferred to the first actual API call so that instantiation
        # never fails — making RAGFlow truly optional at import time.

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make an HTTP request to the RAGFlow API.

        Lazily checks credentials before the first real API call.
        """
        # Deferred credential check: only fail when you actually try
        # to use the API, not on import or instantiation.
        if not self.api_key and os.environ.get("RAGFLOW_URL", "").strip():
            # URL is set but key is missing — try to re-resolve
            self.api_key = _resolve_api_key(tenant=1)
            if self.api_key:
                self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        _ensure_api_key()

        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 60)
        resp = self.session.request(method, url, timeout=timeout, **kwargs)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(
                f"RAGFlow API error (code={data.get('code')}): "
                f"{data.get('message', data.get('msg', 'unknown'))}"
            )
        return data

    # ── Dataset operations ──────────────────────────────────

    def check_health(self) -> dict:
        """Diagnose all datasets for common issues: Infinity wipe, RAPTOR
        pollution, embedding mismatch, stale metadata."""
        import sys
        ALL_MINILM = "all-minilm:latest@Ollma@Ollama"
        issues = []
        ok = 0
        dead = 0

        datasets = self.list_datasets()
        ds_list = datasets.get("data", []) if isinstance(datasets, dict) else datasets

        emb = ds_list[0].get("embedding_model", "") if ds_list else ""
        if emb and emb != ALL_MINILM:
            issues.append(f"⚠ Embedding model mismatch: current={ALL_MINILM}, "
                          f"datasets built with={emb} → ALL retrievals broken")

        for ds in sorted(ds_list, key=lambda d: d["name"]):
            name = ds["name"]
            chunks_meta = ds.get("chunk_count", 0)
            docs = ds.get("document_count", 0)
            raptor = ds.get("parser_config", {}).get("raptor", {}).get("use_raptor", False)
            graphrag = ds.get("parser_config", {}).get("graphrag", {}).get("use_graphrag", False)

            if docs == 0:
                continue  # empty dataset, skip

            # Real retrieval test
            try:
                r = self.session.post(
                    f"{self.base_url}/api/v1/retrieval",
                    json={"question": "quantum", "dataset_ids": [ds["id"]],
                          "page": 1, "page_size": 1},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    timeout=10,
                )
                retrievable = len(r.json().get("data", {}).get("chunks", []))
            except Exception:
                retrievable = -1

            if chunks_meta > 0 and retrievable == 0:
                issues.append(f"💀 {name}: {chunks_meta} chunks in metadata, 0 retrievable — "
                              f"Infinity data wiped, needs rebuild")
                dead += 1
            elif chunks_meta > 0:
                if raptor:
                    issues.append(f"🦖 {name}: RAPTOR enabled ({docs} docs, {chunks_meta} chunks) "
                                  f"— summaries polluting top-k")
                ok += 1

        print(f"Health: {ok} OK, {dead} dead, {len(issues)} issues", file=sys.stderr)
        for i in issues:
            print(f"  {i}", file=sys.stderr)
        return {"ok": ok, "dead": dead, "issues": issues}

    def list_datasets(self, page: int = 1, page_size: int = 50) -> dict:
        return self._request(
            "GET", f"/api/v1/datasets?page={page}&page_size={page_size}"
        )

    def create_dataset(self, name: str, description: str = "",
                       chunk_method: str = "naive",
                       parser_config: dict | None = None,
                       graphrag_entity_types: list[str] | None = None) -> dict:
        # ═══ Pre-flight checks ═══
        # 1. Warn if dataset with same name already exists
        existing = self.list_datasets()
        if isinstance(existing, dict):
            existing = existing.get("data", [])
        for ds in existing:
            if ds.get("name") == name:
                import sys
                print(f"⚠ WARNING: Dataset '{name}' already exists "
                      f"(id={ds['id'][:16]}, docs={ds.get('document_count',0)}, "
                      f"chunks={ds.get('chunk_count',0)}).",
                      file=sys.stderr)
                print(f"  Creating will produce a DUPLICATE. "
                      f"Delete the old one first if rebuilding.",
                      file=sys.stderr)

        # 2. Check embedding model — changing it breaks old vectors
        emb = existing[0].get("embedding_model", "") if existing else ""
        ALL_MINILM = "all-minilm:latest@Ollma@Ollama"
        if emb and emb != ALL_MINILM:
            import sys
            print(f"⚠ WARNING: Tenant embedding model is '{emb}'. "
                  f"Datasets built with this model will be INCOMPATIBLE "
                  f"with the default '{ALL_MINILM}'.",
                  file=sys.stderr)
            print(f"  Changing embedding model requires rebuilding ALL datasets.",
                  file=sys.stderr)

        body = {"name": name, "description": description,
                "chunk_method": chunk_method}
        if parser_config is not None:
            body["parser_config"] = parser_config
        else:
            pc = {
                "raptor": {"use_raptor": False},
                "graphrag": {"use_graphrag": False},
            }
            if graphrag_entity_types:
                pc["graphrag"]["use_graphrag"] = True
                pc["graphrag"]["entity_types"] = graphrag_entity_types
            body["parser_config"] = pc
        return self._request("POST", "/api/v1/datasets", json=body)

    def delete_dataset(self, dataset_id: str) -> dict:
        return self._request("DELETE", f"/api/v1/datasets/{dataset_id}")

    # ── Document operations ─────────────────────────────────

    def upload_document(self, dataset_id: str, file_path: Path,
                        display_name: str | None = None,
                        meta_fields: dict | None = None) -> dict:
        name = display_name if display_name else file_path.name
        data = {}
        if meta_fields:
            data["meta_fields"] = json.dumps(meta_fields)
        with open(file_path, "rb") as f:
            files = {"file": (name, f, "text/markdown")}
            return self._request(
                "POST",
                f"/api/v1/datasets/{dataset_id}/documents",
                files=files, data=data or None, timeout=120,
            )

    def upload_content(self, dataset_id: str, file_name: str,
                       content: str,
                       meta_fields: dict | None = None) -> dict:
        """Upload file content as string (no local file needed)."""
        data = {}
        if meta_fields:
            data["meta_fields"] = json.dumps(meta_fields)
        return self._request(
            "POST",
            f"/api/v1/datasets/{dataset_id}/documents",
            files={"file": (file_name, content.encode("utf-8"), "text/markdown")},
            data=data or None, timeout=120,
        )

    def list_documents(self, dataset_id: str, page: int = 1,
                       page_size: int = 50,
                       run_status: str | None = None,
                       keywords: str = "") -> dict:
        params = [f"page={page}", f"page_size={page_size}"]
        if run_status:
            params.append(f"run={run_status}")
        if keywords:
            params.append(f"keywords={keywords}")
        return self._request(
            "GET",
            f"/api/v1/datasets/{dataset_id}/documents?{'&'.join(params)}"
        )

    def start_parse(self, dataset_id: str, document_ids: list[str]) -> dict:
        return self._request(
            "POST",
            f"/api/v1/datasets/{dataset_id}/chunks",
            json={"document_ids": document_ids}, timeout=30,
        )

    def delete_documents(self, dataset_id: str,
                         document_ids: list[str]) -> dict:
        return self._request(
            "DELETE", f"/api/v1/datasets/{dataset_id}/documents",
            json={"ids": document_ids},
        )

    def get_existing_docs(self, dataset_id: str) -> dict[str, str | None]:
        """Return {name: sha256_or_none} for all docs in dataset."""
        try:
            resp = self.list_documents(dataset_id, page_size=100)
            docs = resp.get("data", {}).get("docs", [])
            return {
                d["name"]: d.get("meta_fields", {}).get("sha256")
                for d in docs
            }
        except Exception:
            return {}

    def wait_for_parse(self, dataset_id: str, timeout: int = 300):
        """Poll dataset until all documents are DONE/FAIL/CANCEL."""
        start = time.time()
        terminal = {"DONE", "FAIL", "CANCEL"}
        while time.time() - start < timeout:
            resp = self.list_documents(dataset_id, page_size=100)
            docs = resp.get("data", {}).get("docs", [])
            states = {}
            for d in docs:
                s = d.get("run", "?")
                states[s] = states.get(s, 0) + 1
            line = " ".join(f"{k}={v}" for k, v in sorted(states.items()))
            elapsed = int(time.time() - start)
            print(f"\r    [{elapsed}s] {line}", end="", flush=True)
            if all(d.get("run") in terminal for d in docs):
                print()
                return
            time.sleep(5)
        print(" (timeout)")


# ── High-level parallel upload ──────────────────────────────────

def batch_upload_parallel(client: RAGFlowClient, dataset_id: str,
                          upload_tasks: list[dict],
                          concurrency: int = 8) -> list[str]:
    """
    Upload multiple file contents in parallel.

    upload_tasks: list of dicts with keys:
        - name (str): RAGFlow document name
        - content (str): markdown content
        - sha256 (str, optional): pre-computed hash for dedup

    Returns: list of uploaded document IDs.
    """
    if not upload_tasks:
        return []

    existing = client.get_existing_docs(dataset_id)
    doc_ids: list[str] = []
    _lock = threading.Lock()

    def _upload_one(task: dict) -> str | None:
        name = task["name"]
        content = task["content"]
        content_sha256 = task.get("sha256") or hashlib.sha256(
            content.encode()).hexdigest()

        # Dedup against existing
        existing_sha = existing.get(name)
        if existing_sha is not None and existing_sha == content_sha256:
            return None  # skip

        try:
            resp = client.upload_content(
                dataset_id, name, content,
                meta_fields={"sha256": content_sha256},
            )
            doc_id = resp.get("data", [{}])[0].get("id")
            if doc_id:
                existing[name] = content_sha256
                return doc_id
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_upload_one, t): t for t in upload_tasks}
        for fut in as_completed(futures):
            doc_id = fut.result()
            if doc_id:
                doc_ids.append(doc_id)

    return doc_ids


# ── Retrieval test ──────────────────────────────────────────────

def ragflow_retrieval_check(client: RAGFlowClient, dataset_id: str,
                   query: str, top_k: int = 3):
    """Test RAGFlow retrieval on a dataset."""
    try:
        resp = client._request(
            "POST", "/api/v1/retrieval",
            json={
                "question": query,
                "dataset_ids": [dataset_id],
                "page_size": top_k,
                "similarity_threshold": 0.2,
            },
            timeout=30,
        )
        chunks = resp.get("data", {}).get("chunks", [])
        print(f"\n  Test query: {query!r}")
        print(f"  Found {len(chunks)} chunks:")
        for i, c in enumerate(chunks):
            content = c.get("content", "")[:120].replace("\n", " ")
            doc = c.get("document_name", "?")
            print(f"    [{i+1}] {doc[:70]}")
            print(f"        {content!r}...")
    except Exception as e:
        print(f"  Retrieval test failed: {e}")
