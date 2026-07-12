# Environment Variable Reference

All `librarian` tools read from environment variables and project-local `.env` files. This page explains what each variable controls, what value to set, and what happens when it's absent.

---

## `MINERU_TOKEN`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `mineru-api`, `pipeline-async` |
| **Value** | API token from https://mineru.net/apiManage |
| **Required?** | No — only needed for **v4 Precision API** (PDFs >20pp or >10MB) |
| **Missing →** | Falls back to **v1 Agent API** (free, no token, ≤20pp / ≤10MB) |

If you only parse short papers (≤20 pages), you never need this token. The v1 API is free and requires no setup.

**Set it when:** your PDFs are longer than 20 pages, or larger than 10 MB, and you want better OCR quality.

---

## `RAGFLOW_URL` + `RAGFLOW_API_KEY`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `pipeline-async`, `ragflow-upload`, `_ragflow_client` |
| **Value** | `RAGFLOW_URL=http://your-server:9380`, `RAGFLOW_API_KEY=ragflow-xxxxxxxxxxxx` |
| **Required?** | No — only needed for RAGFlow upload |
| **Missing →** | Tools skip RAGFlow operations. `pipeline-async` stops after OCR. |

Without these, the pipeline produces markdown files in `_raw/`. You can search them with `grep -r` or any local search tool.

**Get the key:** RAGFlow → Settings → API key.

---

## `MINERU_PDF_LIBRARY`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `deposit` |
| **Value** | Path to the managed PDF library (e.g. `./pdfs` or `/data/papers`) |
| **Required?** | No |
| **Missing →** | Defaults to `./pdfs/` (relative to current directory) |

Controls where `deposit.py` places and looks up PDF files. The directory is created automatically if it doesn't exist.

---

## `MINERU_RAW_DIR`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `deposit`, `pipeline-async` |
| **Value** | Path for MinerU OCR output (e.g. `./_raw`) |
| **Required?** | No |
| **Missing →** | Defaults to `./_raw/` |

MinerU writes one subdirectory per paper here, each containing `full.md` + metadata files.

---

## `MINERU_DOWNLOAD_DIR`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `pdf-downloader` |
| **Value** | Cache directory for downloaded PDFs |
| **Required?** | No |
| **Missing →** | Defaults to `./downloads/` |

---

## `MINERU_CACHE_DIR`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `mineru-api` |
| **Value** | Cache directory for MinerU API responses |
| **Required?** | No |
| **Missing →** | Defaults to `~/.mineru_cache/` |

Speeds up repeated OCR of the same PDF — API responses are cached by content hash.

---

## `CONTACT_EMAIL`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `pdf-downloader` |
| **Value** | Your email address |
| **Required?** | No — strongly recommended for CrossRef API politeness |
| **Missing →** | Uses generic User-Agent `"mineru-pipeline (https://github.com/e-zz/librarian)"` |

CrossRef's API terms ask users to identify themselves. Setting this helps maintain access:

```
CONTACT_EMAIL=your.email@example.com
```

---

## `UNPAYWALL_EMAIL`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `pdf-downloader` (via Unpaywall API path) |
| **Value** | Your email address |
| **Required?** | No — only needed if using Unpaywall for OA PDF lookups |
| **Missing →** | Unpaywall lookup is skipped; other download paths (arXiv, DOI) still work |

---

## `RAGFLOW_STATE`

| Attribute | Detail |
|-----------|--------|
| **Used by** | `ragflow-upload` |
| **Value** | Path to upload state file (e.g. `./upload_state.json`) |
| **Required?** | No |
| **Missing →** | Defaults to `./ragflow_upload_state.json` |

Persistence file for the upload queue — allows resuming interrupted batch uploads.

---

## Quick setup: bare minimum

If you want to get started with zero config beyond a MinerU token for big PDFs:

```bash
cp .env.example .env
# Edit .env — set only MINERU_TOKEN if you parse papers >20 pages
# Everything else is optional
mineru-api paper.pdf
```
