---
name: librarian
description: Use when processing academic papers — from discovery and download to OCR and knowledge base ingestion
---

# librarian

## Overview

**librarian** is a collection of modular scripts that together form an end-to-end academic paper pipeline:

1. **Discover** papers — scrape Google Scholar profiles for recent publications
2. **Download** — fetch PDFs via DOI/arXiv/Sci-Hub/PMC with metadata enrichment
3. **OCR** — parse PDFs to clean Markdown using the MinerU Cloud API (v4 precision mode)
4. **Upload** — ingest parsed Markdown into RAGFlow for semantic search, or NotebookLM for AI-powered analysis
5. **Bridge** — integrate with Zotero to process papers from your reference library

**Installation:** See [docs/install.md](docs/install.md) (pip, Hermes skill, npx skills, or source clone).

What you need:
- A **MinerU Cloud API token** (free tier available at https://mineru.net)
- **Python 3.10+**
- Optionally: RAGFlow instance, NotebookLM account, CloakBrowser license, Zotero

## Prerequisites

| Requirement | Details | How to set up |
|---|---|---|
| **MinerU account** | Required for v4 precision OCR. Sign up at https://mineru.net, get your API token | [docs/setup-guide.md](docs/setup-guide.md) → MinerU |
| **Python 3.10+** | All scripts target Python 3.10 or newer | — |
| **NotebookLM** (recommended) | Easy AI-powered paper Q&A — no infra needed | [docs/setup-guide.md](docs/setup-guide.md) → NotebookLM |
| **RAGFlow** (optional) | Semantic search for large collections. Requires Docker | [docs/setup-guide.md](docs/setup-guide.md) → RAGFlow |
| **CloakBrowser** (optional) | GS scraping + Sci-Hub download. Paid license required | [docs/setup-guide.md](docs/setup-guide.md) → CloakBrowser |
| **Zotero** (optional) | Reference library bridge. Requires desktop + cookjohn plugin | [docs/setup-guide.md](docs/setup-guide.md) → Zotero |


## Quick Start — "Parse a PDF with one command"

The fastest path from zero to markdown:

```bash
# Install
pip install librarian

# Run the pipeline on an arXiv paper (auto-downloads + OCRs):
librarian 2311.08990

# Or a local PDF:
librarian paper.pdf
```

No `.env` file needed for papers ≤20pp (free MinerU v1 API). Output goes to `./_raw/<paper_name>/` as Markdown.

For advanced usage, each step is also available as a standalone CLI tool (see Scripts Reference below).

## .env Reference

All environment variables used across the pipeline. For detailed effect descriptions and fallback behavior, see [docs/env-reference.md](docs/env-reference.md).

| Variable | Required | Default | Description |
|---|---|---|---|
| `MINERU_TOKEN` | **Yes** | — | mineru.net API token (v4 precision mode) |
| `RAGFLOW_URL` | No | `http://localhost:9380` | RAGFlow server URL |
| `RAGFLOW_API_KEY` | No | — | RAGFlow API key |
| `MINERU_PDF_LIBRARY` | No | `./pdfs` | PDF storage directory for `deposit.py` |
| `MINERU_RAW_DIR` | No | `./_raw` | MinerU OCR output directory |
| `MINERU_DOWNLOAD_DIR` | No | `./downloads` | Downloaded PDF directory |
| `MINERU_CACHE_DIR` | No | `~/.mineru_cache` | API response cache |
| `CONTACT_EMAIL` | No | — | Used in CrossRef User-Agent header |
| `UNPAYWALL_EMAIL` | No | — | Required by Unpaywall API |

## Fallback chains

No external dependency is mandatory. Each tool degrades gracefully when a service is unavailable.

| Service | Required? | Fallback |
|---------|-----------|----------|
| MinerU Cloud API | ✅ Yes (OCR) | Free v1 agent API — ≤20pp, no token needed |
| MinerU v4 token | 🟡 Optional | v1 agent API works for papers ≤20pp |
| CloakBrowser (GS / Sci-Hub) | ❌ No | arXiv API / DOI → `curl` download |
| RAGFlow server | ❌ No | Stop after markdown, or use NotebookLM |
| NotebookLM | ❌ No | Local markdown only — `grep` / `rg` |
| Zotero + cookjohn | ❌ No | Manual file management |

### Quickest path — CLI only, zero services

```bash
pip install librarian requests
pdf-downloader 2311.08990          # arXiv → PDF download (free API)
mineru-api ./downloads/2311.08990.pdf  # OCR (uses free v1 if no token)
```

Three pip deps (`librarian`, `requests`, nothing else), markdown out in `_raw/`.

### PDF download without CloakBrowser / SciHub

`pdf-downloader` hits arXiv API + CrossRef directly (both free, no key):

```bash
pdf-downloader 10.1007/s42484-025-00254-8    # DOI → arXiv lookup → download
pdf-downloader 2311.08990                     # bare arXiv ID
pdf-downloader --input dois.txt              # batch
```

### RAGFlow-less pipeline

```bash
pipeline-async pdfs/*.pdf        # OCR only — no --dataset-id = skip upload
# Output in _raw/ — grep/rg the markdown files locally
```

## Scripts Reference

> After `pip install librarian`, each script is available as a CLI command (e.g. `mineru-api`).
> From a source clone, use `python scripts/<name>.py` instead.

### 1. `deposit.py` — File Placement + Manifest Tracking (CLI: `deposit`)

Places PDF or Markdown files into the managed library directory (`MINERU_PDF_LIBRARY`) and updates a manifest for provenance tracking.

```bash
deposit pdf path/to/paper.pdf --dataset my-papers
deposit md path/to/output.md --dataset my-papers
deposit validate                              # check manifest integrity
```

### 2. `_ragflow_client.py` — Shared RAGFlow API Client

Internal shared module. Provides a reusable client class for RAGFlow REST API interactions. Imported by other scripts; not intended for direct CLI use.

### 3. `mineru_api.py` — MinerU Cloud API CLI (CLI: `mineru-api`)

Parses PDFs to clean Markdown via the mineru.net Cloud API.

```bash
mineru-api parse paper.pdf
mineru-api batch ./pdfs/*.pdf          # batch mode
mineru-api parse paper.pdf --output-dir ./my_markdown
mineru-api test                         # API connectivity test
```

### 4. `pdf_downloader.py` — DOI/arXiv → Download with Metadata Enrichment (CLI: `pdf-downloader`)

Resolves DOIs and arXiv IDs to PDF download URLs. Enriches metadata via CrossRef and Unpaywall. Accepts IDs as positional arguments.

```bash
pdf-downloader 10.1007/s42484-025-00254-8     # DOI
pdf-downloader 2311.08990                      # arXiv ID
pdf-downloader --input dois.txt                # batch from file
pdf-downloader 2311.08990 --out ./downloads    # custom output dir
```

### 5. `pipeline_async.py` — Batch OCR → (Optional) Upload Pipeline (CLI: `pipeline-async`)

The main orchestration script. Reads PDFs as positional args, submits them to MinerU OCR in parallel, then optionally uploads results to RAGFlow.

```bash
pipeline-async process pdfs/*.pdf                       # OCR only
pipeline-async process pdfs/*.pdf --dataset-id <id>     # OCR + RAGFlow
pipeline-async resume                                    # resume interrupted run
pipeline-async status                                    # check pipeline state
```

### 6. `ragflow_uploader.py` — Upload Markdown Files to RAGFlow (CLI: `ragflow-upload`)

Uploads MinerU-parsed Markdown files to a RAGFlow knowledge base for semantic search.

```bash
ragflow-upload upload ./_raw --dataset-id <id>
ragflow-upload datasets                          # list available datasets
ragflow-upload create-dataset "My Papers"        # create a new dataset
ragflow-upload status --dataset-id <id>          # check parsing status
```

### 7. `gs_full_scrape.py` — Google Scholar Profile Scraper

Scrapes a researcher's Google Scholar profile to discover their publications. Outputs structured data for downstream download + OCR.

```bash
gs-scrape --uid Ub1bvfkAAAAJ
gs-scrape --uid-file researchers.json --output-dir ./gs_results
```

### 8. `scihub_downloader.py` — Sci-Hub/PMC PDF Extraction + Download (CLI: `scihub-download`)

Extracts PDF download URLs or downloads PDFs directly from Sci-Hub and PubMed Central.

```bash
scihub-download --mode extract --doi 10.1234/example
scihub-download --mode download --doi 10.1234/example
```

### 9. `zotero_linked_pipeline.py` — Zotero → Pipeline Bridge (CLI: `zotero-pipeline`)

Reads linked PDFs from a Zotero collection and feeds them through the OCR pipeline. Requires Zotero desktop + cookjohn MCP plugin.

```bash
# Dry run — see which PDFs will be processed
zotero-pipeline --collection "quantum-chemistry" --dry-run

# Full pipeline: Zotero PDFs → MinerU OCR → RAGFlow upload
zotero-pipeline --collection "quantum-chemistry" --dataset-id <id>
```

Alternatively, `pdf-downloader --zotero` can download a single paper and auto-create its Zotero entry in one step.

### 10. `librarian_pipeline.py` — One-Shot End-to-End Pipeline (CLI: `librarian`)

Accepts arXiv IDs, DOIs, or local PDFs and runs the full download → OCR workflow. Best starting point for new users.

```bash
librarian 2311.08990                  # arXiv ID → download → OCR
librarian 10.1007/s42484-025-00254-8  # DOI → download → OCR
librarian paper.pdf                    # local PDF → OCR
librarian --input ids.txt             # batch from file
librarian --demo                       # try with a real paper
```

## Workflows

### Standalone OCR (No External Services)

Quickly OCR a single PDF without RAGFlow or any other service:

```bash
export MINERU_TOKEN=tk_xxxxxxxxxx
mineru-api parse paper.pdf
# Output: ./_raw/paper/paper.md with embedded images
```

### RAGFlow Pipeline (PDF → OCR → Semantic Search)

Full pipeline from PDF to searchable knowledge base:

```bash
# 1. Download papers
pdf-downloader 10.1007/s42484-025-00254-8 --out ./downloads

# 2. OCR with MinerU
mineru-api parse ./downloads/example.pdf

# 3. Upload to RAGFlow
ragflow-upload upload ./_raw/example --dataset-id <id>
```

Or use the integrated pipeline:

```bash
pipeline-async process pdfs/*.pdf --dataset-id <id>
```

### NotebookLM Alternative (PDF → NotebookLM Source)

Upload MinerU-parsed Markdown as NotebookLM sources:

```bash
# Requires: pip install notebooklm-py
mineru-api parse paper.pdf
notebooklm upload ./_raw/paper/paper.md
```

### Full Researcher Survey (GS + Download + OCR + RAGFlow)

End-to-end survey of a researcher's recent work:

```bash
# 1. Scrape Google Scholar profile
gs-scrape --uid Ub1bvfkAAAAJ --output-dir ./gs_results

# 2. Download all found papers
pdf-downloader --input ./scholar_results/dois.txt

# 3. OCR and upload
pipeline-async process ./downloads/*.pdf --dataset-id <id>
```

## Architecture

```
┌─────────────────┐
│  Google Scholar  │
│  (gs_full_scrape)│
└────────┬────────┘
         │ publication list
         ▼
┌─────────────────┐    ┌──────────────────┐
│  DOI / arXiv ID  │◄───│  Zotero Library   │
│  (pdf_downloader) │    │(zotero_linked_pipeline)│
└────────┬─────────┘    └──────────────────┘
         │ PDF file
         ▼
┌─────────────────┐    ┌──────────────────┐
│  Sci-Hub / PMC   │    │  CrossRef /       │
│(scihub_downloader)│    │  Unpaywall        │
└────────┬─────────┘    └──────────────────┘
         │ PDF (fallback)
         ▼
┌──────────────────┐
│  MinerU Cloud API │
│  (mineru_api.py)  │
└────────┬─────────┘
         │ Markdown
         ▼
┌──────────────────┐    ┌──────────────────┐
│  RAGFlow Upload   │    │  NotebookLM       │
│(ragflow_uploader.py)│   │(via notebooklm-py)│
└──────────────────┘    └──────────────────┘

Orchestration:
  pipeline_async.py — batch OCR + optional upload
  deposit.py        — file management + manifest
  _ragflow_client.py — shared API client
```

## Pitfalls

- **MinerU API rate limits**: The free tier has daily limits. Monitor your usage at https://mineru.net
- **CloakBrowser on Windows**: `gs_full_scrape.py` and `scihub_downloader.py` rely on CloakBrowser for headless browsing. CloakBrowser can have stability issues on Windows — use on Linux if possible, or run with retry logic.
- **arXiv Terms of Use**: Automated bulk downloading from arXiv may violate their terms. Use responsibly — add reasonable delays and respect `robots.txt`.
- **RAGFlow default configuration**: RAGFlow's default chunking and embedding settings may not be optimal for academic papers. Configure your knowledge base parser settings for best results.
- **Google Scholar scraping**: Google may rate-limit or CAPTCHA-gate automated scraping. Add delays between profile requests.
- **Sci-Hub availability**: Sci-Hub domains change frequently. The script includes fallback mechanisms, but availability is not guaranteed.
- **File path length on Windows**: Deeply nested paths from MinerU output may exceed Windows MAX_PATH (260 chars). Use short base directory names.
- **Concurrent API calls**: MinerU API has per-account concurrency limits. `pipeline_async.py` respects these but you may need to adjust `--max-workers`.
