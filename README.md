# 📄 Librarian

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue?logo=python)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**End-to-end academic paper pipeline — discover, download, OCR with MinerU, and upload to RAGFlow or NotebookLM.**

Install in seconds, then use `librarian` for the default workflow or pick any tool for a custom pipeline.

```bash
pip install librarian
cp .env.example .env              # set MINERU_TOKEN (or skip — free tier works without it)
librarian 2311.08990              # arXiv ID → download → OCR → markdown in one command

# Or build your own pipeline with individual tools:
mineru-api parse paper.pdf        # just OCR
pdf-downloader 2311.08990         # just download
ragflow-upload upload ./_raw/...  # just upload
```

## ✨ Features

| Icon | Feature | Description |
|---|---|---|
| 🔍 | **Discover** | Scrape Google Scholar profiles to find recent publications |
| ⬇️ | **Download** | Fetch PDFs via DOI, arXiv, Sci-Hub, or PubMed Central |
| 📝 | **OCR** | Parse PDFs to clean Markdown using MinerU Cloud API (v4) |
| 🗄️ | **RAGFlow** | Upload parsed papers to RAGFlow for semantic search |
| 🤖 | **NotebookLM** | Feed parsed papers into Google NotebookLM |
| 🔗 | **Zotero Bridge** | Process papers straight from your Zotero library |
| 📋 | **Manifest Tracking** | Deposit and track files with provenance metadata |
| ⚡ | **Async Pipeline** | Batch OCR with parallel processing and caching |

## 🚀 Quick Start

### 1. Install

See [docs/install.md](docs/install.md) for all installation methods (pip, Hermes skill, npx skills, source clone).

```bash
pip install git+ssh://git@github.com/e-zz/librarian.git
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your MinerU API token
```

### 3. Run the pipeline

One command handles download (arXiv/DOI) + OCR:

```bash
# Download + OCR an arXiv paper in one shot:
librarian 2311.08990

# Or a DOI:
librarian 10.1007/s42484-025-00254-8

# Or a local PDF:
librarian paper.pdf

# Try with a sample paper (no API key needed):
librarian --demo
```

Output goes to `./_raw/<paper_name>/` as clean Markdown.

## 📋 CLI Reference

| Command | Script | Description |
|---|---|---|
| `librarian` | `librarian_pipeline.py` | **One-shot pipeline** — download + OCR (default workflow) |
| `mineru-api` | `mineru_api.py` | Parse PDF(s) to Markdown via MinerU Cloud API |
| `pdf-downloader` | `pdf_downloader.py` | Download PDFs by DOI/arXiv ID with metadata |
| `pipeline-async` | `pipeline_async.py` | Batch OCR pipeline with optional RAGFlow upload |
| `ragflow-upload` | `ragflow_uploader.py` | Upload parsed Markdown to RAGFlow |
| `gs-scrape` | `gs_full_scrape.py` | Scrape Google Scholar profile for publications |
| `scihub-download` | `scihub_downloader.py` | Extract/download PDFs from Sci-Hub/PMC |
| `deposit` | `deposit.py` | Place files into managed library with manifest |
| `zotero-pipeline` | `zotero_linked_pipeline.py` | Bridge Zotero collection → pipeline |

## 🔧 Workflows

See [docs/dataflows.md](docs/dataflows.md) for detailed dataflow diagrams of every pipeline path. Quick reference:

### Standalone OCR

```bash
mineru-api parse paper.pdf
```

### RAGFlow Pipeline

```bash
pdf-downloader --doi 10.1234/example
mineru-api ./downloads/example.pdf
ragflow-upload --dir ./_raw/example --kb "My Research"
```

Or in one shot with the integrated pipeline:

```bash
pipeline-async --pdf-list papers.txt --ragflow
```

## 🧪 Prompt Classification

When running librarian through an AI agent (e.g. Hermes Agent), not all prompts reliably trigger the pipeline tools. Some phrasing triggers ad-hoc workflows (direct API calls, temp files, scattered output). The matrix below shows what works:

| Prompt | Router | Comment |
|--------|--------|---------|
| `2311.08990` | **`librarian`** | Bare arXiv ID → pipeline |
| `10.1007/s42484-025-00254-8` | **`librarian`** | Bare DOI → pipeline |
| `Parse paper.pdf` | **`mineru-api parse`** | Explicit local file |
| `Download and OCR 2311.08990` | **`librarian`** | Action verb + ID |
| `Extract text from arXiv:2311.08990` | **`librarian`** | Verb "extract" + arXiv ID |
| `Read this paper for me: 2311.08990` | **`librarian`** | Vague verb + bare ID still works |
| `Get me the full text of arXiv paper 2311.08990` | **`librarian`** | Vague verb + "arXiv paper" |
| `search quantum ml in Zotero and upload` | **`zotero-search`** | Keyword → Zotero search pipeline |
| `help me access content of arxiv.org/abs/...` | ⚠️ Improvised | Full URL + vague verb bypasses pipeline |

**Rule of thumb:** Bare identifiers (`2311.08990`, `10.1007/...`, `paper.pdf`) and explicit actions (`parse`, `download`, `OCR`) reliably trigger the pipeline. Full arXiv URLs + vague verbs like "access the content" may cause agents to scrape PDFs directly via `web_extract` instead of using the pipeline.

### Full Researcher Survey

```bash
# 1. Scrape Google Scholar
gs-scrape --profile-url "https://scholar.google.com/citations?user=XXXXX"

# 2. Download papers
pdf-downloader --doi-list ./scholar_results/dois.txt

# 3. OCR and upload
pipeline-async --pdf-list ./downloads/manifest.txt --ragflow
```

## 📦 Dependencies

| Package | Required | Used By |
|---|---|---|
| `requests` | ✅ Yes | All scripts (API calls, downloads) |
| `cloakbrowser` | ❌ Optional | Google Scholar scraping, Sci-Hub/PMC download |
| `notebooklm-py` | ❌ Optional | NotebookLM source upload |

## 🤝 Contributing

Contributions are welcome! Please open an issue first to discuss what you'd like to change. Fork the repo, make your changes, and submit a pull request. Keep scripts self-contained with minimal dependencies, follow the existing argparse CLI patterns, and add a `main()` entry point for any new script.

## 📄 License

[MIT](LICENSE) © 2026
