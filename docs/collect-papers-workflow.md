---
name: collect-papers-workflow
description: One-shot arXiv ID → Zotero entry + PDF → OCR → RAGFlow pipeline
---

# Collect Papers Workflow

Single orchestration script that wraps the existing tools into an atomic pipeline.

## Usage

```bash
# Minimum — just download + Zotero entry
python scripts/collect_papers.py 2501.07231 2403.08868 --collection XNPAFHMB

# Full pipeline — Zotero → OCR → RAGFlow
python scripts/collect_papers.py 2501.07231 --collection XNPAFHMB --dataset-id <id>

# Dry run
python scripts/collect_papers.py 2501.07231 --dry-run
```

## What it fixes

| # | Improvement | How |
|---|-------------|-----|
| 1 | **Atomic Zotero create+attach** | `create_zotero_entry()` creates item, attaches PDF, adds to collection in one function — no gap between create and import |
| 3 | **Cleanup at end, not in middle** | `cleanup()` runs AFTER all steps (Zotero + OCR + RAGFlow) complete |
| 4 | **No MSYS path ambiguity** | All paths use `Path(…).resolve()` for Win32 native paths; pdf_downloader's double-nesting quirk has fallback detection |
| 5 | **Single entry point** | `collect_papers.py` — one command replaces: pdf_downloader → MCP create → MCP import → pipeline_async → rm temp |

## Pipeline flow

```
arXiv IDs
    ↓
 Step 1: Download PDFs → downloads/<dataset-name>/
    ├─ Uses pdf_downloader.py (with fallback path detection)
    └─ Returns {arxiv_id: Path} mapping
    ↓
 Step 2: Create Zotero entry + attach PDF
    ├─ fetch_arxiv_meta() from arXiv API
    ├─ write_item(create) → bibliographic entry
    ├─ write_metadata(fix title) — workaround for cookjohn dropped-title bug
    ├─ write_item(import) → attach PDF
    └─ add_items_to_collection → sqd collection
    ↓
 Step 3: (optional) OCR + RAGFlow via pipeline_async
    └─ --dataset-id triggers this step
    ↓
 Step 4: Cleanup downloads/<dataset-name>/_ocr and *.pdf
```

## Path rules (mandatory)

1. Pass `--download-dir` as Win32 native path (`D:/mineru_pipeline/downloads/sqd/`)
2. Default resolves to `downloads/<dataset-name>/` relative to CWD (resolved with `Path.resolve()`)
3. pdf_downloader's double-nested path bug (`/d/d/...`) is detected as fallback

## Re-run safety

- Zotero items get tagged `from-collect` for easy rollback
- Zotero check: runs `search_library` to skip already-existing arXiv IDs (--no-cover skips this)
- Pipeline resumes via `pipeline_async resume` on state file
