# Dataflows

Visual reference of all major librarian pipeline dataflows — entry points, data paths, and outputs.

---

## 1. One-Shot: arXiv ID / DOI → OCR

**CLI:** `librarian <arXiv_id_or_DOI>`

This is the simplest path. A single command resolves the identifier, downloads the PDF, and runs OCR.

```
User input
  │
  ├── arXiv ID (2311.08990)
  ├── DOI (10.1007/s42484-025-00254-8)
  └── PDF path (paper.pdf)
       │
       ▼
  ┌──────────────────┐
  │   librarian      │  ← entry point
  └──────┬───────────┘
         │
    ┌────┴────┐
    │  DOI?   │───NO───► use as-is
    └────┬────┘
         │ YES
         ▼
  ┌──────────────────┐
  │ pdf-downloader   │  →  downloads/<arxiv_id>.pdf
  │ (arXiv API)      │     (or uses existing file)
  └──────┬───────────┘
         │
         ▼
  ┌──────────────────┐
  │ mineru-api parse │  →  _raw/<paper_title>/
  │ (MinerU Cloud)   │     ├── full.md
  │ v1 (≤40pp) or v4 │     ├── images/
  └──────────────────┘     └── metadata.json
```

**Output:** `_raw/<paper_title>/full.md`

---

## 2. Zotero Collection → OCR → RAGFlow

**CLI:** `zotero-pipeline --collection <key_or_name> --dataset-id <ragflow_id>`

Reads linked-file PDFs from a Zotero collection, OCRs them, uploads to RAGFlow with SHA256 dedup.

```
  ┌──────────────────┐
  │ Zotero Desktop   │  ← must be running with cookjohn plugin
  │ (cookjohn MCP)   │
  └──────┬───────────┘
         │
         ▼
  ┌─────────────────────────────┐
  │ zotero_linked_pipeline.py   │
  │                             │
  │ 1. Resolve collection key   │
  │ 2. Get all items            │
  │ 3. Filter linkMode=2 PDFs   │
  │ 4. Verify on disk           │
  └──────┬──────────────────────┘
         │
         ▼
  ┌──────────────────┐
  │ pipeline_async   │  process <pdf_paths...>
  │                  │  --dataset-id <id>
  │ Per file:        │  --lang en
  │  • OCR (MinerU)  │  --concurrency 4
  │  • deposit MD    │
  │  • Upload MD     │  →  _raw/<dataset>/
  │  • Start parse   │     →  RAGFlow (SHA256 dedup)
  │  • Wait for DONE │
  └──────────────────┘
```

**Prerequisite:** Zotero running + cookjohn plugin on `http://127.0.0.1:23120/mcp`

---

## 3. Zotero Keyword Search → OCR → RAGFlow

**CLI:** `zotero-search --query "quantum machine learning" --ragflow-dataset <id>`

Searches Zotero by keyword, collects PDFs from matching items, feeds into OCR + RAGFlow.

```
  ┌──────────────────┐
  │ User query       │  --query "quantum machine learning"
  └──────┬───────────┘
         │
         ▼
  ┌──────────────────────────────────┐
  │ zotero_search_pipeline.py        │
  │                                  │
  │ 1. search_library(q=...)         │  ← Zotero MCP
  │ 2. For each hit:                 │
  │    get_item_details(itemKey)     │  ← extract PDF path
  │ 3. Filter to existing PDFs       │
  │ 4. Show summary                  │
  └──────┬───────────────────────────┘
         │
         ▼
  ┌──────────────────┐
  │ pipeline_async   │  process <pdf_paths...>
  │                  │  --dataset-id <id>
  │ (same as flow 2) │
  └──────────────────┘
```

**Opt-in scoping:** `--collection <key_or_name>` restricts search to items in a specific collection.

---

## 4. Local PDF / Standalone OCR

**CLI:** `mineru-api parse paper.pdf` or `mineru-api parse https://example.com/doc.pdf`

Single-file OCR. Auto-detects API version (v1 for ≤40pp, v4 for larger files with `MINERU_TOKEN`).

```
  Input
  │
  ├── Local file (paper.pdf)
  └── URL (https://example.com/doc.pdf)
       │
       ▼
  ┌──────────────────────────────┐
  │ mineru-api parse             │
  │                              │
  │ Auto-detect API:             │
  │  • ≤40pp  → v1 (free)       │
  │  • >40pp  → v4 (needs token)│
  │                              │
  │ Options:                     │
  │  --api v4    (force v4)      │
  │  --model vlm (better OCR)    │
  │  --pages 1-20 (range)        │
  │  --lang en                   │
  └──────┬───────────────────────┘
         │
         ▼
    _raw/<filename>/
    ├── full.md          ← clean Markdown
    ├── images/          ← extracted figures (v4)
    └── metadata.json
```

---

## 5. Batch OCR → RAGFlow (with state persistence)

**CLI:** `pipeline-async process *.pdf --dataset-id <id>`

Batch-oriented pipeline. Processes multiple PDFs in parallel, persists state for resume, optionally uploads to RAGFlow.

```
  Input
  │
  ├── *.pdf  (glob pattern)
  ├── --dataset-id <id>      (omit for OCR-only)
  ├── --dataset-name <name>  (deposit path under _raw/)
  ├── --concurrency 4
  └── --state-file .pipeline_state.json
       │
       ▼
  ┌────────────────────────────────────┐
  │ pipeline_async process             │
  │                                    │
  │ Per file (parallel, ≤4 workers):   │
  │                                    │
  │  ┌──────────┐                      │
  │  │ OCR via   │  MinerU API         │
  │  │ MinerU    │  (v1/v4 auto)       │
  │  └────┬─────┘                      │
  │       │                            │
  │       ▼                            │
  │  ┌──────────┐                      │
  │  │ Deposit  │  → _raw/<dataset>/   │
  │  │ MD       │  + _manifest.json    │
  │  └────┬─────┘                      │
  │       │                            │
  │       ▼ (if --dataset-id)          │
  │  ┌──────────┐                      │
  │  │ Upload   │  → RAGFlow           │
  │  │ + parse  │  (SHA256 dedup)      │
  │  └──────────┘                      │
  │                                    │
  │ State file tracks per-file progress│
  └────────────────────────────────────┘
       │
       ▼  (interrupted? resume)
  ┌──────────────────┐
  │ pipeline-async   │  resume
  │ resume           │  (reads state, skips done files,
  │                  │   retries failed/pending)
  └──────────────────┘
```

**Resume safety:** State file records PDF path + SHA256. On resume, missing PDFs are searched in:
1. Original path
2. `PIPELINE_PDF_SEARCH_DIRS` (env var, semicolon-separated)
3. Default: Downloads, Desktop

---

## 6. Download + Zotero Entry + OCR (collect_papers)

**CLI:** `collect_papers.py 2501.07231 2403.08868 --collection <key> --dataset-id <id>`

Orchestration script: arXiv ID → download PDF → create Zotero entry + attach PDF → OCR → RAGFlow → cleanup.

```
  arXiv IDs (2501.07231, 2403.08868...)
       │
       ▼
  ┌─────────────────────────────┐
  │ Step 1: Download PDFs       │
  │                             │
  │ pdf_downloader or curl      │
  │ → downloads/<dataset-name>/ │
  └──────┬──────────────────────┘
         │
         ▼
  ┌─────────────────────────────┐
  │ Step 2: Zotero entry        │
  │                             │
  │ • fetch_arxiv_meta()        │
  │ • write_item(create)        │
  │ • write_item(import PDF)    │
  │ • add_items_to_collection   │
  └──────┬──────────────────────┘
         │
         ▼  (if --dataset-id)
  ┌─────────────────────────────┐
  │ Step 3: OCR → RAGFlow       │
  │                             │
  │ pipeline_async process      │
  └──────┬──────────────────────┘
         │
         ▼
  ┌─────────────────────────────┐
  │ Step 4: Cleanup             │
  │                             │
  │ Remove temp PDFs + OCR dirs │
  └─────────────────────────────┘
```

**Atomic guarantee:** Zotero entry + PDF attach happen in the same function — no orphan entries without attachments.

---

## 7. Deposit + Manifest Tracking

**CLI:** `deposit pdf paper.pdf --dataset qml`

Places a PDF into the managed library and registers it in the manifest index.

```
  Input file (paper.pdf)
       │
       ▼
  ┌──────────────────────┐
  │ deposit pdf          │
  │                      │
  │ • Validate (%PDF-    │
  │   magic, >50KB)      │
  │ • Copy to library    │  → MINERU_PDF_LIBRARY/<name>.pdf
  │ • Record in manifest │     (D:/library/qn/ if configured)
  │ • SHA256 index       │  → _manifest.json
  └──────────────────────┘
```

**Manifest lookup:** Path-insensitive — indexed by SHA256. Files can move between datasets without breaking the index.

---

## 8. RAGFlow Upload (standalone)

**CLI:** `ragflow-upload upload ./_raw/ --dataset-id <id>`

Upload pre-OCRed markdown files to RAGFlow. Supports resume, dedup, parallel upload.

```
  Input
  │
  ├── ./_raw/<paper1>/full.md
  ├── ./_raw/<paper2>/full.md
  └── --dataset-id <id>
       │
       ▼
  ┌────────────────────────────┐
  │ ragflow-upload upload      │
  │                            │
  │ Phase 1: Upload (parallel) │
  │  • SHA256 dedup against    │
  │    existing docs           │
  │  • Upload MD to RAGFlow    │
  │  • meta_fields.sha256      │
  │                            │
  │ Phase 2: Start parse       │
  │  • POST /chunks            │
  │                            │
  │ Phase 3: Poll (if --wait)  │
  │  • Until DONE/FAIL         │
  └────────────────────────────┘
```

**Two-tier dedup:**
1. **Upload level** — RAGFlow `meta_fields.sha256` checked via `get_existing_docs`
2. **Content level** — SHA256 of file content, not just filename

---

## Data Flow Summary

```
                ┌─────────────┐
                │  User input │
                └──────┬──────┘
                       │
       ┌───────────────┼───────────────────┐
       │               │                   │
       ▼               ▼                   ▼
  arXiv/DOI       Zotero               Local PDF
       │           │                       │
       ▼           ▼                       ▼
  ┌────────┐  ┌──────────┐          ┌──────────┐
  │ pdf-   │  │ zotero-  │          │ mineru-  │
  │ down-  │  │ pipeline │          │ api      │
  │ loader │  │ or       │          │ parse    │
  └───┬────┘  │ zotero-  │          └────┬─────┘
      │       │ search   │               │
      ▼       └────┬─────┘               │
  ./downloads/     │                     │
      │            ▼                     │
      └────────► pipeline_async ◄────────┘
                     │
                     ├── OCR (MinerU v1/v4)
                     ├── Deposit (optional)
                     │       └→ _raw/<dataset>/
                     ├── Upload RAGFlow
                     │       └→ SHA256 dedup
                     └── State file ← resume
```

---

## Env Variables per Dataflow

| Command | Env Vars Required | Env Vars Optional |
|---------|------------------|-------------------|
| `librarian <id>` | — | `MINERU_TOKEN`, `MINERU_DOWNLOAD_DIR` |
| `mineru-api parse` | — | `MINERU_TOKEN`, `MINERU_CACHE_DIR` |
| `pdf-downloader` | — | `CONTACT_EMAIL`, `MINERU_DOWNLOAD_DIR` |
| `pipeline-async` | — | `MINERU_TOKEN`, `RAGFLOW_URL`, `RAGFLOW_API_KEY`, `PIPELINE_PDF_SEARCH_DIRS` |
| `ragflow-upload` | — | `RAGFLOW_URL`, `RAGFLOW_API_KEY`, `RAGFLOW_STATE` |
| `zotero-pipeline` | — | `RAGFLOW_URL`, `RAGFLOW_API_KEY` |
| `zotero-search` | — | `RAGFLOW_URL`, `RAGFLOW_API_KEY` |
| `deposit` | — | `MINERU_PDF_LIBRARY`, `MINERU_RAW_DIR`, `MINERU_MANIFEST_PATH` |
| `collect_papers.py` | — | `MINERU_DOWNLOAD_DIR`, `MINERU_TOKEN`, `RAGFLOW_URL`, `RAGFLOW_API_KEY` |
