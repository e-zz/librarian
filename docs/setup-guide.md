# Setup Guide — Optional Dependencies

librarian works with zero config if you only need PDF → markdown. Add extra services only when you need them.

---

## 🟢 Easy (minutes, no infra)

### NotebookLM — AI-powered paper Q&A

NotebookLM is Google's free notebook tool (NotebookLM.google.com). It reads parsed markdown and lets you ask questions, generate summaries, podcasts, and study guides.

#### 1. Install

```bash
pip install notebooklm-py
```

Or with librarian: `pip install librarian[notebooklm]`

#### 2. Authenticate

```bash
notebooklm login
```

This opens a browser window. Sign in with any Google account. After successful login, a `storage_state.json` is saved and reused automatically.

**If the browser doesn't open** (headless server, WSL, etc.):

```bash
# Extract cookies from a browser where you're already signed into Google
notebooklm login --browser-cookies chrome::Default   # Windows Chrome
notebooklm login --browser-cookies firefox::none     # Windows Firefox
```

#### 3. Verify it works

```bash
notebooklm auth check --test --json
```

Expected output (abbreviated):

```json
{"status": "ok", "checks": {"token_fetch": true}}
```

`token_fetch: true` is the key field. Bare `"status": "ok"` without `--test` is a false positive — it only checks the file exists, not that the session is still valid.

#### 4. Use it

```bash
# Create a notebook for your project
notebooklm create "Quantum Chemistry Papers"

# Upload parsed markdown
notebooklm source add ./_raw/paper-12345/full.md

# Ask questions
notebooklm ask "What method does this paper use for ground-state estimation?"
```

#### Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `token_fetch: false` | Session expired | `notebooklm login` again |
| `not found` in Google | Account not enabled | Open https://notebooklm.google.com in browser first |
| `WSL: browser can't open` | No display | Use `--browser-cookies chrome::Default` |
| `rate limited` | Google API quota | Wait 5-10 minutes and retry |

#### Limits

| Plan | Sources per notebook |
|------|-------------------|
| Free | ~50 |
| NotebookLM Plus | ~600 (via Google Workspace) |

---

### MinerU Cloud API — PDF OCR

MinerU offers two API tiers:

| | v1 Agent API (free) | v4 Precision API |
|---|---|---|
| Token | Not needed | Required (`MINERU_TOKEN`) |
| Max size | ≤20pp / ≤10MB | ≤200pp / ≤200MB |
| Quality | Good for clean PDFs | Better for scanned/scraped PDFs |
| Cost | Free | Pay-per-page |

#### 1. Sign up

Visit https://mineru.net and create an account. No credit card needed.

#### 2. Get your token (only if you need v4)

Go to **API Key management** → **Create token**. Copy the string starting with `tk_`.

#### 3. Set it up

```bash
cp .env.example .env
# Edit .env — uncomment MINERU_TOKEN and paste your key:
# MINERU_TOKEN=tk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

#### 4. Verify

```bash
# Parse a short PDF with free v1 (no config needed)
mineru-api some-paper.pdf

# Parse with v4 precision (needs token)
mineru-api big-paper.pdf --api v4 --model vlm
```

If v1 works but v4 returns a 401/403, your token is wrong or expired.

#### How to decide which API to use

```text
Check file size + page count
  │
  ├── ≤20 pages AND ≤10MB → v1 Agent API (free, no token)
  │     mineru-api paper.pdf
  │
  ├── 21-200 pages OR 10-200MB → v4 Precision API (needs token)
  │     export MINERU_TOKEN=tk_...
  │     mineru-api paper.pdf --api v4 --model vlm
  │
  └── >200 pages OR >200MB → Split the PDF first
        mineru-api paper.pdf --pages 1-200
        mineru-api paper.pdf --pages 201-400
```

Check page count:

```bash
# Requires pypdf or poppler:
python -c "from pypdf import PdfReader; print(len(PdfReader('paper.pdf').pages))"
```

---

### CrossRef / Unpaywall — Metadata enrichment

These are free public APIs with no setup. `pdf-downloader` uses them automatically when it has a DOI.

**Optional — set your email for polite API usage:**

```bash
export CONTACT_EMAIL=you@example.com
export UNPAYWALL_EMAIL=you@example.com
```

Without this, the generic User-Agent string is used. CrossRef's terms ask for identification, but it's not enforced.

---

### CloakBrowser — Google Scholar + Sci-Hub

[CloakBrowser](https://cloakbrowser.com) is a paid headless browser service. Needed only for:

- `gs-scrape` — Google Scholar profile scraping
- `scihub-downloader` — Sci-Hub / PMC PDF extraction

#### Install

```bash
pip install cloakbrowser
```

**This package is NOT on PyPI.** The correct package is distributed through CloakBrowser's own registry. Follow the install instructions at cloakbrowser.com after purchasing a license.

#### Verify

```python
# Quick test — new users should verify the browser launches:
python -c "import asyncio; from cloakbrowser import launch_context_async; asyncio.run(launch_context_async(headless=True))"
```

If this fails, CloakBrowser isn't properly installed or licensed.

#### Without CloakBrowser

You don't need it for basic PDF downloads:

```bash
# arXiv API + CrossRef (free, no key, no CloakBrowser):
pdf-downloader 10.1007/s42484-025-00254-8
pdf-downloader 2311.08990
pdf-downloader --input dois.txt
```

---

## 🟡 Medium (needs a running process)

### Zotero + cookjohn MCP — Reference library bridge

Bridge between your Zotero reference library and the pipeline — pulls linked PDFs from Zotero collections and feeds them into MinerU.

#### 1. Install Zotero

Download from https://www.zotero.org/download/ and install. Start it at least once.

#### 2. Install cookjohn plugin

- Download the `.xpi` file from https://github.com/OurMachinery/cookjohn/releases
- In Zotero: **Tools → Add-ons** → gear icon → **Install Add-on From File...**
- Select the `.xpi` file
- Restart Zotero

#### 3. Verify

The plugin listens on `http://127.0.0.1:23120/mcp` automatically. Check it's alive:

```bash
python -c "
import requests
r = requests.post('http://127.0.0.1:23120/mcp',
    json={'jsonrpc':'2.0','id':1,'method':'tools/call',
    'params':{'name':'get_libraries','arguments':{}}},
    timeout=5)
print(r.json())
"
```

Expected: a JSON response listing your Zotero libraries. If connection refused, Zotero isn't running or cookjohn isn't installed.

#### 4. Use it — two paths

**Path A — Batch pipeline from a Zotero collection:**

```bash
# Dry run — see which PDFs would be processed
zotero-pipeline --collection "quantum-chemistry" --dry-run

# Full pipeline: Zotero PDFs → MinerU OCR → RAGFlow upload
zotero-pipeline --collection "quantum-chemistry" --dataset-id <id>
```

**Path B — Download + auto-import a single paper into Zotero:**

```bash
# Download PDF, create Zotero entry, attach the file
pdf-downloader 2311.08990 --zotero --zotero-collection "quantum-chemistry"

# Same with DOI
pdf-downloader 10.1007/s42484-025-00254-8 --zotero --zotero-collection "my-collection"
```

Both paths connect to the same cookjohn MCP endpoint (`http://127.0.0.1:23120/mcp`). The difference:
- `zotero-pipeline` — reads existing PDFs from your Zotero library and sends them to MinerU OCR
- `pdf-downloader --zotero` — downloads a new PDF and creates its Zotero entry in one step

---

## 🔴 Hard (needs infrastructure)

### RAGFlow — Semantic search at scale

RAGFlow is a self-hosted document retrieval server. Only set this up if you have 100+ papers and want full-text semantic search across the entire collection.

#### 1. Docker setup

Create `docker-compose.yml`:

```yaml
services:
  ragflow:
    image: infiniflow/ragflow:latest
    ports:
      - "9380:9380"
    volumes:
      - ./ragflow_data:/ragflow/data
      - ./ragflow_logs:/ragflow/logs
    restart: unless-stopped
```

```bash
docker compose up -d
# First boot takes 2-5 minutes (database initialization)
# Watch logs: docker compose logs -f
# Look for: "RAGFlow server started"
```

#### 2. First-time setup

1. Open http://localhost:9380
2. Create an admin account (first user becomes admin)
3. Go to **Settings** → **API key**
4. Copy the key (starts with `ragflow-`)

#### 3. Configure librarian

```bash
# Add to .env:
RAGFLOW_URL=http://localhost:9380
RAGFLOW_API_KEY=ragflow-xxxxxxxxxxxxxxxxxxxx
```

#### 4. Verify

```bash
ragflow-upload datasets
```

Expected: a list of datasets (empty on first run) or an API error message. If connection refused, Docker isn't running.

#### 5. Upload papers

```bash
# After OCR produces _raw/ directory:
ragflow-upload upload ./_raw --dataset-id <id>

# Or via pipeline (all-in-one):
pipeline-async pdfs/*.pdf --dataset-id <id>
```

#### When to use RAGFlow vs NotebookLM

| | RAGFlow | NotebookLM |
|---|---|---|
| Scale | 1000s of papers | ~600 per notebook |
| Setup | Docker | pip install |
| Cost | Your server | Free |
| Search | Full-text + semantic | LLM-powered Q&A |
| Podcasts | No | Yes |
| Automation | REST API | CLI |

**For most users, start with NotebookLM.** Add RAGFlow only when you outgrow it.

---

## Summary — what to install when

| Use case | Install | Config needed |
|----------|---------|--------------|
| Just OCR a PDF | `pip install librarian` | Nothing |
| Download papers | `pip install librarian` | Nothing |
| OCR + paper Q&A | + `notebooklm-py` | `notebooklm login` |
| OCR + semantic search (100+) | + RAGFlow (Docker) | `RAGFLOW_URL` + key |
| Browse researchers' papers | + `cloakbrowser` + license | License key |
| Zotero bridge | + cookjohn plugin | Keep Zotero running |
