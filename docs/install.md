# Installation

librarian is both a **Python package** (CLI tools) and a **Hermes skill** (agent instructions). Choose your path:

---

## 🐍 Install as a Python package

### From GitHub (recommended)

```bash
pip install git+ssh://git@github.com/e-zz/librarian.git
```

Or with optional dependencies:

```bash
pip install "librarian[cloakbrowser]"   # for GS scraping + Sci-Hub
pip install "librarian[notebooklm]"      # for NotebookLM upload
pip install "librarian[all]"            # everything
```

### From a local clone

```bash
git clone git@github.com:e-zz/librarian.git
cd librarian
pip install -e .
```

### Verify

```bash
mineru-api --help
pdf-downloader --help
```

All 8 entry points should show usage.

---

## 🤖 Install as a Hermes skill

If you use [Hermes Agent](https://hermes-agent.nousresearch.com), the SKILL.md gives the agent full awareness of librarian's tools.

### Option A — Skill market (if published)

```bash
hermes skills install librarian
```

### Option B — Manual install

Clone the repo and symlink or copy the SKILL.md into your Hermes skills directory:

```bash
# Windows (PowerShell):
New-Item -ItemType Junction -Path "$env:LOCALAPPDATA\hermes\skills\librarian" -Target "D:\path\to\librarian"

# Linux / macOS:
ln -s /path/to/librarian ~/.hermes/skills/librarian
```

Or copy only the SKILL.md:

```bash
cp /path/to/librarian/SKILL.md ~/.hermes/skills/librarian/
```

### Verify

In a Hermes session, the agent should load the librarian skill automatically when processing academic papers.

---

## 🧩 Install via npx skills

```bash
npx skills add e-zz/librarian
```

---

## 📦 What's included

| Component | What it does | Installed via |
|-----------|-------------|---------------|
| `mineru-api` | PDF → Markdown OCR | pip |
| `pdf-downloader` | DOI/arXiv → PDF download | pip |
| `pipeline-async` | Batch OCR → upload | pip |
| `ragflow-upload` | Markdown → RAGFlow | pip |
| `gs-scrape` | Google Scholar scraping | pip (+ CloakBrowser) |
| `scihub-download` | Sci-Hub/PMC PDF extraction | pip (+ CloakBrowser) |
| `deposit` | File placement + manifest | pip |
| `zotero-pipeline` | Zotero → OCR bridge | pip |
| `SKILL.md` | Agent instructions | skill install |
| `docs/` | Setup guides, env reference | repo clone or docs site |
