#!/usr/bin/env python3
"""
GS Full Metadata Scraper — scrape ALL papers from one or more Google Scholar
profiles, extract PDF/arXiv/DOI links, and save per-researcher JSON output.

**Dependencies**
- cloakbrowser (pip install cloakbrowser) — headless browser automation
- Python 3.8+ (standard library only beyond cloakbrowser)

**Usage**
    # Single researcher by GS user ID
    python gs_full_scrape.py --uid Ub1bvfkAAAAJ

    # Single researcher with custom output dir and dedup file
    python gs_full_scrape.py --uid Ub1bvfkAAAAJ --output-dir ./output --dedup-file ./my_titles.txt

    # Batch mode — JSON file with a list of researchers
    python gs_full_scrape.py --uid-file researchers.json --output-dir ./output

    # Batch JSON file format:
    # [
    #     {"name": "黄合良", "uid": "Ub1bvfkAAAAJ", "institution": "USTC"},
    #     {"name": "陈建鑫", "uid": "V7Ye1uQAAAAJ", "institution": "Alibaba"}
    # ]

**Output**
    For each researcher, a JSON file named <name>_papers.json is written to the
    output directory, containing metadata, citation stats, and the full list of
    papers with extracted links.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Third-party import — cloakbrowser handles headless browser launch
# ---------------------------------------------------------------------------
from cloakbrowser import launch_context_async


# ===========================================================================
# Core scraping routine (reusable, researcher-agnostic)
# ===========================================================================
async def scrape_all(name: str, uid: str, institution: str, zotero_titles: set) -> dict:
    """Scrape EVERY paper from a single Google Scholar profile page.

    Args:
        name: Display name for the researcher (used in output filename).
        uid: Google Scholar user ID (the "user=..." parameter in the profile URL).
        institution: Optional institution string — stored in output metadata.
        zotero_titles: Set of normalized title strings for dedup checking.
                        Pass an empty set to skip dedup.

    Returns:
        A dict with the scraped profile data and paper list ready to save as JSON.
        Returns None if a fatal error occurs.
    """
    print(f"\n{'=' * 60}")
    print(f"  {name} (uid={uid})")
    print(f"{'=' * 60}")

    # Launch a headless browser context
    #   humanize=False disables stealth anti-bot measures for speed;
    #   set humanize=True if Google Scholar starts blocking.
    ctx = await launch_context_async(
        headless=True, humanize=False, viewport={"width": 1280, "height": 900}
    )
    page = await ctx.new_page()

    try:
        # Navigate to the GS profile with up to 100 papers per page request
        await page.goto(
            f"https://scholar.google.com/citations?user={uid}&hl=en&pagesize=100",
            timeout=30000,
        )
        await asyncio.sleep(5)  # Let JS-rendered content settle

        # --- Extract citation stats from the sidebar ---
        tds = await page.evaluate(
            "Array.from(document.querySelectorAll('td.gsc_rsb_std')).map(t=>t.textContent.trim())"
        )
        cites = tds[0] if tds else "?"
        h_idx = tds[2] if len(tds) > 2 else "?"
        print(f"  Stats: {cites} cites, h={h_idx}")

        # --- Click 'Show more' repeatedly until all papers are loaded ---
        # GS truncates results; each click loads the next batch.
        clicks = 0
        while clicks < 30:  # safety cap to avoid infinite loop
            btn = await page.evaluate(
                """() => {
                let b = document.querySelector('#gsc_bpf_more');
                if (!b || b.disabled || b.style.display === 'none') return null;
                return true;
            }"""
            )
            if not btn:
                break  # No more button — all papers loaded
            try:
                await page.click("#gsc_bpf_more")
                clicks += 1
                await asyncio.sleep(2)  # Brief pause for next batch to render
            except Exception:
                break
        print(f"  Loaded all papers ({clicks} 'Show more' clicks)")

        # --- Extract ALL paper rows from the publications table ---
        # Each <tr class="gsc_a_tr"> is one publication entry.
        papers = await page.evaluate(
            """() => {
            return Array.from(document.querySelectorAll('tr.gsc_a_tr')).map(r => {
                let titleEl = r.querySelector('.gsc_a_at');
                let title = titleEl?.textContent?.trim() || '';
                let gsUrl = titleEl?.href || '';
                let cites = r.querySelector('.gsc_a_ac')?.textContent?.trim() || '0';
                let year = r.querySelector('.gsc_a_y span')?.textContent?.trim() || '';
                let authors = r.querySelectorAll('.gs_gray')[0]?.textContent?.trim() || '';
                let journal = r.querySelectorAll('.gs_gray')[1]?.textContent?.trim() || '';

                // Extract links: arXiv, PDF, DOI
                let links = [];
                let allLinks = r.querySelectorAll('a');
                for (let a of allLinks) {
                    let href = a.href || '';
                    let text = a.textContent?.trim() || '';
                    if (href.includes('arxiv.org'))
                        links.push({type: 'arxiv', url: href});
                    else if (href.endsWith('.pdf') || text === '[PDF]')
                        links.push({type: 'pdf', url: href});
                    else if (href.includes('doi.org'))
                        links.push({type: 'doi', url: href});
                }
                return {title, gsUrl, cites, year, authors, journal, links};
            });
        }"""
        )

        total = len(papers)
        print(f"  Total papers: {total}")

        # --- Build structured result list ---
        results = []
        for p in papers:
            # Dedup check: does this paper title (truncated) appear in Zotero titles?
            # The zotero_titles set is built from filenames so we use the first 80 chars.
            in_zotero = any(
                p["title"][:60].lower() in zt for zt in zotero_titles
            ) if zotero_titles else False
            results.append(
                {
                    "title": p["title"],
                    "cites": p["cites"],
                    "year": p["year"],
                    "authors": p["authors"],
                    "journal": p.get("journal", ""),
                    "gs_url": p.get("gsUrl", ""),
                    "links": p.get("links", []),
                    "in_zotero": in_zotero,
                }
            )

        # Summary stats for this researcher
        with_link = sum(1 for r in results if r["links"])
        in_zot = sum(1 for r in results if r["in_zotero"])
        print(f"  → {with_link}/{total} have direct PDF/arXiv links, {in_zot} in Zotero")

        return {
            "name": name,
            "uid": uid,
            "institution": institution,
            "gs_cites": cites,
            "h_index": h_idx,
            "total_papers": total,
            "papers": results,
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

    finally:
        # Always clean up the browser page and context
        await page.close()
        await ctx.close()


# ===========================================================================
# Helpers
# ===========================================================================
def load_zotero_titles(dedup_file: Path | None) -> set:
    """Load previously-downloaded paper titles for dedup checking.

    The dedup file should contain one filename (or title) per line.
    Each line is converted to lowercase and truncated to 80 characters
    for fuzzy matching against scraped paper titles.

    Args:
        dedup_file: Path to a text file with titles/filenames. If None,
                    returns an empty set (no dedup).

    Returns:
        A set of normalized title strings.
    """
    titles: set[str] = set()
    if dedup_file is None:
        return titles

    dedup_path = Path(dedup_file)
    if not dedup_path.exists():
        print(f"  [WARN] Dedup file not found: {dedup_file} — skipping dedup")
        return titles

    for line in dedup_path.read_text(encoding="utf-8").strip().split("\n"):
        line = line.strip()
        if line:
            # Normalise like the original script — extract stem and lowercase
            titles.add(Path(line).stem[:80].lower())
    print(f"  Loaded {len(titles)} titles from dedup file: {dedup_file}")
    return titles


def load_researchers(uid_file: Path | None) -> list[dict]:
    """Load one or more researcher definitions from a JSON file.

    Expected JSON format (list of objects):
        [
            {"name": "黄合良", "uid": "Ub1bvfkAAAAJ", "institution": "USTC"},
            ...
        ]

    "institution" is optional; defaults to empty string if omitted.

    Args:
        uid_file: Path to a JSON file. If None, returns an empty list.

    Returns:
        List of researcher dicts with keys: name, uid, institution.
    """
    if uid_file is None:
        return []
    path = Path(uid_file)
    if not path.exists():
        print(f"[ERROR] uid-file not found: {uid_file}", file=sys.stderr)
        sys.exit(1)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("[ERROR] uid-file must be a JSON array", file=sys.stderr)
        sys.exit(1)
    # Normalise: ensure every entry has 'name', 'uid', and 'institution'
    researchers = []
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            print(f"[ERROR] Entry {i} in uid-file is not a JSON object", file=sys.stderr)
            sys.exit(1)
        if "uid" not in entry:
            print(f"[ERROR] Entry {i} in uid-file is missing 'uid'", file=sys.stderr)
            sys.exit(1)
        researchers.append(
            {
                "name": entry.get("name", f"researcher_{entry['uid']}"),
                "uid": entry["uid"],
                "institution": entry.get("institution", entry.get("inst", "")),
            }
        )
    return researchers


# ===========================================================================
# CLI entry point
# ===========================================================================
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace with attributes: uid, uid_file, output_dir, dedup_file.
    """
    parser = argparse.ArgumentParser(
        description="GS Full Metadata Scraper — scrape ALL papers from Google Scholar profiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s --uid Ub1bvfkAAAAJ\n"
            "  %(prog)s --uid Ub1bvfkAAAAJ --output-dir ./output --dedup-file ./titles.txt\n"
            "  %(prog)s --uid-file researchers.json --output-dir ./output\n"
        ),
    )

    # Mutually exclusive: single uid vs batch file
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--uid",
        type=str,
        default=None,
        help="Google Scholar user ID (e.g., Ub1bvfkAAAAJ)",
    )
    target_group.add_argument(
        "--uid-file",
        type=str,
        default=None,
        help="Path to JSON file with list of researcher objects [{name, uid, institution?}]",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="./gs_output/",
        help="Output directory for JSON result files (default: ./gs_output/)",
    )
    parser.add_argument(
        "--dedup-file",
        type=str,
        default=None,
        help="Optional path to a text file of filenames for Zotero dedup checking",
    )
    # Optional metadata for single-uid mode
    parser.add_argument(
        "--name",
        type=str,
        default=None,
        help="Researcher name (for single-uid mode; defaults to the uid value)",
    )
    parser.add_argument(
        "--institution",
        type=str,
        default="",
        help="Institution name (for single-uid mode; optional metadata)",
    )

    return parser.parse_args(argv)


def main() -> None:
    """Synchronous entry point for CLI — wraps the async orchestrator."""
    asyncio.run(async_main())


async def async_main() -> None:
    """Orchestrate scraping for all configured researchers."""
    args = parse_args()

    # --- Resolve output directory ---
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Load dedup titles (optional) ---
    dedup_file = Path(args.dedup_file) if args.dedup_file else None
    zotero_titles = load_zotero_titles(dedup_file)

    # --- Resolve researcher list ---
    if args.uid_file:
        researchers = load_researchers(Path(args.uid_file))
        if not researchers:
            print("[ERROR] uid-file is empty", file=sys.stderr)
            sys.exit(1)
        print(f"Loaded {len(researchers)} researchers from: {args.uid_file}")
    else:
        # Single-uid mode
        name = args.name or f"uid_{args.uid}"
        researchers = [
            {"name": name, "uid": args.uid, "institution": args.institution}
        ]
        print(f"Single researcher: {name} (uid={args.uid})")

    print(f"Output directory: {output_dir.resolve()}")
    print()

    # --- Scrape each researcher ---
    for i, r in enumerate(researchers, 1):
        print(f"[{i}/{len(researchers)}] ", end="")
        try:
            data = await scrape_all(r["name"], r["uid"], r["institution"], zotero_titles)
            if data is None:
                continue  # Fatal error already logged inside scrape_all

            # Save per-researcher JSON
            out_file = output_dir / f"{r['name']}_papers.json"
            out_file.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"  → Saved: {out_file}")

            # Brief pause between researchers to avoid rate-limiting
            if i < len(researchers):
                await asyncio.sleep(10)

        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            import traceback

            traceback.print_exc()
            # Continue to next researcher rather than aborting entirely

    print(f"\n{'=' * 60}")
    print(f"  ALL DONE. Results in: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
