#!/usr/bin/env python3
"""librarian-pipeline — one-shot end-to-end paper workflow.

Usage:
  librarian-pipeline <pdf_or_arxiv_id_or_doi>...
  librarian-pipeline --input ids.txt
  librarian-pipeline --demo                     # download + OCR a sample paper

Examples:
  librarian-pipeline 2311.08990                 # arXiv ID → download → OCR
  librarian-pipeline 10.1007/s42484-025-00254-8  # DOI → download → OCR
  librarian-pipeline paper.pdf                  # local PDF → OCR
  librarian-pipeline --demo                     # try with a real paper
  librarian-pipeline --ragflow --dataset-id <id> 2311.08990  # + RAGFlow upload
"""
import argparse, subprocess, sys, json
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent


def pip_script(name: str) -> str:
    """Return the CLI entry point or python scripts/ fallback."""
    return name  # relies on pip-installed entry points


def run(cmd: list, label: str) -> bool:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"  ✗ FAILED (exit {r.returncode})")
        return False
    print(f"  ✓ OK")
    return True


def download_and_ocr(item: str, out_dir: Path, ragflow: bool, dataset_id: str | None):
    """Resolve input → download (if needed) → OCR."""
    item = item.strip()
    
    # Case 1: local PDF file
    if Path(item).exists() and item.lower().endswith('.pdf'):
        pdf_path = Path(item)
        print(f"\n  Local PDF: {pdf_path.name}")
        ocr_input = str(pdf_path)
    
    # Case 2: arXiv ID or DOI — download first
    else:
        print(f"\n  Resolving: {item}")
        if not run(
            [pip_script("pdf-downloader"), item, "--out", str(out_dir)],
            f"Download {item}"
        ):
            return
    
        # Find the downloaded file
        pdfs = list(out_dir.glob("*.pdf"))
        if not pdfs:
            print(f"  ✗ No PDF downloaded for {item}")
            return
        ocr_input = str(pdfs[-1])
    
    # OCR
    if not run(
        [pip_script("mineru-api"), "parse", ocr_input],
        f"OCR {Path(ocr_input).name}"
    ):
        return
    
    print(f"\n  ✓ Complete. Output in ./_raw/")


def demo():
    """Download a real arXiv paper and OCR it."""
    print("  Demo: downloading arXiv:2311.08990 (VQE review paper)")
    download_and_ocr("2311.08990", Path("./downloads"), False, None)
    print(f"\n{'='*60}")
    print(f"  DONE. Try:")
    print(f"    grep -r 'variational' ./_raw/")
    print(f"{'='*60}")


def main():
    p = argparse.ArgumentParser(
        description="librarian-pipeline — one-shot paper workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("items", nargs="*", help="arXiv IDs, DOIs, or PDF paths")
    p.add_argument("--input", "-i", help="File with one ID per line")
    p.add_argument("--demo", action="store_true", help="Run with a sample paper")
    p.add_argument("--ragflow", action="store_true", help="Also upload to RAGFlow")
    p.add_argument("--dataset-id", help="RAGFlow dataset ID (required with --ragflow)")
    args = p.parse_args()

    if args.ragflow and not args.dataset_id:
        print("Error: --dataset-id is required with --ragflow")
        sys.exit(1)
    
    if args.demo:
        demo()
        return
    
    items = list(args.items)
    if args.input:
        with open(args.input) as f:
            items.extend(line.strip() for line in f if line.strip() and not line.startswith('#'))
    
    if not items:
        print("Error: provide at least one arXiv ID, DOI, or PDF path (or --demo)")
        p.print_help()
        sys.exit(1)
    
    out_dir = Path("./downloads")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    for item in items:
        download_and_ocr(item, out_dir, args.ragflow, args.dataset_id)


if __name__ == "__main__":
    main()
