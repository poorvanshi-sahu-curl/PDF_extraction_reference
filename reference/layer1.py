"""
Layer 1 — Extraction
Runs pdfplumber and PyMuPDF in parallel, tags every block,
then deduplicates by bounding box (flags overlap, never deletes).

Block grouping:
  - Text  → 5 lines per block
  - Table → one block per table (unchanged)
"""

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Literal

import pdfplumber
import fitz  # PyMuPDF


# ── Config ────────────────────────────────────────────────────────────────────

LINES_PER_BLOCK = 500   # change this number to adjust grouping


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Block:
    source:  Literal["pdfplumber", "pymupdf"]
    page:    int
    bbox:    tuple[float, float, float, float]
    type:    Literal["text", "table"]
    text:    str
    overlap: bool = False


# ── Shared helpers ────────────────────────────────────────────────────────────

Y_TOLERANCE = 4  # px — words within this y-distance are on the same line

def _words_to_lines(words: list[dict]) -> list[dict]:
    """Group word-dicts into line-dicts sorted in reading order."""
    if not words:
        return []

    sorted_words = sorted(
        words,
        key=lambda w: (round(w["top"] / Y_TOLERANCE) * Y_TOLERANCE, w["x0"])
    )

    lines    = []
    cur_line = [sorted_words[0]]

    for w in sorted_words[1:]:
        prev_y = (cur_line[-1]["top"] + cur_line[-1]["bottom"]) / 2
        curr_y = (w["top"] + w["bottom"]) / 2
        if abs(curr_y - prev_y) <= Y_TOLERANCE:
            cur_line.append(w)
        else:
            lines.append(_merge_line(cur_line))
            cur_line = [w]

    if cur_line:
        lines.append(_merge_line(cur_line))

    return lines


def _merge_line(words: list[dict]) -> dict:
    return {
        "text":   " ".join(w["text"] for w in words),
        "x0":     min(w["x0"]     for w in words),
        "top":    min(w["top"]    for w in words),
        "x1":     max(w["x1"]     for w in words),
        "bottom": max(w["bottom"] for w in words),
    }


def _lines_to_blocks(lines: list[dict], source: str, page: int, n: int) -> list[Block]:
    """Chunk lines into blocks of n lines. bbox spans the full group."""
    blocks = []
    for i in range(0, len(lines), n):
        group = lines[i:i + n]
        text  = "\n".join(ln["text"] for ln in group)
        bbox  = (
            min(ln["x0"]     for ln in group),
            min(ln["top"]    for ln in group),
            max(ln["x1"]     for ln in group),
            max(ln["bottom"] for ln in group),
        )
        blocks.append(Block(source=source, page=page, bbox=bbox, type="text", text=text))
    return blocks


# ── pdfplumber branch ─────────────────────────────────────────────────────────

def extract_pdfplumber(pdf_path: str) -> list[Block]:
    blocks: list[Block] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):

            # tables — one block each, kept separate
            tables       = page.find_tables()
            table_bboxes = []

            for table in tables:
                x0, top, x1, bottom = table.bbox
                table_bboxes.append(table.bbox)
                blocks.append(Block(
                    source = "pdfplumber",
                    page   = page_num,
                    bbox   = (x0, top, x1, bottom),
                    type   = "table",
                    text   = _table_to_text(table.extract()),
                ))

            # text — words → lines → 5-line blocks
            raw_words = page.extract_words(
                keep_blank_chars = False,
                use_text_flow    = True,
                x_tolerance      = 3,
                y_tolerance      = 3,
            )
            outside_words = [
                w for w in raw_words
                if not _inside_any(w["x0"], w["top"], w["x1"], w["bottom"], table_bboxes)
            ]
            lines = _words_to_lines(outside_words)
            blocks.extend(_lines_to_blocks(lines, "pdfplumber", page_num, LINES_PER_BLOCK))

    return blocks


def _table_to_text(rows: list) -> str:
    if not rows:
        return ""
    return "\n".join(" | ".join(cell or "" for cell in row) for row in rows)


def _inside_any(x0, y0, x1, y1, bboxes: list[tuple]) -> bool:
    margin = 2
    for bx0, by0, bx1, by1 in bboxes:
        if (x0 >= bx0 - margin and y0 >= by0 - margin and
                x1 <= bx1 + margin and y1 <= by1 + margin):
            return True
    return False


# ── PyMuPDF branch ────────────────────────────────────────────────────────────

def extract_pymupdf(pdf_path: str) -> list[Block]:
    blocks: list[Block] = []
    doc = fitz.open(pdf_path)

    for page_num, page in enumerate(doc, start=1):

        flags = (
            fitz.TEXT_PRESERVE_WHITESPACE |
            fitz.TEXT_PRESERVE_LIGATURES  |
            fitz.TEXT_DEHYPHENATE
        )
        raw = page.get_text("dict", flags=flags)

        word_dicts = []
        for block in raw.get("blocks", []):
            if block.get("type", 0) != 0:
                continue
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span.get("text", "").strip()
                    if not text:
                        continue
                    r = span["bbox"]
                    word_dicts.append({
                        "text":   text,
                        "x0":     r[0],
                        "top":    r[1],
                        "x1":     r[2],
                        "bottom": r[3],
                    })

        lines = _words_to_lines(word_dicts)
        blocks.extend(_lines_to_blocks(lines, "pymupdf", page_num, LINES_PER_BLOCK))

        # fallback if nothing extracted for this page
        page_blocks = [b for b in blocks if b.page == page_num and b.source == "pymupdf"]
        if not page_blocks:
            plain = page.get_text("text").strip()
            if plain:
                r = page.rect
                blocks.append(Block(
                    source = "pymupdf",
                    page   = page_num,
                    bbox   = (r.x0, r.y0, r.x1, r.y1),
                    type   = "text",
                    text   = plain,
                ))

    doc.close()
    return blocks


# ── Dedup by bounding box ─────────────────────────────────────────────────────

IOU_THRESHOLD = 0.85

def dedup_by_bbox(blocks: list[Block]) -> list[Block]:
    pages: dict[int, list[int]] = {}
    for i, b in enumerate(blocks):
        pages.setdefault(b.page, []).append(i)

    for page_indices in pages.values():
        for i in range(len(page_indices)):
            for j in range(i + 1, len(page_indices)):
                a = blocks[page_indices[i]]
                b = blocks[page_indices[j]]
                if a.source == b.source:
                    continue
                if _iou(a.bbox, b.bbox) >= IOU_THRESHOLD:
                    blocks[page_indices[i]].overlap = True
                    blocks[page_indices[j]].overlap = True

    return blocks


def _iou(a: tuple, b: tuple) -> float:
    ix0 = max(a[0], b[0]);  iy0 = max(a[1], b[1])
    ix1 = min(a[2], b[2]);  iy1 = min(a[3], b[3])
    inter_w = max(0.0, ix1 - ix0)
    inter_h = max(0.0, iy1 - iy0)
    inter   = inter_w * inter_h
    if inter == 0:
        return 0.0
    area_a = max(0.0, a[2] - a[0]) * max(0.0, a[3] - a[1])
    area_b = max(0.0, b[2] - b[0]) * max(0.0, b[3] - b[1])
    union  = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


# ── Report ────────────────────────────────────────────────────────────────────

def print_extraction_report(blocks: list[dict]):
    from collections import defaultdict

    print("\n" + "=" * 50)
    print("LAYER 1 — EXTRACTION REPORT")
    print("=" * 50)

    for src in ["pdfplumber", "pymupdf"]:
        src_blocks   = [b for b in blocks if b["source"] == src]
        text_blocks  = [b for b in src_blocks if b["type"] == "text"]
        table_blocks = [b for b in src_blocks if b["type"] == "table"]
        word_count   = sum(len(b["text"].split()) for b in src_blocks)
        char_count   = sum(len(b["text"])         for b in src_blocks)

        print(f"\n  [{src}]")
        print(f"    blocks      : {len(src_blocks)}")
        print(f"    text blocks : {len(text_blocks)}")
        print(f"    table blocks: {len(table_blocks)}")
        print(f"    words       : {word_count}")
        print(f"    characters  : {char_count}")

    print("\n  [per page breakdown]")
    pages = defaultdict(lambda: {"pdfplumber": 0, "pymupdf": 0})
    for b in blocks:
        pages[b["page"]][b["source"]] += len(b["text"].split())

    print(f"  {'Page':<8} {'pdfplumber (words)':<25} {'pymupdf (words)'}")
    print(f"  {'-' * 55}")
    for page_num in sorted(pages.keys()):
        p = pages[page_num]["pdfplumber"]
        m = pages[page_num]["pymupdf"]
        print(f"  {page_num:<8} {p:<25} {m}")

    flagged = [b for b in blocks if b["overlap"]]
    print(f"\n  [dedup]")
    print(f"    total blocks : {len(blocks)}")
    print(f"    flagged      : {len(flagged)}")
    print(f"    clean        : {len(blocks) - len(flagged)}")
    print("=" * 50 + "\n")


# ── Public entry point ────────────────────────────────────────────────────────

def run_layer1(pdf_path: str, doc_id: str | None = None) -> list[dict]:
    """
    Run layer 1 extraction.
    If doc_id is provided, saves the raw blocks to MongoDB (raw_extractions).
    """
    plumber_blocks = extract_pdfplumber(pdf_path)
    pymupdf_blocks = extract_pymupdf(pdf_path)

    all_blocks = plumber_blocks + pymupdf_blocks
    all_blocks = dedup_by_bbox(all_blocks)

    result = [asdict(b) for b in all_blocks]
    print_extraction_report(result)

    # ── MongoDB save (non-fatal) ──────────────────────────────────────────────
    if doc_id:
        try:
            from db_setup import save_raw_extraction
            save_raw_extraction(doc_id, 1, result)
            print(f"[layer1] saved to MongoDB — doc_id: {doc_id}")
        except Exception as exc:
            print(f"[layer1] MongoDB save skipped: {exc}")

    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    _is_direct = len(sys.argv) >= 2 and sys.argv[1].endswith(".pdf")
    if not _is_direct:
        sys.exit(0)

    if len(sys.argv) < 2:
        print("Usage: python layer1.py <path/to/file.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else "layer1_output.json"

    blocks = run_layer1(pdf_path)

    Path(out_path).write_text(
        json.dumps(blocks, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    total     = len(blocks)
    flagged   = sum(1 for b in blocks if b["overlap"])
    plumber_n = sum(1 for b in blocks if b["source"] == "pdfplumber")
    pymupdf_n = sum(1 for b in blocks if b["source"] == "pymupdf")

    print(f"Done — {total} blocks total")
    print(f"  pdfplumber : {plumber_n}")
    print(f"  pymupdf    : {pymupdf_n}")
    print(f"  flagged    : {flagged} overlapping pairs")
    print(f"  output     : {out_path}")
