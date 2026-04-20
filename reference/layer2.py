"""
Layer 2 — LLM Extraction (Production)
Uses AWS Bedrock qwen/qwen3-32b for extraction.

LLM swap note: to move back to Groq, replace the call_llm() function only.
All surrounding logic (chunking, prompting, merging) stays the same.
"""

import json
import sys
import time
from pathlib import Path
from collections import defaultdict
import boto3
import os

# ── Config ────────────────────────────────────────────────────────────────────

BEDROCK_REGION  = os.getenv("AWS_REGION", "ap-southeast-2")
BEDROCK_MODEL   = "qwen.qwen3-32b-v1:0"
MAX_TOKENS      = 4096
TEMPERATURE     = 0.6
TOP_P           = 0.95

PAGE_CHUNK_SIZE = 4

MAX_RETRIES     = 3
RETRY_DELAY_SEC = 3


# ── Step 1 — Resolve overlapping blocks ──────────────────────────────────────

PRIORITY = {
    ("pdfplumber", "table"): 3,
    ("pdfplumber", "text"):  2,
    ("pymupdf",    "text"):  1,
}

def resolve_overlaps(blocks: list[dict]) -> list[dict]:
    for b in blocks:
        b.setdefault("suppressed", False)

    pages = defaultdict(list)
    for i, b in enumerate(blocks):
        pages[b["page"]].append(i)

    for page_indices in pages.values():
        for i in range(len(page_indices)):
            for j in range(i + 1, len(page_indices)):
                a  = blocks[page_indices[i]]
                bk = blocks[page_indices[j]]

                if not (a["overlap"] and bk["overlap"]):
                    continue
                if a["source"] == bk["source"]:
                    continue

                pri_a = PRIORITY.get((a["source"],  a["type"]),  0)
                pri_b = PRIORITY.get((bk["source"], bk["type"]), 0)

                if pri_a >= pri_b:
                    blocks[page_indices[j]]["suppressed"] = True
                else:
                    blocks[page_indices[i]]["suppressed"] = True

    kept       = sum(1 for b in blocks if not b["suppressed"])
    suppressed = sum(1 for b in blocks if b["suppressed"])
    print(f"[layer2] dedup resolved — kept: {kept}  suppressed: {suppressed}")
    return blocks


# ── Step 2 — Assemble blocks into readable pages ──────────────────────────────

def assemble_pages(blocks: list[dict]) -> dict[int, dict]:
    pages = defaultdict(lambda: {"text_words": [], "tables": []})

    for b in blocks:
        if b["suppressed"]:
            continue
        text = b["text"].strip()
        if not text:
            continue

        if b["type"] == "table":
            pages[b["page"]]["tables"].append(text)
        else:
            pages[b["page"]]["text_words"].append((b["bbox"], text))

    assembled = {}

    for page_num in sorted(pages.keys()):
        raw = pages[page_num]

        sorted_words = sorted(
            raw["text_words"],
            key=lambda x: (round(x[0][1] / 5) * 5, x[0][0])
        )

        lines   = []
        current = []
        last_y  = None

        for bbox, word in sorted_words:
            y_mid = (bbox[1] + bbox[3]) / 2
            if last_y is None or abs(y_mid - last_y) <= 4:
                current.append(word)
            else:
                if current:
                    lines.append(" ".join(current))
                current = [word]
            last_y = y_mid

        if current:
            lines.append(" ".join(current))

        assembled[page_num] = {
            "text":   "\n".join(lines),
            "tables": raw["tables"],
        }

    total_words = sum(len(v["text"].split()) for v in assembled.values())
    print(f"[layer2] assembled {len(assembled)} pages — {total_words} words (was {len(blocks)} raw blocks)")
    return assembled


# ── Step 3 — Chunk pages ──────────────────────────────────────────────────────

def chunk_pages(assembled: dict[int, dict], chunk_size: int) -> list[list[int]]:
    page_nums = sorted(assembled.keys())
    chunks    = [page_nums[i:i + chunk_size] for i in range(0, len(page_nums), chunk_size)]
    print(f"[layer2] {len(page_nums)} pages → {len(chunks)} chunks of ≤{chunk_size} pages each")
    return chunks


# ── Step 4 — Build prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "You are a strict JSON-only extraction engine. "
    "Never output anything except a single valid JSON object. "
    "No explanation. No markdown. No code fences. No preamble. "

    "You will receive a JSON array of blocks extracted from a PDF document, produced by two libraries "
    "(pdfplumber and PyMuPDF) plus a raw_text pass. "
    "Because of this, duplicate or near-duplicate content will be present — treat them as the same information and deduplicate. "

    "Each block has the following fields: "
    "  'source' : the library that produced this block — 'pdfplumber', 'pymupdf', or 'raw_text'. "
    "  'page'   : the PDF page number this block came from (integer, 1-based). "
    "  'bbox'   : [x0, y0, x1, y1] in PDF points — x0/y0 is the top-left corner, x1/y1 is the bottom-right corner. "
    "             Smaller y0 means higher on the page. Blocks with similar y0/y1 ranges are on the same horizontal band. "
    "             Blocks with similar x0 values are left-aligned to the same column. "
    "  'type'   : 'text' for prose or labels, 'table' for tabular content. "
    "  'text'   : the raw extracted content — may be broken, garbled, or contain artefacts. "
    "  'overlap': true means this block's bbox overlaps with a block from another source on the same page — "
    "             both represent the same physical region of the PDF, treat as duplicate. "

    "When resolving ambiguous or conflicting values, prefer the block whose bbox is spatially closest to the field label. "
    "A value belongs to a label when both share the same page and the value's x0 is to the right of the label "
    "on the same horizontal band (overlapping y ranges), or the value's y0 is just below the label's y1. "
    "Do not pull values from blocks that are far away on the page or on a different page. "
    "Only associate a value with a field if it appears on the same page or the immediately following page. Never reach further than that. "

    "The extracted text may be broken, garbled, or split mid-word due to PDF parsing limitations. "
    "Reconstruct and correct broken words, split numbers, and misaligned columns before writing the output. "
    "For example, '4 53.00' should be read as '453.00', 'SAYBOL T' as 'SAYBOLT', etc. "

    "Use all three sources (pdfplumber, pymupdf) together to cross-validate and produce the most accurate and complete extraction. "
    "raw_text blocks preserve the visual layout of the page and are especially useful for resolving column alignment. "

    "Think carefully before finalising each field value. leave the values which you are not sure"
    "Output only a single valid JSON object with no extra text."
)

def build_chunk_prompt(assembled: dict[int, dict], page_nums: list[int]) -> str:
    doc_lines = []
    for pn in page_nums:
        content = assembled[pn]
        doc_lines.append(f"=== PAGE {pn} ===")
        if content["text"].strip():
            doc_lines.append(content["text"])
        for i, table in enumerate(content["tables"], 1):
            doc_lines.append(f"[TABLE {i}]")
            doc_lines.append(table)
            doc_lines.append(f"[/TABLE {i}]")

    document_text = "\n".join(doc_lines)

    return f"""Extract ALL important fields from the document section below.

RULES:
- Return ONLY a valid JSON object.
- Use null if a field is not found. Do not guess or infer.
- Dates → ISO format YYYY-MM-DD where possible, otherwise return as-is.
- Amounts → include currency symbol if present in the document.
- Nested fields (bank details, address, line items) → nested JSON objects or arrays.
- Field names → snake_case, descriptive.
- Extract everything with business value: invoice numbers, dates, amounts, vendor,
  client, payment terms, bank details, addresses, line items, totals, taxes,
  reference numbers, product/service descriptions, quantities, contacts.

DOCUMENT SECTION (pages {page_nums[0]}–{page_nums[-1]}):
{document_text}
"""


# ── Step 5 — Call Bedrock with retry ──────────────────────────────────────────
#
# SWAP POINT: To move back to Groq, replace this function only.
# Keep the signature:  call_llm(prompt: str, chunk_label: str) -> str
#
def call_llm(prompt: str, chunk_label: str) -> str:
    client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

    messages = [
        {
            "role": "user",
            "content": [{"text": prompt}]
        }
    ]

    system = [{"text": SYSTEM_PROMPT}]

    inference_config = {
        "maxTokens":   MAX_TOKENS,
        "temperature": TEMPERATURE,
        "topP":        TOP_P,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[layer2] {chunk_label} attempt {attempt} ...", end=" ", flush=True)

            response = client.converse(
                modelId=BEDROCK_MODEL,
                system=system,
                messages=messages,
                inferenceConfig=inference_config,
            )

            full_response = response["output"]["message"]["content"][0]["text"]
            print(f" done ({len(full_response)} chars)")
            return full_response.strip()

        except Exception as e:
            print(f"\n[layer2] {chunk_label} attempt {attempt} error: {e}")
            if attempt < MAX_RETRIES:
                print(f"[layer2] retrying in {RETRY_DELAY_SEC}s ...")
                time.sleep(RETRY_DELAY_SEC)
            else:
                print(f"[layer2] {chunk_label} — all retries exhausted, returning empty")
                return "{}"

    return "{}"


# ── Step 6 — Parse JSON output ────────────────────────────────────────────────

def parse_output(raw: str, chunk_label: str) -> dict:
    clean = raw.strip()

    if clean.startswith("```"):
        lines = clean.splitlines()
        clean = "\n".join(l for l in lines if not l.strip().startswith("```"))
        clean = clean.strip()

    start = clean.find("{")
    end   = clean.rfind("}")
    if start != -1 and end != -1 and end > start:
        clean = clean[start:end + 1]

    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"[layer2] WARNING {chunk_label} — JSON parse failed: {e}")
        return {}


# ── Step 7 — Merge results ────────────────────────────────────────────────────

def merge_results(chunk_results: list[dict]) -> dict:
    merged = {}

    for result in chunk_results:
        for key, value in result.items():
            if key not in merged:
                merged[key] = value
                continue

            existing = merged[key]

            if isinstance(existing, list) and isinstance(value, list):
                merged[key] = existing + value
            elif existing is None and value is not None:
                merged[key] = value
            elif isinstance(existing, dict) and isinstance(value, dict):
                for subkey, subval in value.items():
                    if subkey not in existing or existing[subkey] is None:
                        existing[subkey] = subval

    return merged


# ── Public entry point ────────────────────────────────────────────────────────

def run_layer2(layer1_output_path: str, doc_id: str | None = None) -> dict:
    """
    Run layer 2 LLM extraction.
    If doc_id is provided, saves the structured JSON to MongoDB (raw_extractions).
    """
    raw_json = Path(layer1_output_path).read_text(encoding="utf-8", errors="ignore")
    blocks: list[dict] = json.loads(raw_json)
    print(f"[layer2] loaded {len(blocks)} blocks from {layer1_output_path}")

    blocks    = resolve_overlaps(blocks)
    assembled = assemble_pages(blocks)
    chunks    = chunk_pages(assembled, PAGE_CHUNK_SIZE)

    chunk_results = []
    for idx, page_nums in enumerate(chunks, 1):
        words_in_chunk = sum(len(assembled[p]["text"].split()) for p in page_nums)
        chunk_label    = f"chunk {idx}/{len(chunks)} pages={page_nums}"
        print(f"[layer2] {chunk_label} — {words_in_chunk} words")

        prompt  = build_chunk_prompt(assembled, page_nums)
        raw_out = call_llm(prompt, chunk_label)
        parsed  = parse_output(raw_out, chunk_label)

        print(f"[layer2] chunk {idx} — {len(parsed)} fields extracted")
        chunk_results.append(parsed)

    final = merge_results(chunk_results)
    print(f"[layer2] merge complete — {len(final)} total unique fields")

    # ── MongoDB save (non-fatal) ──────────────────────────────────────────────
    if doc_id:
        try:
            from db_setup import save_raw_extraction
            save_raw_extraction(doc_id, 2, final)
            print(f"[layer2] saved to MongoDB — doc_id: {doc_id}")
        except Exception as exc:
            print(f"[layer2] MongoDB save skipped: {exc}")

    return final


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Guard: only run CLI when invoked directly as  python layer2.py <file>
    # Streamlit sets sys.argv to ['...streamlit/__main__.py', 'run', 'layer3.py']
    # so we check that argv[1] looks like a json path, not a streamlit sub-command.
    _is_direct = (
        len(sys.argv) >= 2
        and sys.argv[1].endswith(".json")
    )
    if not _is_direct:
        sys.exit(0)

    if len(sys.argv) < 2:
        print("Usage: python layer2.py <layer1_output.json> [output.json]")
        sys.exit(1)

    layer1_path = sys.argv[1]
    out_path    = sys.argv[2] if len(sys.argv) > 2 else "layer2_output.json"

    try:
        result = run_layer2(layer1_path)

        Path(out_path).write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print(f"\n{'=' * 55}")
        print("LAYER 2 — FINAL STRUCTURED OUTPUT")
        print("=" * 55)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("=" * 55)
        print(f"saved → {out_path}")

    except FileNotFoundError:
        print(f"ERROR: file not found: {layer1_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: bad JSON in layer1 output: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
