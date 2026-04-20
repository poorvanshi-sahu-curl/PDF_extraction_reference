import json
from collections import defaultdict

PRIORITY = {
    ("pdfplumber", "table"): 3,
    ("pdfplumber", "text"):  2,
    ("pymupdf",    "text"):  1,
}

def resolve_overlaps(blocks):
    for b in blocks:
        b["suppressed"] = False

    pages = defaultdict(list)
    for i, b in enumerate(blocks):
        pages[b["page"]].append(i)

    for page_indices in pages.values():
        for i in range(len(page_indices)):
            for j in range(i + 1, len(page_indices)):
                a = blocks[page_indices[i]]
                b = blocks[page_indices[j]]

                if not (a.get("overlap") and b.get("overlap")):
                    continue

                if a.get("source") == b.get("source"):
                    continue

                pri_a = PRIORITY.get((a.get("source"), a.get("type")), 0)
                pri_b = PRIORITY.get((b.get("source"), b.get("type")), 0)

                if pri_a >= pri_b:
                    b["suppressed"] = True
                else:
                    a["suppressed"] = True

    return blocks


# 🔹 LOAD YOUR ACTUAL INPUT JSON FROM FILE
with open("input.json", "r") as f:
    payload = json.load(f)

blocks = payload["blocks"]

# Run logic
result = resolve_overlaps(blocks)

# Print output
print(json.dumps(result, indent=2))