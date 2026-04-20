# Invoice Processing Pipeline

Three-layer PDF invoice extraction system with MongoDB storage and Streamlit review UI.

## Files

| File | Purpose |
|------|---------|
| `db_setup.py` | MongoDB connection, collection creation, all CRUD helpers |
| `layer1.py` | PDF extraction (pdfplumber + PyMuPDF, dedup by bbox) |
| `layer2.py` | LLM extraction via Groq · Qwen3-32B → structured JSON |
| `layer3.py` | Streamlit review UI · validation · MongoDB storage |
| `requirements.txt` | Python dependencies |

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start MongoDB (local)
```bash
# macOS (Homebrew)
brew services start mongodb-community

# Linux
sudo systemctl start mongod

# Docker
docker run -d -p 27017:27017 --name mongo mongo:7
```

### 3. Create collections & indexes
```bash
python db_setup.py
```

### 4. Run the Streamlit UI (all three layers)
```bash
streamlit run layer3.py
```
Open http://localhost:8501 in your browser.

The UI lets you:
- **Run Full Pipeline** — upload a PDF and run Layer 1 + Layer 2 automatically
- **Review Document** — load a `layer2_output.json` directly for review
- **Invoice History** — browse all approved invoices stored in MongoDB

### 5. Run layers from the command line (optional)
```bash
# Layer 1 only
python layer1.py invoice.pdf layer1_output.json

# Layer 2 only (needs layer1 output)
python layer2.py layer1_output.json layer2_output.json
```

## MongoDB Collections

| Collection | Contents |
|------------|----------|
| `invoices` | Approved invoice documents (upserted by doc_id) |
| `bank_details` | Payment methods per document |
| `raw_extractions` | Layer 1 blocks + Layer 2 JSON (archived) |
| `audit_log` | Every action: LOADED, EDITED, APPROVED, REJECTED |

## Swapping Groq → AWS Bedrock

Only one function in `layer2.py` needs to change — `call_llm()`.
Keep the signature:
```python
def call_llm(prompt: str, chunk_label: str) -> str:
    ...
```
Replace the Groq client calls with Bedrock's `boto3` invocations.
Everything else (chunking, prompting, merging, MongoDB save) stays the same.

## MongoDB URI

Change `MONGO_URI` in `db_setup.py`:
```python
MONGO_URI = "mongodb://localhost:27017"   # local
MONGO_URI = "mongodb+srv://user:pass@cluster.mongodb.net/"  # Atlas
```
