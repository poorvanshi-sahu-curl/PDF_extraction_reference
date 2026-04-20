"""
DocParse — Streamlit UI
Calls FastAPI /extract → /clean, then saves to MongoDB.
"""

import json
import re
import uuid
import datetime
import tempfile
from pathlib import Path

import requests
import streamlit as st
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

API_BASE  = os.getenv("API_BASE", "http://localhost:8000")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME", "docparse")

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="DocParse · Invoice Intelligence",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"] { display: none !important; }

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; font-size: 14px; }
.stApp { background: #0d0f14; }
.main .block-container { padding: 0 !important; max-width: 100% !important; }

[data-testid="stSidebar"] { background: #0d0f14 !important; border-right: 1px solid #1e2230 !important; }
[data-testid="stSidebar"] * { color: #8b93a7 !important; }
[data-testid="stSidebar"] .stButton > button {
    background: transparent !important; border: 1px solid #1e2230 !important;
    color: #8b93a7 !important; border-radius: 8px !important; font-size: 13px !important;
    font-weight: 500 !important; padding: 9px 16px !important;
    text-align: left !important; width: 100% !important; transition: all 0.15s ease !important;
}
[data-testid="stSidebar"] .stButton > button:hover { background: #1e2230 !important; color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stButton > button[kind="primary"] { background: #1e2230 !important; border-color: #4f8ef7 !important; color: #e2e8f0 !important; }

.topbar {
    background: #0d0f14; border-bottom: 1px solid #1e2230;
    padding: 0 32px; height: 56px; display: flex; align-items: center;
    gap: 16px; position: sticky; top: 0; z-index: 999;
}
.topbar-logo { display: flex; align-items: center; gap: 10px; }
.topbar-logo-icon {
    width: 30px; height: 30px;
    background: linear-gradient(135deg, #4f8ef7, #7c5cfc);
    border-radius: 8px; display: flex; align-items: center;
    justify-content: center; font-size: 15px;
}
.topbar-logo-name { font-size: 16px; font-weight: 700; color: #e2e8f0; letter-spacing: -0.3px; }
.topbar-logo-sub  { font-size: 11px; color: #4b5568; margin-top: 1px; }
.topbar-spacer    { flex: 1; }
.topbar-badge {
    display: flex; align-items: center; gap: 6px;
    font-size: 12px; font-weight: 500; padding: 4px 12px;
    border-radius: 20px; border: 1px solid;
}
.topbar-badge.ok  { color: #34d399; border-color: #064e3b; background: #022c22; }
.topbar-badge.err { color: #f87171; border-color: #7f1d1d; background: #1f0808; }
.topbar-dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }

.page-wrap { padding: 28px 36px; }

.sec-title {
    font-size: 11px; font-weight: 600; color: #4b5568;
    text-transform: uppercase; letter-spacing: 1px;
    margin-bottom: 16px; margin-top: 28px;
}
.sec-title:first-child { margin-top: 0; }

.card { background: #131620; border: 1px solid #1e2230; border-radius: 12px; padding: 20px 24px; margin-bottom: 16px; }
.card-head {
    font-size: 13px; font-weight: 600; color: #94a3b8;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #1e2230;
}

.stat-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
.stat { background: #131620; border: 1px solid #1e2230; border-radius: 10px; padding: 14px 18px; }
.stat-label { font-size: 11px; font-weight: 600; color: #4b5568; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 8px; }
.stat-value { font-size: 20px; font-weight: 700; color: #e2e8f0; font-family: 'DM Mono', monospace; }
.stat.accent { border-top: 2px solid #4f8ef7; }
.stat.green  { border-top: 2px solid #34d399; }
.stat.amber  { border-top: 2px solid #fbbf24; }
.stat.red    { border-top: 2px solid #f87171; }

.alert { padding: 11px 16px; border-radius: 8px; font-size: 13px; margin-bottom: 8px; display: flex; align-items: flex-start; gap: 8px; }
.alert-err  { background: #1a0a0a; border: 1px solid #7f1d1d; color: #fca5a5; }
.alert-warn { background: #1a1200; border: 1px solid #78350f; color: #fcd34d; }
.alert-ok   { background: #021a0e; border: 1px solid #064e3b; color: #6ee7b7; }

.field-row { display: flex; align-items: flex-start; padding: 10px 0; border-bottom: 1px solid #1a1d2a; gap: 16px; }
.field-row:last-child { border-bottom: none; }
.field-key { width: 240px; min-width: 240px; font-size: 12px; font-weight: 600; color: #64748b; font-family: 'DM Mono', monospace; padding-top: 2px; }
.field-val { font-size: 13px; color: #cbd5e1; word-break: break-word; flex: 1; }
.field-null { color: #2d3449; font-style: italic; }

.log-line { font-family: 'DM Mono', monospace; font-size: 12px; color: #64748b; padding: 3px 0; border-bottom: 1px solid #131620; }
.log-line.ok   { color: #34d399; }
.log-line.info { color: #4f8ef7; }
.log-line.warn { color: #fbbf24; }

.hist-row {
    display: grid; grid-template-columns: 100px 80px 1fr 180px 120px 110px;
    gap: 12px; padding: 12px 16px; border-bottom: 1px solid #1a1d2a;
    align-items: center;
}
.hist-head {
    display: grid; grid-template-columns: 100px 80px 1fr 180px 120px 110px;
    gap: 12px; padding: 8px 16px;
    font-size: 11px; font-weight: 600; color: #4b5568;
    text-transform: uppercase; letter-spacing: 0.6px; border-bottom: 1px solid #1e2230;
}
.hist-cell { font-size: 13px; color: #94a3b8; }
.hist-cell.mono { font-family: 'DM Mono', monospace; font-size: 12px; }
.badge-clean   { background: #022c22; color: #34d399; border: 1px solid #064e3b; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
.badge-flagged { background: #1a0a0a; color: #fca5a5; border: 1px solid #7f1d1d; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }

.stButton > button { border-radius: 8px !important; font-size: 13px !important; font-weight: 600 !important; font-family: 'DM Sans', sans-serif !important; padding: 9px 20px !important; transition: all 0.15s ease !important; }
.stButton > button[kind="primary"] { background: linear-gradient(135deg, #4f8ef7, #7c5cfc) !important; border: none !important; color: #fff !important; }
.stButton > button[kind="primary"]:hover { opacity: 0.9 !important; transform: translateY(-1px) !important; }
.stButton > button:not([kind="primary"]) { background: #131620 !important; border: 1px solid #2d3449 !important; color: #94a3b8 !important; }

.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #0d0f14 !important; border: 1px solid #2d3449 !important;
    border-radius: 8px !important; color: #e2e8f0 !important;
    font-size: 13px !important; font-family: 'DM Sans', sans-serif !important;
}
.stTextInput label, .stTextArea label { color: #64748b !important; font-size: 12px !important; font-weight: 600 !important; }

[data-testid="stFileUploader"] { background: #0d0f14 !important; border: 2px dashed #2d3449 !important; border-radius: 12px !important; }

.success-banner { background: linear-gradient(135deg, #021a0e, #022c22); border: 1px solid #064e3b; border-radius: 12px; padding: 20px 24px; margin-bottom: 16px; }
.success-banner h3 { color: #34d399; margin: 0 0 6px 0; font-size: 16px; }
.success-banner p  { color: #6ee7b7; margin: 0; font-size: 13px; }

hr { border-color: #1e2230 !important; }
</style>
""", unsafe_allow_html=True)


# ── MongoDB ───────────────────────────────────────────────────────────────────

@st.cache_resource
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client[DB_NAME], True
    except Exception:
        return None, False

_mongo, _db_ok = get_db()


def save_to_mongo(doc_id: str, filename: str, status: str, data: dict, audit: list):
    if not _db_ok or _mongo is None:
        return False
    try:
        _mongo.invoices.update_one(
            {"doc_id": doc_id},
            {"$set": {
                "doc_id":    doc_id,
                "filename":  filename,
                "status":    status,
                "data":      data,
                "audit":     audit,
                "saved_at":  datetime.datetime.utcnow().isoformat(),
            }},
            upsert=True,
        )
        return True
    except Exception as exc:
        st.error(f"MongoDB error: {exc}")
        return False


def get_all_docs():
    if not _db_ok or _mongo is None:
        return []
    try:
        return list(_mongo.invoices.find({}, {"_id": 0}).sort("saved_at", -1))
    except Exception:
        return []


# ── Session state ─────────────────────────────────────────────────────────────

_DEFAULTS = {
    "view":      "history",
    "l1_blocks": None,
    "l2_data":   None,
    "edited":    None,
    "errors":    [],
    "warnings":  [],
    "status":    None,
    "stored":    False,
    "doc_id":    None,
    "filename":  None,
    "audit":     [],
    "edit_mode": False,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_amount(val):
    if val is None: return None
    s = re.sub(r"[^0-9.\-]", "", str(val).replace(",", ""))
    try: return float(s)
    except ValueError: return None


def parse_date(val):
    if val is None: return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try: return datetime.datetime.strptime(str(val).strip(), fmt).date()
        except ValueError: continue
    return None


def find_val(data, *keys):
    if isinstance(data, dict):
        for k, v in data.items():
            k_norm = k.lower().replace("_","").replace("-","").replace(" ","")
            for key in keys:
                if k_norm == key.lower().replace("_","").replace("-","").replace(" ",""): return v
        for v in data.values():
            r = find_val(v, *keys)
            if r is not None: return r
    elif isinstance(data, list):
        for item in data:
            r = find_val(item, *keys)
            if r is not None: return r
    return None


def flatten(obj, prefix=""):
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            full = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict): out.update(flatten(v, full))
            elif isinstance(v, list): out[full] = f"[{len(v)} items]"
            else: out[full] = v
    return out


def validate(data):
    errors, warnings = [], []
    inv_num  = find_val(data, "invoice_number","invoicenumber","invno","invoice_no")
    inv_date = find_val(data, "invoice_date","issuedate","date","issue_date")
    total    = find_val(data, "total_amount","totalamount","grandtotal","total","amount_due","final_amount")
    vendor   = find_val(data, "vendor_name","vendor","supplier","seller")
    if isinstance(vendor, dict): vendor = vendor.get("name")

    if not inv_num:  errors.append("Invoice Number is missing")
    if not inv_date: errors.append("Invoice Date is missing")
    if not total:    errors.append("Total Amount is missing")
    if not vendor:   errors.append("Vendor Name is missing")

    total_f = parse_amount(total)
    if total_f is not None and total_f <= 0:
        errors.append(f"Total amount ({total}) must be positive")

    d_inv = parse_date(inv_date)
    d_due = parse_date(find_val(data, "due_date","duedate","payment_due_date","paymentdue"))

    if inv_date and d_inv is None:
        warnings.append(f"Invoice date '{inv_date}' format not recognized")
    if d_inv and d_due and d_due < d_inv:
        errors.append("Due date is before invoice date")
    if d_inv and d_inv > datetime.date.today():
        warnings.append("Invoice date is in the future")

    return errors, warnings


def log_audit(action, detail=""):
    st.session_state.audit.append({
        "time":   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "detail": detail,
    })


def switch(view):
    st.session_state.view = view
    st.rerun()


# ── API calls ─────────────────────────────────────────────────────────────────

def api_extract(pdf_bytes: bytes, filename: str) -> list[dict]:
    resp = requests.post(
        f"{API_BASE}/extract",
        files={"file": (filename, pdf_bytes, "application/pdf")},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["blocks"]


def api_clean(blocks: list[dict], doc_id: str) -> dict:
    resp = requests.post(
        f"{API_BASE}/clean",
        json={"blocks": blocks, "doc_id": doc_id},
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json()["structured"]


# ── Topbar ────────────────────────────────────────────────────────────────────

db_cls = "ok"  if _db_ok else "err"
db_txt = "Atlas Connected" if _db_ok else "Atlas Offline"

st.markdown(f"""
<div class="topbar">
  <div class="topbar-logo">
    <div class="topbar-logo-icon">🧾</div>
    <div>
      <div class="topbar-logo-name">DocParse</div>
      <div class="topbar-logo-sub">Invoice Intelligence Pipeline</div>
    </div>
  </div>
  <div class="topbar-spacer"></div>
  <div class="topbar-badge {db_cls}"><div class="topbar-dot"></div>{db_txt}</div>
</div>
<div class="page-wrap">
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<span style="font-size:11px;font-weight:600;color:#4b5568;text-transform:uppercase;letter-spacing:1px;">Navigation</span>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("📂  Parsed Documents", use_container_width=True,
                 type="primary" if st.session_state.view == "history" else "secondary"):
        switch("history")
    if st.button("⬆️  Upload & Process", use_container_width=True,
                 type="primary" if st.session_state.view == "upload" else "secondary"):
        switch("upload")
    if st.session_state.l2_data is not None:
        if st.button("🔍  Review / Edit", use_container_width=True,
                     type="primary" if st.session_state.view == "review" else "secondary"):
            switch("review")

    st.markdown("---")
    st.markdown('<span style="font-size:11px;font-weight:600;color:#4b5568;text-transform:uppercase;letter-spacing:1px;">Pipeline</span>', unsafe_allow_html=True)
    st.markdown("""
    <div style="margin-top:12px;">
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #1e2230;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">1</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">POST /extract</div><div style="font-size:11px;color:#4b5568;">pdfplumber + PyMuPDF</div></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #1e2230;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">2</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">POST /clean</div><div style="font-size:11px;color:#4b5568;">Qwen3-32B · Bedrock</div></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">3</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">Review & Store</div><div style="font-size:11px;color:#4b5568;">Validate · MongoDB</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.caption(f"API: {API_BASE}")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — HISTORY
# ══════════════════════════════════════════════════════════════════════════════

if st.session_state.view == "history":
    st.markdown('<div class="sec-title">Parsed Documents</div>', unsafe_allow_html=True)

    if not _db_ok:
        st.markdown('<div class="alert alert-warn">⚠️ MongoDB is not connected. Set MONGO_URI in .env</div>', unsafe_allow_html=True)
    else:
        docs = get_all_docs()
        if not docs:
            st.markdown('<div class="card" style="text-align:center;padding:48px 24px;"><div style="font-size:32px;margin-bottom:12px;">📭</div><div style="color:#4b5568;font-size:14px;">No documents parsed yet.</div></div>', unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="card" style="padding:0;overflow:hidden;">
              <div class="hist-head">
                <div>Doc ID</div><div>Status</div><div>File</div>
                <div>Vendor</div><div>Total</div><div>Saved</div>
              </div>
            """, unsafe_allow_html=True)

            for doc in docs:
                d      = doc.get("data", {})
                did    = doc.get("doc_id", "—")
                status = doc.get("status", "—").lower()
                fname  = doc.get("filename", "—")
                vendor = find_val(d, "vendor_name","vendor","supplier") or "—"
                if isinstance(vendor, dict): vendor = vendor.get("name","—")
                total  = str(find_val(d, "total_amount","total","amount_due","final_amount") or "—")
                saved  = doc.get("saved_at","—")[:10]
                badge  = '<span class="badge-clean">CLEAN</span>' if status=="clean" else '<span class="badge-flagged">FLAGGED</span>'

                st.markdown(f"""
                <div class="hist-row">
                  <div class="hist-cell mono">{did}</div>
                  <div class="hist-cell">{badge}</div>
                  <div class="hist-cell">{fname}</div>
                  <div class="hist-cell">{vendor}</div>
                  <div class="hist-cell mono">{total}</div>
                  <div class="hist-cell mono">{saved}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="sec-title" style="margin-top:24px;">View Document Details</div>', unsafe_allow_html=True)
            sel = st.selectbox("Select Document ID", [d.get("doc_id") for d in docs], label_visibility="collapsed")
            if sel:
                doc = next((d for d in docs if d.get("doc_id") == sel), None)
                if doc:
                    d = doc.get("data", {})
                    st.markdown(f'<div class="card"><div class="card-head">📄 {sel} — {doc.get("status","").upper()} — {doc.get("filename","")}</div>', unsafe_allow_html=True)
                    for key, val in d.items():
                        if isinstance(val, list) and val:
                            st.markdown(f'<div style="margin:12px 0 6px;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                            if isinstance(val[0], dict):
                                st.dataframe(val, use_container_width=True, hide_index=True)
                            else:
                                for item in val:
                                    st.markdown(f'<div style="font-size:13px;color:#94a3b8;padding:2px 0;">• {item}</div>', unsafe_allow_html=True)
                            continue
                        if isinstance(val, dict):
                            st.markdown(f'<div style="margin:12px 0 4px;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                            for k2, v2 in flatten(val).items():
                                st.markdown(f'<div class="field-row"><div class="field-key">{k2.replace("."," › ").replace("_"," ").title()}</div><div class="field-val">{v2 if v2 is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
                            continue
                        st.markdown(f'<div class="field-row"><div class="field-key">{key.replace("_"," ").title()}</div><div class="field-val">{val if val is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)
                    with st.expander("{ } Raw JSON"):
                        st.json(d)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — UPLOAD
# ══════════════════════════════════════════════════════════════════════════════

elif st.session_state.view == "upload":
    st.markdown('<div class="sec-title">Upload & Process PDF</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    pdf_file = st.file_uploader("Drop a PDF invoice here, or click to browse", type=["pdf"])
    st.markdown("</div>", unsafe_allow_html=True)

    if pdf_file:
        if st.button("▶  Run Full Pipeline", type="primary", use_container_width=True):

            logs     = []
            log_area = st.empty()

            def add_log(msg, kind="info"):
                logs.append((msg, kind))
                html = '<div class="card" style="font-family:\'DM Mono\',monospace;padding:16px 20px;">'
                for m, k in logs:
                    html += f'<div class="log-line {k}">{m}</div>'
                html += "</div>"
                log_area.markdown(html, unsafe_allow_html=True)

            doc_id   = str(uuid.uuid4())[:8].upper()
            pdf_bytes = pdf_file.read()

            add_log(f"▶  doc_id: {doc_id}  |  file: {pdf_file.name}", "info")

            # ── Layer 1 — /extract ────────────────────────────────────────────
            add_log("━━  LAYER 1 — POST /extract  ━━━━━━━━━━━━━━━━━━━━━━━━", "info")
            add_log("⏳  Sending PDF to API ...", "info")
            try:
                blocks = api_extract(pdf_bytes, pdf_file.name)
                plumber_n = sum(1 for b in blocks if b["source"] == "pdfplumber")
                pymupdf_n = sum(1 for b in blocks if b["source"] == "pymupdf")
                flagged_n = sum(1 for b in blocks if b["overlap"])
                pages_n   = len(set(b["page"] for b in blocks))
                add_log(f"✓  {len(blocks)} blocks  |  {pages_n} pages  |  pdfplumber: {plumber_n}  pymupdf: {pymupdf_n}  flagged: {flagged_n}", "ok")
                st.session_state.l1_blocks = blocks
            except Exception as exc:
                add_log(f"✗  /extract failed: {exc}", "warn")
                st.stop()

            # ── Layer 2 — /clean ─────────────────────────────────────────────
            add_log("━━  LAYER 2 — POST /clean  ━━━━━━━━━━━━━━━━━━━━━━━━━━", "info")
            add_log("⏳  Sending blocks to LLM (Qwen3-32B) ...", "info")
            try:
                structured = api_clean(blocks, doc_id)
                add_log(f"✓  {len(structured)} fields extracted", "ok")
                st.session_state.l2_data  = structured
                st.session_state.edited   = json.loads(json.dumps(structured))
                errors, warnings          = validate(structured)
                st.session_state.errors   = errors
                st.session_state.warnings = warnings
                st.session_state.status   = "flagged" if errors else "clean"
                st.session_state.doc_id   = doc_id
                st.session_state.filename = pdf_file.name
                st.session_state.stored   = False
                st.session_state.edit_mode = False
                st.session_state.audit    = []
                log_audit("PIPELINE_RUN", f"file: {pdf_file.name} | blocks: {len(blocks)} | fields: {len(structured)}")
            except Exception as exc:
                add_log(f"✗  /clean failed: {exc}", "warn")
                st.stop()

            add_log("━━  LAYER 3 — Validation complete  ━━━━━━━━━━━━━━━━━━", "info")
            add_log(f"✓  Status: {'FLAGGED' if errors else 'CLEAN'}  |  Errors: {len(errors)}  Warnings: {len(warnings)}", "ok" if not errors else "warn")
            add_log("↪  Redirecting to review ...", "info")

            import time; time.sleep(0.6)
            switch("review")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — REVIEW
# ══════════════════════════════════════════════════════════════════════════════

elif st.session_state.view == "review":
    if st.session_state.l2_data is None:
        st.markdown('<div class="alert alert-warn">⚠️ No document loaded. Upload a PDF first.</div>', unsafe_allow_html=True)
        if st.button("⬆️ Go to Upload", type="primary"):
            switch("upload")
    else:
        data     = st.session_state.edited
        errors   = st.session_state.errors
        warnings = st.session_state.warnings
        status   = st.session_state.status

        # ── Stats ─────────────────────────────────────────────────────────────
        st.markdown(f"""
        <div class="stat-row">
          <div class="stat {'red' if errors else 'green'}">
            <div class="stat-label">Status</div>
            <div class="stat-value">{'FLAGGED' if errors else 'CLEAN'}</div>
          </div>
          <div class="stat accent">
            <div class="stat-label">Document ID</div>
            <div class="stat-value" style="font-size:16px;">{st.session_state.doc_id}</div>
          </div>
          <div class="stat accent">
            <div class="stat-label">Fields</div>
            <div class="stat-value">{len(flatten(data))}</div>
          </div>
          <div class="stat {'red' if errors else 'green'}">
            <div class="stat-label">Issues</div>
            <div class="stat-value" style="font-size:16px;">{len(errors)}E · {len(warnings)}W</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        for e in errors:
            st.markdown(f'<div class="alert alert-err">❌ {e}</div>', unsafe_allow_html=True)
        for w in warnings:
            st.markdown(f'<div class="alert alert-warn">⚠️ {w}</div>', unsafe_allow_html=True)
        if not errors and not warnings:
            st.markdown('<div class="alert alert-ok">✅ All validation checks passed.</div>', unsafe_allow_html=True)

        st.markdown("---")

        # ── Edit toggle ───────────────────────────────────────────────────────
        if not st.session_state.stored:
            _, c_right = st.columns([6, 1])
            with c_right:
                if st.button("✏️ Edit" if not st.session_state.edit_mode else "👁 View"):
                    st.session_state.edit_mode = not st.session_state.edit_mode
                    st.rerun()

        # ── Edit mode ─────────────────────────────────────────────────────────
        if st.session_state.edit_mode and not st.session_state.stored:
            st.markdown('<div class="sec-title">Edit Fields</div>', unsafe_allow_html=True)
            st.markdown('<div class="card">', unsafe_allow_html=True)
            flat        = flatten(data)
            edited_vals = {}
            cols        = st.columns(2)
            for i, (k, v) in enumerate(flat.items()):
                if "[" in str(v): continue
                edited_vals[k] = cols[i % 2].text_input(
                    k.replace(".", " › ").replace("_", " ").title(),
                    value=str(v) if v is not None else "",
                    key=f"ef_{k}",
                )
            st.markdown("</div>", unsafe_allow_html=True)

            if st.button("💾 Apply Edits", type="primary", use_container_width=True):
                def set_nested(d, keys, val):
                    for key in keys[:-1]: d = d.setdefault(key, {})
                    d[keys[-1]] = val

                rebuilt = json.loads(json.dumps(data))
                for dotkey, val in edited_vals.items():
                    try: set_nested(rebuilt, dotkey.split("."), val)
                    except Exception: pass

                st.session_state.edited   = rebuilt
                e2, w2                    = validate(rebuilt)
                st.session_state.errors   = e2
                st.session_state.warnings = w2
                st.session_state.status   = "flagged" if e2 else "clean"
                st.session_state.edit_mode = False
                log_audit("EDITED", f"Fields: {list(edited_vals.keys())}")
                st.rerun()

        # ── View mode ─────────────────────────────────────────────────────────
        else:
            st.markdown('<div class="sec-title">Extracted Fields</div>', unsafe_allow_html=True)
            st.markdown('<div class="card">', unsafe_allow_html=True)
            for key, val in data.items():
                if isinstance(val, list) and val:
                    st.markdown(f'<div class="card-head">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                    if isinstance(val[0], dict):
                        st.dataframe(val, use_container_width=True, hide_index=True)
                    else:
                        for item in val:
                            st.markdown(f'<div style="font-size:13px;color:#94a3b8;padding:3px 0;">• {item}</div>', unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)
                    continue
                if isinstance(val, dict):
                    st.markdown(f'<div class="card-head">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                    for k2, v2 in flatten(val).items():
                        st.markdown(f'<div class="field-row"><div class="field-key">{k2.replace("."," › ").replace("_"," ").title()}</div><div class="field-val">{v2 if v2 is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)
                    continue
                st.markdown(f'<div class="field-row"><div class="field-key">{key.replace("_"," ").title()}</div><div class="field-val">{val if val is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            with st.expander("{ } Raw JSON"):
                st.json(data)

        st.markdown("---")

        # ── Actions ───────────────────────────────────────────────────────────
        if st.session_state.stored:
            st.markdown(f"""
            <div class="success-banner">
                <h3>✅ Document stored successfully</h3>
                <p>ID: <strong>{st.session_state.doc_id}</strong> &nbsp;·&nbsp; Collection: <code>invoices</code>
                {"" if _db_ok else " <em>(MongoDB offline — not persisted)</em>"}</p>
            </div>
            """, unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            with c1:
                if st.button("📂 Process Another Document", use_container_width=True):
                    for k, v in _DEFAULTS.items(): st.session_state[k] = v
                    switch("upload")
            with c2:
                st.download_button(
                    "⬇️ Export JSON",
                    data=json.dumps({"doc_id": st.session_state.doc_id, "status": status, "data": data, "audit": st.session_state.audit}, indent=2, ensure_ascii=False),
                    file_name=f"invoice_{st.session_state.doc_id}.json",
                    mime="application/json",
                    use_container_width=True,
                )
        else:
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Approve & Store" if status=="clean" else "✅ Override & Store", type="primary", use_container_width=True):
                    log_audit("APPROVED", f"status: {status}")
                    ok = save_to_mongo(st.session_state.doc_id, st.session_state.filename, status, data, st.session_state.audit)
                    if not ok and _db_ok:
                        st.error("Failed to write to MongoDB.")
                    else:
                        st.session_state.stored = True
                        st.rerun()
            with c2:
                if st.button("🔄 Reject & Discard", use_container_width=True):
                    log_audit("REJECTED", f"errors: {errors}")
                    st.markdown('<div class="alert alert-warn">⚠️ Document rejected and discarded.</div>', unsafe_allow_html=True)

# ── Close page-wrap ───────────────────────────────────────────────────────────
st.markdown("</div>", unsafe_allow_html=True)