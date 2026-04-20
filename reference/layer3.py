"""
Layer 3 — Validation, Human Review and Storage
Streamlit UI — clean single-flow interface.

Changes:
  - MongoDB Atlas via MONGO_URI in .env
  - Single flow: History → Upload → Layer 1 progress → Layer 2 progress → Review/Edit → Approve
  - Professional navbar + topbar
  - All original validation logic preserved exactly
"""

import json
import re
import uuid
import datetime
import tempfile
from pathlib import Path
import streamlit as st

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

/* ── Reset ── */
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"] { display: none !important; }

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
}

.stApp { background: #0d0f14; }

.main .block-container {
    padding: 0 !important;
    max-width: 100% !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #0d0f14 !important;
    border-right: 1px solid #1e2230 !important;
}
[data-testid="stSidebar"] * { color: #8b93a7 !important; }
[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    border: 1px solid #1e2230 !important;
    color: #8b93a7 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 9px 16px !important;
    text-align: left !important;
    width: 100% !important;
    transition: all 0.15s ease !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: #1e2230 !important;
    color: #e2e8f0 !important;
    border-color: #2d3449 !important;
}
[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background: #1e2230 !important;
    border-color: #4f8ef7 !important;
    color: #e2e8f0 !important;
}

/* ── Topbar ── */
.topbar {
    background: #0d0f14;
    border-bottom: 1px solid #1e2230;
    padding: 0 32px;
    height: 56px;
    display: flex;
    align-items: center;
    gap: 16px;
    position: sticky;
    top: 0;
    z-index: 999;
}
.topbar-logo {
    display: flex;
    align-items: center;
    gap: 10px;
}
.topbar-logo-icon {
    width: 30px;
    height: 30px;
    background: linear-gradient(135deg, #4f8ef7, #7c5cfc);
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 15px;
}
.topbar-logo-name {
    font-size: 16px;
    font-weight: 700;
    color: #e2e8f0;
    letter-spacing: -0.3px;
}
.topbar-logo-sub {
    font-size: 11px;
    color: #4b5568;
    margin-top: 1px;
}
.topbar-spacer { flex: 1; }
.topbar-badge {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    font-weight: 500;
    padding: 4px 12px;
    border-radius: 20px;
    border: 1px solid;
}
.topbar-badge.ok {
    color: #34d399;
    border-color: #064e3b;
    background: #022c22;
}
.topbar-badge.err {
    color: #f87171;
    border-color: #7f1d1d;
    background: #1f0808;
}
.topbar-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: currentColor;
}

/* ── Page wrapper ── */
.page-wrap {
    padding: 28px 36px;
}

/* ── Section title ── */
.sec-title {
    font-size: 11px;
    font-weight: 600;
    color: #4b5568;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 16px;
    margin-top: 28px;
}
.sec-title:first-child { margin-top: 0; }

/* ── Cards ── */
.card {
    background: #131620;
    border: 1px solid #1e2230;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 16px;
}
.card-head {
    font-size: 13px;
    font-weight: 600;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid #1e2230;
}

/* ── Stat row ── */
.stat-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 20px;
}
.stat {
    background: #131620;
    border: 1px solid #1e2230;
    border-radius: 10px;
    padding: 14px 18px;
}
.stat-label {
    font-size: 11px;
    font-weight: 600;
    color: #4b5568;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 8px;
}
.stat-value {
    font-size: 20px;
    font-weight: 700;
    color: #e2e8f0;
    font-family: 'DM Mono', monospace;
}
.stat.accent { border-top: 2px solid #4f8ef7; }
.stat.green  { border-top: 2px solid #34d399; }
.stat.amber  { border-top: 2px solid #fbbf24; }
.stat.red    { border-top: 2px solid #f87171; }

/* ── Alerts ── */
.alert {
    padding: 11px 16px;
    border-radius: 8px;
    font-size: 13px;
    margin-bottom: 8px;
    display: flex;
    align-items: flex-start;
    gap: 8px;
}
.alert-err  { background: #1a0a0a; border: 1px solid #7f1d1d; color: #fca5a5; }
.alert-warn { background: #1a1200; border: 1px solid #78350f; color: #fcd34d; }
.alert-ok   { background: #021a0e; border: 1px solid #064e3b; color: #6ee7b7; }

/* ── Field table ── */
.field-row {
    display: flex;
    align-items: flex-start;
    padding: 10px 0;
    border-bottom: 1px solid #1a1d2a;
    gap: 16px;
}
.field-row:last-child { border-bottom: none; }
.field-key {
    width: 240px;
    min-width: 240px;
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    font-family: 'DM Mono', monospace;
    padding-top: 2px;
}
.field-val {
    font-size: 13px;
    color: #cbd5e1;
    word-break: break-word;
    flex: 1;
}
.field-null { color: #2d3449; font-style: italic; }

/* ── Progress steps ── */
.progress-wrap {
    display: flex;
    align-items: center;
    gap: 0;
    margin-bottom: 24px;
}
.prog-step {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    flex: 1;
    position: relative;
}
.prog-step::after {
    content: '';
    position: absolute;
    top: 14px;
    left: calc(50% + 14px);
    width: calc(100% - 28px);
    height: 2px;
    background: #1e2230;
}
.prog-step:last-child::after { display: none; }
.prog-step.done::after  { background: #34d399; }
.prog-circle {
    width: 28px; height: 28px;
    border-radius: 50%;
    background: #1e2230;
    border: 2px solid #2d3449;
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700; color: #4b5568;
    position: relative; z-index: 1;
}
.prog-circle.done  { background: #064e3b; border-color: #34d399; color: #34d399; }
.prog-circle.active { background: #1e3a5f; border-color: #4f8ef7; color: #4f8ef7; }
.prog-label {
    font-size: 11px; font-weight: 500; color: #4b5568;
    text-align: center;
}
.prog-label.done   { color: #34d399; }
.prog-label.active { color: #4f8ef7; }

/* ── Pipeline log ── */
.log-line {
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #64748b;
    padding: 3px 0;
    border-bottom: 1px solid #131620;
}
.log-line.ok   { color: #34d399; }
.log-line.info { color: #4f8ef7; }
.log-line.warn { color: #fbbf24; }

/* ── History table ── */
.hist-row {
    display: grid;
    grid-template-columns: 100px 80px 1fr 180px 120px 110px;
    gap: 12px;
    padding: 12px 16px;
    border-bottom: 1px solid #1a1d2a;
    align-items: center;
    cursor: pointer;
    transition: background 0.1s;
}
.hist-row:hover { background: #1a1d2a; border-radius: 6px; }
.hist-head {
    display: grid;
    grid-template-columns: 100px 80px 1fr 180px 120px 110px;
    gap: 12px;
    padding: 8px 16px;
    font-size: 11px; font-weight: 600; color: #4b5568;
    text-transform: uppercase; letter-spacing: 0.6px;
    border-bottom: 1px solid #1e2230;
}
.hist-cell { font-size: 13px; color: #94a3b8; font-family: 'DM Mono', monospace; }
.hist-cell.mono { font-family: 'DM Mono', monospace; font-size: 12px; }
.badge-clean   { background: #022c22; color: #34d399; border: 1px solid #064e3b; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
.badge-flagged { background: #1a0a0a; color: #fca5a5; border: 1px solid #7f1d1d; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }

/* ── Buttons ── */
.stButton > button {
    border-radius: 8px !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    font-family: 'DM Sans', sans-serif !important;
    padding: 9px 20px !important;
    transition: all 0.15s ease !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #4f8ef7, #7c5cfc) !important;
    border: none !important;
    color: #fff !important;
}
.stButton > button[kind="primary"]:hover {
    opacity: 0.9 !important;
    transform: translateY(-1px) !important;
}
.stButton > button:not([kind="primary"]) {
    background: #131620 !important;
    border: 1px solid #2d3449 !important;
    color: #94a3b8 !important;
}
.stButton > button:not([kind="primary"]):hover {
    background: #1e2230 !important;
    color: #e2e8f0 !important;
}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #0d0f14 !important;
    border: 1px solid #2d3449 !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
    font-size: 13px !important;
    font-family: 'DM Sans', sans-serif !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #4f8ef7 !important;
    box-shadow: 0 0 0 3px rgba(79,142,247,.12) !important;
}
.stTextInput label, .stTextArea label {
    color: #64748b !important;
    font-size: 12px !important;
    font-weight: 600 !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    background: #0d0f14 !important;
    border: 2px dashed #2d3449 !important;
    border-radius: 12px !important;
}

/* ── Expander ── */
.streamlit-expanderHeader {
    background: #131620 !important;
    border: 1px solid #1e2230 !important;
    border-radius: 8px !important;
    color: #94a3b8 !important;
    font-size: 13px !important;
    font-weight: 600 !important;
}

/* ── Dataframe ── */
.stDataFrame { border-radius: 8px !important; overflow: hidden !important; }
[data-testid="stDataFrameResizable"] { background: #131620 !important; }

/* ── Divider ── */
hr { border-color: #1e2230 !important; }

/* ── Success banner ── */
.success-banner {
    background: linear-gradient(135deg, #021a0e, #022c22);
    border: 1px solid #064e3b;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 16px;
}
.success-banner h3 { color: #34d399; margin: 0 0 6px 0; font-size: 16px; }
.success-banner p  { color: #6ee7b7; margin: 0; font-size: 13px; }

/* ── Spinner override ── */
.stSpinner > div { border-top-color: #4f8ef7 !important; }
</style>
""", unsafe_allow_html=True)


# ── MongoDB Atlas (non-fatal) ─────────────────────────────────────────────────

_db_ok = False
try:
    from db_setup import setup_collections, health_check, save_invoice, \
        save_bank_details, save_audit_entry, save_raw_extraction, get_all_invoices
    setup_collections()
    _hc    = health_check()
    _db_ok = _hc["status"] == "ok"
except Exception as _db_err:
    _db_ok = False


# ── Session state ─────────────────────────────────────────────────────────────

_DEFAULTS = {
    "view":        "history",   # "history" | "upload" | "review"
    "data":        None,
    "edited":      None,
    "errors":      [],
    "warnings":    [],
    "status":      None,
    "stored":      False,
    "doc_id":      None,
    "audit":       [],
    "edit_mode":   False,
    "pipeline_log": [],
    "selected_doc": None,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Helpers (preserved exactly) ───────────────────────────────────────────────

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
                key_norm = key.lower().replace("_","").replace("-","").replace(" ","")
                if k_norm == key_norm: return v
        for v in data.values():
            result = find_val(v, *keys)
            if result is not None: return result
    elif isinstance(data, list):
        for item in data:
            result = find_val(item, *keys)
            if result is not None: return result
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
    inv_num  = find_val(data, "invoice_number","invoicenumber","invno","billnumber","invoice_no")
    inv_date = find_val(data, "invoice_date","issuedate","date","billingdate","issue_date")
    total    = find_val(data, "total_amount","totalamount","grandtotal","total","amount_due")
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
    d_due = parse_date(find_val(data, "due_date","duedate","paymentdue","payment_due"))

    if inv_date and d_inv is None:
        warnings.append(f"Invoice date '{inv_date}' format not recognized")
    if d_inv and d_due and d_due < d_inv:
        errors.append("Due date is before invoice date")
    if d_inv and d_inv > datetime.date.today():
        warnings.append("Invoice date is in the future")

    return errors, warnings


def load_data(raw_data):
    st.session_state.data      = raw_data
    st.session_state.edited    = json.loads(json.dumps(raw_data))
    errors, warnings           = validate(raw_data)
    st.session_state.errors    = errors
    st.session_state.warnings  = warnings
    st.session_state.status    = "flagged" if errors else "clean"
    st.session_state.stored    = False
    st.session_state.doc_id    = str(uuid.uuid4())[:8].upper()
    st.session_state.edit_mode = False


def store_to_mongo(doc_id, status, data, audit):
    if not _db_ok: return False
    try:
        save_invoice(doc_id, status, data)
        pm = find_val(data, "payment_methods","paymentmethods","bank_details")
        if isinstance(pm, list): save_bank_details(doc_id, pm)
        save_raw_extraction(doc_id, 3, data)
        for entry in audit:
            save_audit_entry(doc_id, entry["action"], entry.get("detail",""))
        return True
    except Exception as exc:
        st.error(f"MongoDB write error: {exc}")
        return False


def log_audit(action, detail=""):
    st.session_state.audit.append({
        "time":   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "detail": detail,
    })


def switch(view):
    st.session_state.view = view
    st.rerun()


# ── Topbar ────────────────────────────────────────────────────────────────────

db_cls  = "ok"  if _db_ok else "err"
db_txt  = "Atlas Connected" if _db_ok else "Atlas Offline"

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
    <div class="topbar-badge {db_cls}">
        <div class="topbar-dot"></div>
        {db_txt}
    </div>
</div>
<div class="page-wrap">
""", unsafe_allow_html=True)


# ── Sidebar nav ───────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<span style="font-size:11px;font-weight:600;color:#4b5568;text-transform:uppercase;letter-spacing:1px;">Navigation</span>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    hist_kind    = "primary" if st.session_state.view == "history" else "secondary"
    upload_kind  = "primary" if st.session_state.view == "upload"  else "secondary"
    review_kind  = "primary" if st.session_state.view == "review"  else "secondary"

    if st.button("📂  Parsed Documents", use_container_width=True, type=hist_kind):
        switch("history")
    if st.button("⬆️  Upload & Process",  use_container_width=True, type=upload_kind):
        switch("upload")
    if st.session_state.data is not None:
        if st.button("🔍  Review / Edit",   use_container_width=True, type=review_kind):
            switch("review")

    st.markdown("---")
    st.markdown('<span style="font-size:11px;font-weight:600;color:#4b5568;text-transform:uppercase;letter-spacing:1px;">Pipeline</span>', unsafe_allow_html=True)
    st.markdown("""
    <div style="margin-top:12px;">
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #1e2230;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">1</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">Extraction</div><div style="font-size:11px;color:#4b5568;">pdfplumber + PyMuPDF</div></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #1e2230;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">2</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">LLM Extraction</div><div style="font-size:11px;color:#4b5568;"></div></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;padding:8px 0;">
        <div style="width:22px;height:22px;border-radius:50%;background:#131620;border:1px solid #2d3449;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#4b5568;">3</div>
        <div><div style="font-size:12px;font-weight:600;color:#94a3b8;">Review & Store</div><div style="font-size:11px;color:#4b5568;">Validate · MongoDB Atlas</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.caption("DocParse v1.0 · Invoice Pipeline")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — HISTORY
# ══════════════════════════════════════════════════════════════════════════════

if st.session_state.view == "history":
    st.markdown('<div class="sec-title">Parsed Documents</div>', unsafe_allow_html=True)

    if not _db_ok:
        st.markdown('<div class="alert alert-warn">⚠️ MongoDB Atlas is not connected. Configure MONGO_URI in .env to view history.</div>', unsafe_allow_html=True)
    else:
        invoices = get_all_invoices()
        if not invoices:
            st.markdown('<div class="card" style="text-align:center;padding:48px 24px;"><div style="font-size:32px;margin-bottom:12px;">📭</div><div style="color:#4b5568;font-size:14px;">No documents parsed yet.</div><div style="color:#2d3449;font-size:12px;margin-top:6px;">Upload a PDF to get started.</div></div>', unsafe_allow_html=True)
        else:
            # Header
            st.markdown("""
            <div class="card" style="padding:0;overflow:hidden;">
              <div class="hist-head">
                <div>Doc ID</div><div>Status</div><div>Invoice #</div>
                <div>Vendor</div><div>Total</div><div>Saved</div>
              </div>
            """, unsafe_allow_html=True)

            for inv in invoices:
                d        = inv.get("data", {})
                doc_id   = inv.get("doc_id","—")
                status   = inv.get("status","—").lower()
                inv_num  = find_val(d, "invoice_number","invoicenumber") or "—"
                vendor   = find_val(d, "vendor_name","vendor","supplier") or "—"
                if isinstance(vendor, dict): vendor = vendor.get("name","—")
                total    = str(find_val(d, "total_amount","total") or "—")
                saved    = inv.get("saved_at","—")[:10]
                badge    = f'<span class="badge-clean">CLEAN</span>' if status=="clean" else f'<span class="badge-flagged">FLAGGED</span>'

                st.markdown(f"""
                <div class="hist-row" onclick="">
                  <div class="hist-cell mono">{doc_id}</div>
                  <div class="hist-cell">{badge}</div>
                  <div class="hist-cell">{inv_num}</div>
                  <div class="hist-cell">{vendor}</div>
                  <div class="hist-cell mono">{total}</div>
                  <div class="hist-cell mono">{saved}</div>
                </div>
                """, unsafe_allow_html=True)

                # Expandable detail — use streamlit expander per row
            st.markdown("</div>", unsafe_allow_html=True)

            # Click to expand a doc
            st.markdown('<div class="sec-title" style="margin-top:24px;">View Document Details</div>', unsafe_allow_html=True)
            doc_ids = [inv.get("doc_id","") for inv in invoices]
            sel = st.selectbox("Select a Document ID", options=doc_ids, label_visibility="collapsed")
            if sel:
                sel_inv = next((i for i in invoices if i.get("doc_id")==sel), None)
                if sel_inv:
                    d = sel_inv.get("data", {})
                    # st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown(f'<div class="card-head">📄 {sel} — {sel_inv.get("status","").upper()}</div>', unsafe_allow_html=True)
                    for key, val in d.items():
                        if isinstance(val, list) and val:
                            st.markdown(f'<div style="margin:12px 0 6px;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                            if val and isinstance(val[0], dict):
                                st.dataframe(val, use_container_width=True, hide_index=True)
                            else:
                                for item in val:
                                    st.markdown(f'<div style="font-size:13px;color:#94a3b8;padding:2px 0;">• {item}</div>', unsafe_allow_html=True)
                            continue
                        if isinstance(val, dict):
                            st.markdown(f'<div style="margin:12px 0 4px;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                            for k2, v2 in flatten(val).items():
                                lbl = k2.replace("."," › ").replace("_"," ").title()
                                st.markdown(f'<div class="field-row"><div class="field-key">{lbl}</div><div class="field-val">{str(v2) if v2 is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
                            continue
                        lbl = key.replace("_"," ").title()
                        st.markdown(f'<div class="field-row"><div class="field-key">{lbl}</div><div class="field-val">{str(val) if val is not None else "<span class=\'field-null\'>null</span>"}</div></div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — UPLOAD & PROCESS
# ══════════════════════════════════════════════════════════════════════════════

elif st.session_state.view == "upload":
    st.markdown('<div class="sec-title">Upload & Process PDF</div>', unsafe_allow_html=True)

    # Progress indicator
    step = 0
    if st.session_state.get("_step1_done"): step = 1
    if st.session_state.get("_step2_done"): step = 2

    def prog_cls(n):
        if n < step: return "done"
        if n == step: return "active"
        return ""

    st.markdown(f"""
    <div class="progress-wrap">
      <div class="prog-step {prog_cls(0) + (' done' if step>0 else '')}">
        <div class="prog-circle {('done' if step>0 else 'active')}">{'✓' if step>0 else '1'}</div>
        <div class="prog-label {'done' if step>0 else 'active'}">Layer 1<br>Extraction</div>
      </div>
      <div class="prog-step {'done' if step>1 else ''}">
        <div class="prog-circle {'done' if step>1 else ('active' if step==1 else '')}">{'✓' if step>1 else '2'}</div>
        <div class="prog-label {'done' if step>1 else ('active' if step==1 else '')}">Layer 2<br>LLM Parse</div>
      </div>
      <div class="prog-step">
        <div class="prog-circle {'active' if step==2 else ''}">3</div>
        <div class="prog-label {'active' if step==2 else ''}">Review<br>&amp; Store</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    pdf_file = st.file_uploader("Drop a PDF invoice here, or click to browse", type=["pdf"])
    st.markdown("</div>", unsafe_allow_html=True)

    if pdf_file:
        if st.button("▶  Run Full Pipeline", type="primary", use_container_width=True):
            logs = []

            def add_log(msg, kind="info"):
                logs.append((msg, kind))

            log_area = st.empty()

            def render_logs():
                html = '<div class="card" style="font-family:\'DM Mono\',monospace;padding:16px 20px;">'
                for m, k in logs:
                    html += f'<div class="log-line {k}">{m}</div>'
                html += "</div>"
                log_area.markdown(html, unsafe_allow_html=True)

            with tempfile.TemporaryDirectory() as tmp:
                pdf_path = Path(tmp) / "input.pdf"
                l1_path  = Path(tmp) / "layer1_output.json"
                l2_path  = Path(tmp) / "layer2_output.json"
                doc_id   = str(uuid.uuid4())[:8].upper()

                pdf_path.write_bytes(pdf_file.read())
                add_log(f"▶  Starting pipeline for {pdf_file.name}  [doc_id: {doc_id}]", "info")
                render_logs()

                # ── Layer 1 ──────────────────────────────────────────────────
                add_log("", "info")
                add_log("━━  LAYER 1 — PDF Extraction  ━━━━━━━━━━━━━━━━━━━━━", "info")
                add_log("⏳  Running pdfplumber ...", "info")
                render_logs()

                l1_ok = False
                l1_result = []
                try:
                    from layer1 import run_layer1
                    l1_result = run_layer1(str(pdf_path), doc_id=doc_id)
                    l1_path.write_text(json.dumps(l1_result, indent=2, ensure_ascii=False), encoding="utf-8")

                    plumber_n = sum(1 for b in l1_result if b["source"] == "pdfplumber")
                    pymupdf_n = sum(1 for b in l1_result if b["source"] == "pymupdf")
                    flagged_n = sum(1 for b in l1_result if b["overlap"])
                    pages_n   = len(set(b["page"] for b in l1_result))

                    add_log(f"✓  pdfplumber: {plumber_n} blocks extracted", "ok")
                    add_log(f"✓  PyMuPDF:    {pymupdf_n} blocks extracted", "ok")
                    add_log(f"✓  Pages:      {pages_n}  |  Flagged overlaps: {flagged_n}", "ok")
                    add_log(f"✓  Layer 1 complete — {len(l1_result)} total blocks", "ok")
                    l1_ok = True
                    st.session_state["_step1_done"] = True
                except Exception as exc:
                    add_log(f"✗  Layer 1 failed: {exc}", "warn")
                render_logs()

                if not l1_ok:
                    st.stop()

                # ── Layer 2 ──────────────────────────────────────────────────
                add_log("", "info")
                add_log("━━  LAYER 2 — LLM Extraction (Qwen3-32B)  ━━━━━━━━", "info")
                add_log("⏳  Resolving overlaps ...", "info")
                render_logs()

                l2_ok = False
                l2_result = {}
                try:
                    from layer2 import resolve_overlaps, assemble_pages, chunk_pages, \
                        build_chunk_prompt, call_llm, parse_output, merge_results

                    blocks = resolve_overlaps(l1_result)
                    kept   = sum(1 for b in blocks if not b["suppressed"])
                    sup    = sum(1 for b in blocks if b["suppressed"])
                    add_log(f"✓  Overlap resolution: kept {kept}, suppressed {sup}", "ok")
                    render_logs()

                    assembled = assemble_pages(blocks)
                    total_w   = sum(len(v["text"].split()) for v in assembled.values())
                    add_log(f"✓  Pages assembled: {len(assembled)} pages, {total_w} words", "ok")
                    render_logs()

                    chunks = chunk_pages(assembled, 4)
                    add_log(f"✓  Chunks: {len(chunks)} (≤4 pages each)", "ok")
                    render_logs()

                    chunk_results = []
                    for idx, page_nums in enumerate(chunks, 1):
                        add_log(f"⏳  Calling Qwen3-32B — chunk {idx}/{len(chunks)} (pages {page_nums}) ...", "info")
                        render_logs()
                        prompt  = build_chunk_prompt(assembled, page_nums)
                        raw_out = call_llm(prompt, f"chunk {idx}/{len(chunks)}")
                        parsed  = parse_output(raw_out, f"chunk {idx}")
                        add_log(f"✓  Chunk {idx} done — {len(parsed)} fields extracted", "ok")
                        render_logs()
                        chunk_results.append(parsed)

                    l2_result = merge_results(chunk_results)
                    l2_path.write_text(json.dumps(l2_result, indent=2, ensure_ascii=False), encoding="utf-8")

                    add_log(f"✓  Layer 2 complete — {len(l2_result)} unique fields extracted", "ok")
                    l2_ok = True
                    st.session_state["_step2_done"] = True
                except Exception as exc:
                    add_log(f"✗  Layer 2 failed: {exc}", "warn")
                render_logs()

                if not l2_ok:
                    st.stop()

                # ── Load into review ─────────────────────────────────────────
                add_log("", "info")
                add_log("━━  LAYER 3 — Validation & Review  ━━━━━━━━━━━━━━━━", "info")
                load_data(l2_result)
                st.session_state.doc_id = doc_id
                log_audit("PIPELINE_RUN", f"PDF: {pdf_file.name} | doc_id: {doc_id}")
                add_log(f"✓  Validation complete — redirecting to review ...", "ok")
                render_logs()

                import time; time.sleep(0.8)
                st.session_state["_step1_done"] = False
                st.session_state["_step2_done"] = False
                switch("review")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW — REVIEW
# ══════════════════════════════════════════════════════════════════════════════

elif st.session_state.view == "review":
    if st.session_state.data is None:
        st.markdown('<div class="alert alert-warn">⚠️ No document loaded. Upload a PDF first.</div>', unsafe_allow_html=True)
        if st.button("⬆️ Go to Upload", type="primary"):
            switch("upload")
    else:
        data     = st.session_state.edited
        errors   = st.session_state.errors
        warnings = st.session_state.warnings
        status   = st.session_state.status

        # ── Stat row ─────────────────────────────────────────────────────────
        err_color   = "red"   if errors   else "green"
        stat_status = "red"   if errors   else "green"

        st.markdown(f"""
        <div class="stat-row">
          <div class="stat {stat_status}">
            <div class="stat-label">Status</div>
            <div class="stat-value">{'FLAGGED' if errors else 'CLEAN'}</div>
          </div>
          <div class="stat accent">
            <div class="stat-label">Document ID</div>
            <div class="stat-value" style="font-size:16px;">{st.session_state.doc_id}</div>
          </div>
          <div class="stat accent">
            <div class="stat-label">Fields Extracted</div>
            <div class="stat-value">{len(flatten(data))}</div>
          </div>
          <div class="stat {err_color}">
            <div class="stat-label">Issues</div>
            <div class="stat-value" style="font-size:16px;">{len(errors)}E · {len(warnings)}W</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Alerts ───────────────────────────────────────────────────────────
        for e in errors:
            st.markdown(f'<div class="alert alert-err">❌ {e}</div>', unsafe_allow_html=True)
        for w in warnings:
            st.markdown(f'<div class="alert alert-warn">⚠️ {w}</div>', unsafe_allow_html=True)
        if not errors and not warnings:
            st.markdown('<div class="alert alert-ok">✅ All validation checks passed.</div>', unsafe_allow_html=True)

        st.markdown("---")

        # ── Edit / View ───────────────────────────────────────────────────────
        if not st.session_state.stored:
            c_left, c_right = st.columns([6, 1])
            with c_right:
                toggle_lbl = "✏️ Edit" if not st.session_state.edit_mode else "👁 View"
                if st.button(toggle_lbl):
                    st.session_state.edit_mode = not st.session_state.edit_mode
                    st.rerun()

        # ── EDIT MODE ────────────────────────────────────────────────────────
        if st.session_state.edit_mode and not st.session_state.stored:
            st.markdown('<div class="sec-title">Edit Fields</div>', unsafe_allow_html=True)
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.caption("Edit field values below. Field names are shown as labels so you know exactly what you're editing.")

            flat = flatten(data)
            edited_vals = {}
            cols = st.columns(2)
            for i, (k, v) in enumerate(flat.items()):
                if "[" in str(v): continue
                # Show the full dotted field name as label so user knows what they edit
                label = k.replace(".", " › ").replace("_", " ").title()
                edited_vals[k] = cols[i % 2].text_input(
                    label,
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
                log_audit("EDITED", f"Fields modified: {list(edited_vals.keys())}")
                st.rerun()

        # ── VIEW MODE ────────────────────────────────────────────────────────
        else:
            st.markdown('<div class="sec-title">Extracted Fields</div>', unsafe_allow_html=True)
            st.markdown('<div class="card">', unsafe_allow_html=True)

            for key, val in data.items():
                if isinstance(val, list) and val:
                    st.markdown(f'<div class="card-head">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                    if val and isinstance(val[0], dict):
                        st.dataframe(val, use_container_width=True, hide_index=True)
                    else:
                        for item in val:
                            st.markdown(f'<div style="font-size:13px;color:#94a3b8;padding:3px 0;">• {item}</div>', unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)
                    continue

                if isinstance(val, dict):
                    st.markdown(f'<div class="card-head">{key.replace("_"," ").title()}</div>', unsafe_allow_html=True)
                    for k2, v2 in flatten(val).items():
                        lbl = k2.replace("."," › ").replace("_"," ").title()
                        st.markdown(
                            f'<div class="field-row"><div class="field-key">{lbl}</div>'
                            f'<div class="field-val">{str(v2) if v2 is not None else "<span class=\'field-null\'>null</span>"}</div></div>',
                            unsafe_allow_html=True,
                        )
                    st.markdown("<br>", unsafe_allow_html=True)
                    continue

                lbl = key.replace("_"," ").title()
                st.markdown(
                    f'<div class="field-row"><div class="field-key">{lbl}</div>'
                    f'<div class="field-val">{str(val) if val is not None else "<span class=\'field-null\'>null</span>"}</div></div>',
                    unsafe_allow_html=True,
                )

            st.markdown("</div>", unsafe_allow_html=True)

            with st.expander("{ } Raw JSON"):
                st.json(data)

        st.markdown("---")

        # ── Actions ───────────────────────────────────────────────────────────
        if st.session_state.stored:
            st.markdown(f"""
            <div class="success-banner">
                <h3>✅ Document stored successfully</h3>
                <p>
                    ID: <strong>{st.session_state.doc_id}</strong> &nbsp;·&nbsp;
                    Collections: <code>invoices</code> · <code>bank_details</code> ·
                    <code>raw_extractions</code> · <code>audit_log</code>
                    {"" if _db_ok else " <em>(MongoDB offline — not persisted)</em>"}
                </p>
            </div>
            """, unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📂 Process Another Document", use_container_width=True):
                    for k, v in _DEFAULTS.items():
                        st.session_state[k] = v
                    switch("upload")
            with c2:
                export = {
                    "doc_id":      st.session_state.doc_id,
                    "status":      status,
                    "data":        data,
                    "audit_log":   st.session_state.audit,
                    "exported_at": datetime.datetime.now().isoformat(),
                }
                st.download_button(
                    "⬇️ Export JSON",
                    data=json.dumps(export, indent=2, ensure_ascii=False),
                    file_name=f"invoice_{st.session_state.doc_id}.json",
                    mime="application/json",
                    use_container_width=True,
                )
        else:
            c1, c2 = st.columns(2)
            with c1:
                lbl = "✅ Approve & Store" if status == "clean" else "✅ Override & Store"
                if st.button(lbl, type="primary", use_container_width=True):
                    log_audit("APPROVED", f"Status was: {status}")
                    ok = store_to_mongo(st.session_state.doc_id, status, data, st.session_state.audit)
                    if not ok and _db_ok:
                        st.error("Failed to write to MongoDB Atlas.")
                    else:
                        st.session_state.stored = True
                        st.rerun()
            with c2:
                if st.button("🔄 Reject — Back to Step 2", use_container_width=True):
                    payload = {
                        "doc_id":      st.session_state.doc_id,
                        "errors":      errors,
                        "warnings":    warnings,
                        "data":        data,
                        "rejected_at": datetime.datetime.now().isoformat(),
                    }
                    Path("layer3_rejected.json").write_text(
                        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
                    )
                    log_audit("REJECTED", f"Errors: {errors}")
                    st.markdown('<div class="alert alert-warn">⚠️ Document rejected. Saved to <code>layer3_rejected.json</code> for Layer 2 re-processing.</div>', unsafe_allow_html=True)

# ── Close page-wrap ────────────────────────────────────────────────────────────
st.markdown("</div>", unsafe_allow_html=True)