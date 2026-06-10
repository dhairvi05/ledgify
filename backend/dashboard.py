"""
backend/dashboard.py
---------------------
Streamlit real-time monitoring dashboard for Ledgify.

Layout
------
  ┌────────────────────────────────────────────────────┐
  │  🏦  LEDGIFY  –  Financial Fraud Monitor            │
  ├────────────────────────────────────────────────────┤
  │  [Auto-refresh every N s]  [Manual Refresh button] │
  ├─────────────────────┬──────────────────────────────┤
  │  TRANSACTION LEDGER │  COMPLIANCE ALERTS           │
  │  (colour-coded rows)│  (metric grid + detail cards)│
  └─────────────────────┴──────────────────────────────┘

Colour coding
-------------
  SETTLED → green background
  FLAGGED → red background

Run with:
    streamlit run backend/dashboard.py
"""

import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "http://localhost:8000"
LEDGER_ENDPOINT = f"{API_BASE_URL}/api/ledger"
ALERTS_ENDPOINT = f"{API_BASE_URL}/api/alerts"
AUTO_REFRESH_INTERVAL_S = 10  # Seconds between automatic data refreshes

# ---------------------------------------------------------------------------
# Page configuration – must be the first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Ledgify – Fraud Monitor",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* ── Global ────────────────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'IBM Plex Mono', 'Courier New', monospace;
    }

    /* ── Header ─────────────────────────────────────────────────────────── */
    .ledgify-header {
        background: linear-gradient(135deg, #0d0d0d 0%, #1a1a2e 60%, #16213e 100%);
        padding: 1.4rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.4rem;
        border-left: 5px solid #e94560;
    }
    .ledgify-header h1 {
        color: #ffffff;
        font-size: 2rem;
        letter-spacing: 3px;
        margin: 0;
    }
    .ledgify-header p {
        color: #8892b0;
        margin: 0.3rem 0 0 0;
        font-size: 0.85rem;
    }

    /* ── Section headings ───────────────────────────────────────────────── */
    .section-title {
        font-size: 0.75rem;
        letter-spacing: 3px;
        text-transform: uppercase;
        color: #e94560;
        margin-bottom: 0.5rem;
        border-bottom: 1px solid #1e2a3a;
        padding-bottom: 0.3rem;
    }

    /* ── Transaction rows ───────────────────────────────────────────────── */
    .tx-row-settled {
        background-color: rgba(39, 174, 96, 0.12);
        border-left: 3px solid #27ae60;
        padding: 0.5rem 0.8rem;
        margin-bottom: 0.35rem;
        border-radius: 4px;
        font-size: 0.82rem;
    }
    .tx-row-flagged {
        background-color: rgba(233, 69, 96, 0.14);
        border-left: 3px solid #e94560;
        padding: 0.5rem 0.8rem;
        margin-bottom: 0.35rem;
        border-radius: 4px;
        font-size: 0.82rem;
    }
    .tx-badge-settled {
        background: #27ae60;
        color: #fff;
        padding: 1px 8px;
        border-radius: 10px;
        font-size: 0.7rem;
        font-weight: bold;
    }
    .tx-badge-flagged {
        background: #e94560;
        color: #fff;
        padding: 1px 8px;
        border-radius: 10px;
        font-size: 0.7rem;
        font-weight: bold;
    }

    /* ── Metric cards ───────────────────────────────────────────────────── */
    .metric-card {
        background: #0f172a;
        border: 1px solid #1e2a3a;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .metric-card .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #e94560;
    }
    .metric-card .metric-label {
        font-size: 0.72rem;
        color: #8892b0;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    /* ── Alert cards ────────────────────────────────────────────────────── */
    .alert-card {
        background: #0f172a;
        border: 1px solid #1e2a3a;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
    }
    .alert-card .risk-critical { color: #ff4757; font-weight: bold; }
    .alert-card .risk-high     { color: #ff6b35; font-weight: bold; }
    .alert-card .risk-medium   { color: #ffd700; font-weight: bold; }
    .alert-card .risk-low      { color: #27ae60; font-weight: bold; }
    .alert-card .field-label {
        font-size: 0.68rem;
        color: #8892b0;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-top: 0.5rem;
    }
    .alert-card .field-value {
        font-size: 0.85rem;
        color: #ccd6f6;
    }

    /* ── Refresh bar ────────────────────────────────────────────────────── */
    .refresh-bar {
        font-size: 0.75rem;
        color: #8892b0;
        text-align: right;
        margin-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------


@st.cache_data(ttl=AUTO_REFRESH_INTERVAL_S)
def fetch_ledger() -> list[dict]:
    """Fetch latest transactions from the FastAPI /api/ledger endpoint."""
    try:
        response = requests.get(LEDGER_ENDPOINT, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"⚠️  Could not reach ledger API: {exc}")
        return []


@st.cache_data(ttl=AUTO_REFRESH_INTERVAL_S)
def fetch_alerts() -> list[dict[str, Any]]:
    """Fetch latest compliance alerts from the FastAPI /api/alerts endpoint."""
    try:
        response = requests.get(ALERTS_ENDPOINT, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"⚠️  Could not reach alerts API: {exc}")
        return []


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------


def _risk_colour_class(risk: str) -> str:
    mapping = {
        "CRITICAL": "risk-critical",
        "HIGH": "risk-high",
        "MEDIUM": "risk-medium",
        "LOW": "risk-low",
    }
    upper = risk.upper().strip()
    for key, css_class in mapping.items():
        if key in upper:
            return css_class
    return "risk-medium"


def render_header() -> None:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    st.markdown(
        f"""
        <div class="ledgify-header">
            <h1>🏦 LEDGIFY</h1>
            <p>Edge-Native Financial Fraud Monitoring &amp; Compliance Pipeline
               &nbsp;·&nbsp; Last update: {now_str}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_transaction_ledger(transactions: list[dict]) -> None:
    st.markdown('<div class="section-title">Transaction Ledger — Latest 30</div>', unsafe_allow_html=True)

    if not transactions:
        st.info("No transactions found.  Make sure the ingestion worker and stream generator are running.")
        return

    # Summary metrics row
    df = pd.DataFrame(transactions)
    total = len(df)
    settled = (df["status"] == "SETTLED").sum() if "status" in df.columns else 0
    flagged = (df["status"] == "FLAGGED").sum() if "status" in df.columns else 0

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Total Shown", total)
    with m2:
        st.metric("Settled", settled)
    with m3:
        st.metric("Flagged", flagged)

    st.markdown("---")

    # Colour-coded transaction rows
    for tx in transactions:
        status = tx.get("status", "SETTLED")
        css_class = "tx-row-flagged" if status == "FLAGGED" else "tx-row-settled"
        badge_class = "tx-badge-flagged" if status == "FLAGGED" else "tx-badge-settled"

        ts_raw = tx.get("timestamp", "")
        try:
            ts_display = datetime.fromisoformat(ts_raw).strftime("%H:%M:%S")
        except (ValueError, TypeError):
            ts_display = ts_raw[:19] if ts_raw else "—"

        st.markdown(
            f"""
            <div class="{css_class}">
                <span class="{badge_class}">{status}</span>
                &nbsp;
                <strong>{tx.get('user_id', '—')}</strong>
                &nbsp;·&nbsp;
                <strong>${tx.get('amount', 0):.2f}</strong> {tx.get('currency', 'USD')}
                &nbsp;·&nbsp;
                {tx.get('merchant_type', '—')}
                &nbsp;·&nbsp;
                📍 {tx.get('location', '—')}
                &nbsp;·&nbsp;
                🕒 {ts_display}
                &nbsp;·&nbsp;
                <code style="font-size:0.7rem;color:#8892b0">{tx.get('transaction_id', '—')[:13]}…</code>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_compliance_alerts(alerts: list[dict[str, Any]]) -> None:
    st.markdown('<div class="section-title">AI Forensic Compliance Alerts</div>', unsafe_allow_html=True)

    if not alerts:
        st.info("No compliance alerts yet.  Waiting for the AI compliance worker to process flagged transactions.")
        return

    # ── Metric grid ──────────────────────────────────────────────────────
    total_alerts = len(alerts)
    critical_count = sum(
        1 for a in alerts if "CRITICAL" in str(a.get("risk_rating", "")).upper()
    )
    high_count = sum(
        1 for a in alerts if "HIGH" in str(a.get("risk_rating", "")).upper()
        and "CRITICAL" not in str(a.get("risk_rating", "")).upper()
    )
    unique_users = len({a.get("user_id") for a in alerts})

    g1, g2, g3, g4 = st.columns(4)
    with g1:
        st.markdown(
            '<div class="metric-card">'
            f'<div class="metric-value">{total_alerts}</div>'
            '<div class="metric-label">Total Alerts</div>'
            "</div>",
            unsafe_allow_html=True,
        )
    with g2:
        st.markdown(
            '<div class="metric-card">'
            f'<div class="metric-value" style="color:#ff4757">{critical_count}</div>'
            '<div class="metric-label">Critical</div>'
            "</div>",
            unsafe_allow_html=True,
        )
    with g3:
        st.markdown(
            '<div class="metric-card">'
            f'<div class="metric-value" style="color:#ff6b35">{high_count}</div>'
            '<div class="metric-label">High Risk</div>'
            "</div>",
            unsafe_allow_html=True,
        )
    with g4:
        st.markdown(
            '<div class="metric-card">'
            f'<div class="metric-value" style="color:#64ffda">{unique_users}</div>'
            '<div class="metric-label">Unique Users</div>'
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Detail cards (expandable) ─────────────────────────────────────────
    for idx, alert in enumerate(alerts, start=1):
        risk = str(alert.get("risk_rating", "UNKNOWN")).strip()
        typology = alert.get("threat_typology", "Unknown")
        action = alert.get("enforcement_action", "—")
        narrative = alert.get("narrative_rationale", "No narrative available.")
        user_id = alert.get("user_id", "—")
        amount = alert.get("amount", 0)
        currency = alert.get("currency", "USD")
        location = alert.get("location", "—")
        tx_id = str(alert.get("transaction_id", "—"))[:16]
        audited_at = alert.get("audited_at", "—")

        risk_css = _risk_colour_class(risk)
        expander_label = (
            f"🚨  #{idx}  |  {user_id}  |  ${amount:.2f} {currency}  "
            f"|  <{risk}>  |  {typology[:40]}"
        )

        with st.expander(expander_label, expanded=(idx == 1)):
            col_a, col_b = st.columns([1, 2])

            with col_a:
                st.markdown(
                    f"""
                    <div class="alert-card">
                        <div class="field-label">Risk Rating</div>
                        <div class="field-value"><span class="{risk_css}">{risk}</span></div>

                        <div class="field-label">Threat Typology</div>
                        <div class="field-value">{typology}</div>

                        <div class="field-label">Enforcement Action</div>
                        <div class="field-value">{action}</div>

                        <div class="field-label">User</div>
                        <div class="field-value">{user_id}</div>

                        <div class="field-label">Amount</div>
                        <div class="field-value">${amount:.2f} {currency}</div>

                        <div class="field-label">Location</div>
                        <div class="field-value">📍 {location}</div>

                        <div class="field-label">Transaction ID</div>
                        <div class="field-value"><code>{tx_id}…</code></div>

                        <div class="field-label">Audited At</div>
                        <div class="field-value">{audited_at}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with col_b:
                st.markdown("**Narrative Rationale**")
                st.text_area(
                    label="",
                    value=narrative,
                    height=160,
                    key=f"narrative_{idx}_{tx_id}",
                    disabled=True,
                    label_visibility="collapsed",
                )
                with st.expander("Full AI Output", expanded=False):
                    st.text(alert.get("raw_ai_output", "Not available."))


# ---------------------------------------------------------------------------
# Auto-refresh sidebar control
# ---------------------------------------------------------------------------

def render_sidebar() -> None:
    with st.sidebar:
        st.title("Controls")
        st.markdown("---")

        refresh_interval = st.slider(
            "Auto-refresh interval (s)",
            min_value=5,
            max_value=60,
            value=AUTO_REFRESH_INTERVAL_S,
            step=5,
        )

        if st.button("Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.markdown("**Services**")
        st.markdown(f"API: `{API_BASE_URL}`")
        st.markdown("PostgreSQL: `localhost:5432`")
        st.markdown("MongoDB: `localhost:27017`")
        st.markdown("Redis: `localhost:6379`")
        st.markdown("Ollama: `localhost:11434`")

        return refresh_interval


# ---------------------------------------------------------------------------
# Main application entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    refresh_interval = render_sidebar()
    render_header()

    # Manual refresh button in main pane
    top_bar_col1, top_bar_col2 = st.columns([5, 1])
    with top_bar_col2:
        if st.button("Refresh"):
            st.cache_data.clear()
            st.rerun()

    with top_bar_col1:
        st.markdown(
            f'<div class="refresh-bar">Auto-refreshes every {refresh_interval} s</div>',
            unsafe_allow_html=True,
        )

    # ── Two-column layout ────────────────────────────────────────────────
    left_col, right_col = st.columns([1, 1], gap="large")

    with left_col:
        transactions = fetch_ledger()
        render_transaction_ledger(transactions)

    with right_col:
        alerts = fetch_alerts()
        render_compliance_alerts(alerts)

    # ── Auto-refresh via periodic page reload ────────────────────────────
    time.sleep(refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()