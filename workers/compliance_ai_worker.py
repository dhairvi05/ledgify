"""
workers/compliance_ai_worker.py
--------------------------------
Secondary AI-powered compliance consumer for Ledgify.

Architecture
------------
This worker implements an edge-native, privacy-preserving forensic pipeline:

  1. Reads FLAGGED transaction events from Redis Stream 'ledgify:compliance'
     via Consumer Group 'compliance_group'.

  2. Retrieves the last 10 historical transactions for the same user_id from
     PostgreSQL to compute a behavioural baseline (average spend, common
     locations).  This baseline is injected directly into the LLM prompt as
     Retrieval-Augmented Generation (RAG) context.

  3. Calls Ollama (model: llama3) running on localhost.  All inference happens
     in-process; transaction data never leaves the internal network boundary.

  4. Parses the structured AI report and persists a BSON document to MongoDB
     collection 'compliance_audit_db.ai_forensic_logs'.

  5. Acknowledges the stream message only after a successful MongoDB write,
     guaranteeing at-least-once delivery semantics.

Expected AI report schema (Markdown sections in LLM output):
  ## Risk Rating          – e.g. CRITICAL / HIGH / MEDIUM / LOW
  ## Financial Threat Typology  – e.g. Card Skimming, Money Laundering
  ## Narrative Rationale  – free-form forensic analysis paragraph
  ## Enforcement Action   – recommended next step
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import ollama
import redis
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Project root on path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import (  # noqa: E402
    MONGO_URI,
    REDIS_HOST,
    REDIS_PORT,
    SessionLocal,
    TransactionLedger,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COMPLIANCE_STREAM = "ledgify:compliance"
CONSUMER_GROUP = "compliance_group"
CONSUMER_NAME = "compliance_worker_1"
BLOCK_MS = 3000
BATCH_SIZE = 5
OLLAMA_MODEL = "llama3"
HISTORY_LIMIT = 10

MONGO_DB_NAME = "compliance_audit_db"
MONGO_COLLECTION = "ai_forensic_logs"

# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

mongo_client = MongoClient(MONGO_URI)
audit_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]


# ---------------------------------------------------------------------------
# Consumer group bootstrap
# ---------------------------------------------------------------------------

def ensure_consumer_group() -> None:
    try:
        r.xgroup_create(COMPLIANCE_STREAM, CONSUMER_GROUP, id="$", mkstream=True)
        print(f"[compliance_worker] Consumer group '{CONSUMER_GROUP}' created.")
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            print(f"[compliance_worker] Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise


# ---------------------------------------------------------------------------
# RAG: historical baseline query
# ---------------------------------------------------------------------------

def fetch_user_baseline(user_id: str) -> dict:
    """
    Query PostgreSQL for the last HISTORY_LIMIT transactions by this user
    and compute a behavioural baseline for RAG context injection.

    Returns a dict with:
      - history_count     : number of records found
      - average_amount    : float mean of historical amounts
      - max_amount        : highest historical single transaction
      - common_locations  : sorted list of unique locations seen
      - recent_statuses   : list of the last N statuses (oldest→newest)
    """
    session = SessionLocal()
    try:
        rows = (
            session.query(TransactionLedger)
            .filter(TransactionLedger.user_id == user_id)
            .order_by(TransactionLedger.timestamp.desc())
            .limit(HISTORY_LIMIT)
            .all()
        )

        if not rows:
            return {
                "history_count": 0,
                "average_amount": 0.0,
                "max_amount": 0.0,
                "common_locations": [],
                "recent_statuses": [],
            }

        amounts = [r.amount for r in rows]
        locations = list({r.location for r in rows if r.location})
        statuses = [r.status for r in reversed(rows)]  # oldest first

        return {
            "history_count": len(rows),
            "average_amount": round(mean(amounts), 2),
            "max_amount": round(max(amounts), 2),
            "common_locations": sorted(locations),
            "recent_statuses": statuses,
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Prompt engineering
# ---------------------------------------------------------------------------

def build_forensic_prompt(payload: dict, baseline: dict) -> str:
    """
    Construct an intensive forensic prompt with clear Markdown section
    boundaries so the LLM can return a structured audit report.

    The baseline dict is injected as the RAG context block.
    """
    prompt = f"""
You are a senior financial crimes compliance officer and AI forensic analyst.
Your task is to audit the following flagged transaction and produce a formal
compliance report in the EXACT structure specified below.

---
## FLAGGED TRANSACTION UNDER REVIEW

- Transaction ID  : {payload.get('transaction_id')}
- User ID         : {payload.get('user_id')}
- Amount          : ${float(payload.get('amount', 0)):.2f} {payload.get('currency', 'USD')}
- Merchant Type   : {payload.get('merchant_type', 'UNKNOWN')}
- Location        : {payload.get('location', 'UNKNOWN')}
- Timestamp       : {payload.get('timestamp')}
- System Status   : FLAGGED

---
## USER BEHAVIOURAL BASELINE (RAG Context — last {HISTORY_LIMIT} transactions)

- Historical Record Count : {baseline['history_count']}
- Average Transaction Amount : ${baseline['average_amount']:.2f}
- Maximum Historical Amount  : ${baseline['max_amount']:.2f}
- Known Locations            : {', '.join(baseline['common_locations']) or 'None on record'}
- Recent Transaction Statuses: {', '.join(baseline['recent_statuses']) or 'No history'}

---
## AUDIT REPORT — REQUIRED OUTPUT FORMAT

Respond ONLY with the four sections below, using these exact Markdown headers.
Do not add preamble, do not add extra sections.

## Risk Rating
[ONE of: CRITICAL / HIGH / MEDIUM / LOW — justified by the data above]

## Financial Threat Typology
[The most likely financial crime category, e.g.:
 Card Skimming, Account Takeover, Money Laundering, Structuring,
 Smurfing, Synthetic Identity Fraud, Insider Threat, None Detected]

## Narrative Rationale
[2–4 sentences. Explain precisely why this transaction deviates from the
user's established behavioural baseline, citing specific numeric deltas
where relevant.  Reference the location anomaly and/or velocity pattern
if applicable.]

## Enforcement Action
[ONE concrete next step, e.g.:
 Freeze account and notify compliance team |
 Escalate to Tier-2 fraud investigation |
 Request transaction verification from cardholder |
 File Suspicious Activity Report (SAR) |
 No action required — monitor for recurrence]
"""
    return prompt.strip()


# ---------------------------------------------------------------------------
# AI inference
# ---------------------------------------------------------------------------

def run_ollama_inference(prompt: str) -> str:
    """
    Submit the forensic prompt to the local Ollama instance (llama3) and
    return the raw text response.

    All inference is local – no data leaves the system boundary.
    """
    response = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a financial compliance AI.  "
                    "You always respond in the exact structured format requested."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )
    return response["message"]["content"]


# ---------------------------------------------------------------------------
# Report parser
# ---------------------------------------------------------------------------

def parse_ai_report(raw_text: str) -> dict:
    """
    Extract the four structured fields from the LLM Markdown output.

    Uses regex anchored on the expected ## headers.  Falls back to the raw
    text string if a section cannot be isolated.
    """

    def _extract_section(header: str, text: str) -> str:
        pattern = rf"##\s*{re.escape(header)}\s*\n(.*?)(?=\n##\s|\Z)"
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return "PARSE_ERROR – section not found"

    return {
        "risk_rating": _extract_section("Risk Rating", raw_text),
        "threat_typology": _extract_section("Financial Threat Typology", raw_text),
        "narrative_rationale": _extract_section("Narrative Rationale", raw_text),
        "enforcement_action": _extract_section("Enforcement Action", raw_text),
    }


# ---------------------------------------------------------------------------
# MongoDB persistence
# ---------------------------------------------------------------------------

def persist_to_mongo(payload: dict, raw_ai_text: str, parsed_report: dict) -> None:
    """
    Combine the original flagged payload with the AI audit output and insert
    a single BSON document into the MongoDB compliance collection.
    """
    document = {
        # ── Original transaction fields ─────────────────────────────────
        "transaction_id": payload.get("transaction_id"),
        "user_id": payload.get("user_id"),
        "amount": float(payload.get("amount", 0)),
        "currency": payload.get("currency", "USD"),
        "merchant_type": payload.get("merchant_type"),
        "location": payload.get("location"),
        "transaction_timestamp": payload.get("timestamp"),
        # ── AI audit output ─────────────────────────────────────────────
        "risk_rating": parsed_report["risk_rating"],
        "threat_typology": parsed_report["threat_typology"],
        "narrative_rationale": parsed_report["narrative_rationale"],
        "enforcement_action": parsed_report["enforcement_action"],
        "raw_ai_output": raw_ai_text,
        # ── Audit metadata ───────────────────────────────────────────────
        "audited_at": datetime.now(timezone.utc),
        "ai_model": OLLAMA_MODEL,
    }

    result = audit_collection.insert_one(document)
    print(
        f"  [MONGO ✓] Inserted audit doc _id={result.inserted_id} "
        f"for tx={payload.get('transaction_id', '')[:8]}…"
    )


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def process_compliance_message(stream_id: str, fields: dict) -> None:
    """Full compliance pipeline for one flagged transaction."""
    raw = fields.get("data", "{}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"  [PARSE ERROR] stream_id={stream_id} – {exc}")
        return

    user_id = payload.get("user_id", "UNKNOWN")
    tx_short = payload.get("transaction_id", "???")[:8]
    print(f"\n  [COMPLIANCE] Processing tx={tx_short}… user={user_id}")

    # Step 1 – RAG: build user baseline from PostgreSQL ──────────────────
    baseline = fetch_user_baseline(user_id)
    print(
        f"  [RAG] avg_amount=${baseline['average_amount']:.2f} "
        f"history_count={baseline['history_count']} "
        f"locations={baseline['common_locations']}"
    )

    # Step 2 – Prompt engineering ────────────────────────────────────────
    prompt = build_forensic_prompt(payload, baseline)

    # Step 3 – Local AI inference ────────────────────────────────────────
    print(f"  [OLLAMA] Submitting forensic prompt to {OLLAMA_MODEL}…")
    try:
        raw_ai_text = run_ollama_inference(prompt)
    except Exception as exc:
        print(f"  [OLLAMA ERROR] {exc}")
        raw_ai_text = f"INFERENCE_FAILED: {exc}"

    # Step 4 – Parse structured fields ───────────────────────────────────
    parsed_report = parse_ai_report(raw_ai_text)
    print(
        f"  [AI REPORT] Risk={parsed_report['risk_rating']} | "
        f"Type={parsed_report['threat_typology']} | "
        f"Action={parsed_report['enforcement_action']}"
    )

    # Step 5 – Persist to MongoDB ────────────────────────────────────────
    persist_to_mongo(payload, raw_ai_text, parsed_report)


def run() -> None:
    print(f"[compliance_worker] Starting.  Listening on '{COMPLIANCE_STREAM}'…\n")
    ensure_consumer_group()

    while True:
        try:
            entries = r.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={COMPLIANCE_STREAM: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not entries:
                continue

            for stream_name, messages in entries:
                for stream_id, fields in messages:
                    print(f"\n[MSG] compliance stream_id={stream_id}")
                    try:
                        process_compliance_message(stream_id, fields)
                        # Acknowledge only on successful pipeline completion
                        r.xack(COMPLIANCE_STREAM, CONSUMER_GROUP, stream_id)
                    except Exception as exc:
                        print(f"  [PIPELINE ERROR] {exc} – message left in PEL for retry")

        except redis.exceptions.ConnectionError as exc:
            print(f"[compliance_worker] Redis error: {exc}. Retrying in 3 s…")
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n[compliance_worker] Graceful shutdown.")
            break


if __name__ == "__main__":
    run()