"""
workers/ingestion_worker.py
----------------------------
Primary stream consumer for Ledgify.

Responsibilities
----------------
1. Read raw transaction events from Redis Stream 'ledgify:stream:tx'
   via Consumer Group 'ingestion_group'.
2. Deserialise each payload and apply a two-tier classification rule:
     • amount < $5,000 AND location is not an outlier  →  status = "SETTLED"
     • amount >= $5,000 OR location is an outlier       →  status = "FLAGGED"
3. Persist every transaction to PostgreSQL (TransactionLedger table).
4. For FLAGGED transactions: clone the payload, push it to the secondary
   Redis Stream 'ledgify:compliance' for the AI compliance worker, then
   immediately xack the primary stream – never block on the AI path.

Location outlier detection (simulated sliding window)
------------------------------------------------------
A fixed high-risk location set is used as the outlier baseline.  In a
production deployment this would be replaced with a sliding-window cache
(e.g. Redis ZSET) that tracks recent per-user geo-coordinates and flags
sudden jumps in distance-over-time.
"""

import json
import sys
import time
from pathlib import Path

import redis
from sqlalchemy.exc import IntegrityError

# ---------------------------------------------------------------------------
# Project root on path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import (  # noqa: E402
    REDIS_HOST,
    REDIS_PORT,
    SessionLocal,
    TransactionLedger,
    init_db,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRIMARY_STREAM = "ledgify:stream:tx"
COMPLIANCE_STREAM = "ledgify:compliance"
CONSUMER_GROUP = "ingestion_group"
CONSUMER_NAME = "ingestion_worker_1"
BLOCK_MS = 2000          # Block on XREADGROUP for up to 2 seconds
BATCH_SIZE = 10          # Messages to fetch per XREADGROUP call
AMOUNT_FLAG_THRESHOLD = 5_000.0

# Geography-based risk set (simulates outlier location detection)
HIGH_RISK_LOCATIONS = {
    "Lagos, NG",
    "Minsk, BY",
    "Pyongyang, KP",
    "Havana, CU",
    "Tehran, IR",
    "Moscow, RU",
    "Caracas, VE",
    "Tripoli, LY",
}


# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ---------------------------------------------------------------------------
# Consumer group bootstrap
# ---------------------------------------------------------------------------

def ensure_consumer_group() -> None:
    """
    Create the consumer group if it does not already exist.
    '$' means: only new messages going forward; use '0' to replay from start.
    """
    try:
        r.xgroup_create(PRIMARY_STREAM, CONSUMER_GROUP, id="$", mkstream=True)
        print(f"[ingestion_worker] Consumer group '{CONSUMER_GROUP}' created.")
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            print(f"[ingestion_worker] Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def _is_location_outlier(location: str) -> bool:
    """Return True when the transaction location is in the high-risk set."""
    return location in HIGH_RISK_LOCATIONS


def classify_transaction(payload: dict) -> str:
    """
    Apply the two-tier rule engine and return the appropriate status string.

    Rules (any match → FLAGGED):
      1. Amount >= $5,000
      2. Location is a known high-risk geography
    """
    amount: float = float(payload.get("amount", 0))
    location: str = payload.get("location", "")

    if amount >= AMOUNT_FLAG_THRESHOLD or _is_location_outlier(location):
        return "FLAGGED"
    return "SETTLED"


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def persist_to_postgres(payload: dict, status: str) -> None:
    """
    Write a transaction record to PostgreSQL.

    Uses a short-lived session per message to keep lock scope minimal.
    Duplicate transaction_id entries are silently skipped (idempotency).
    """
    session = SessionLocal()
    try:
        record = TransactionLedger(
            transaction_id=payload["transaction_id"],
            user_id=payload["user_id"],
            amount=float(payload["amount"]),
            currency=payload.get("currency", "USD"),
            merchant_type=payload.get("merchant_type"),
            location=payload.get("location"),
            status=status,
        )
        session.add(record)
        session.commit()
        print(
            f"  [PG ✓] {status} | tx={payload['transaction_id'][:8]}… "
            f"user={payload['user_id']} amount=${float(payload['amount']):.2f}"
        )
    except IntegrityError:
        session.rollback()
        print(f"  [PG SKIP] Duplicate tx={payload['transaction_id'][:8]}… – skipped.")
    except Exception as exc:
        session.rollback()
        print(f"  [PG ERROR] {exc}")
        raise
    finally:
        session.close()


def forward_to_compliance_stream(payload: dict) -> None:
    """
    Clone the flagged payload and publish it to the compliance stream.

    The clone is a shallow copy so the original dict is not mutated.
    """
    compliance_payload = dict(payload)
    compliance_payload["flagged_at"] = time.time()
    msg_id = r.xadd(COMPLIANCE_STREAM, {"data": json.dumps(compliance_payload)})
    print(
        f"  [COMPLIANCE →] tx={payload['transaction_id'][:8]}… "
        f"forwarded to '{COMPLIANCE_STREAM}' as {msg_id}"
    )


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def process_message(stream_id: str, fields: dict) -> None:
    """Deserialise, classify, persist, and conditionally forward one message."""
    raw = fields.get("data", "{}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"  [PARSE ERROR] stream_id={stream_id} – {exc}")
        return

    status = classify_transaction(payload)
    persist_to_postgres(payload, status)

    if status == "FLAGGED":
        forward_to_compliance_stream(payload)


def run() -> None:
    """
    Blocking consumer loop.

    Uses '>' as the message ID, meaning: deliver only messages that have not
    yet been delivered to any consumer in this group.  After processing each
    message we immediately xack to release it from the Pending Entries List
    (PEL), even for FLAGGED transactions – the compliance work is fully async.
    """
    print(f"[ingestion_worker] Starting.  Listening on '{PRIMARY_STREAM}'…\n")
    init_db()
    ensure_consumer_group()

    while True:
        try:
            entries = r.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={PRIMARY_STREAM: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not entries:
                continue  # Timeout – loop back and block again

            for stream_name, messages in entries:
                for stream_id, fields in messages:
                    print(f"\n[MSG] stream_id={stream_id}")
                    process_message(stream_id, fields)
                    # Acknowledge immediately regardless of outcome
                    r.xack(PRIMARY_STREAM, CONSUMER_GROUP, stream_id)

        except redis.exceptions.ConnectionError as exc:
            print(f"[ingestion_worker] Redis error: {exc}. Retrying in 3 s…")
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n[ingestion_worker] Graceful shutdown.")
            break


if __name__ == "__main__":
    run()