"""
scripts/stream_generator.py
----------------------------
Synthetic transaction event producer for Ledgify.

Behavioural profile
-------------------
  - 95 % of events: low-value transactions ($5 – $800) for a broad user pool.
  - 5 % anomaly window: randomly chooses between two fraud archetypes:
      a) Whale transaction  – single amount $5,000 – $30,000
      b) Rapid-velocity swipe – 3-6 low-value transactions fired in <2 s for
         the same anomalous user, simulating card-skimming behaviour.

  Both anomaly archetypes are constrained to user IDs USR_2000 – USR_2050 so
  the compliance pipeline can build a targeted behavioural baseline.

Redis target stream: ledgify:stream:tx
"""

import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis

# ---------------------------------------------------------------------------
# Resolve project root so we can import config regardless of cwd
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import REDIS_HOST, REDIS_PORT  # noqa: E402

# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

STREAM_NAME = "ledgify:stream:tx"

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

MERCHANT_TYPES = [
    "GROCERY",
    "RESTAURANT",
    "TRAVEL",
    "E-COMMERCE",
    "FUEL",
    "PHARMACY",
    "ENTERTAINMENT",
    "UTILITIES",
    "ATM_WITHDRAWAL",
    "LUXURY_RETAIL",
]

NORMAL_LOCATIONS = [
    "New York, US",
    "Los Angeles, US",
    "Chicago, US",
    "Houston, US",
    "Phoenix, US",
    "Philadelphia, US",
    "San Antonio, US",
    "San Diego, US",
    "Dallas, US",
    "Austin, US",
]

ANOMALY_LOCATIONS = [
    "Lagos, NG",
    "Minsk, BY",
    "Pyongyang, KP",
    "Havana, CU",
    "Tehran, IR",
    "Moscow, RU",
    "Caracas, VE",
    "Tripoli, LY",
]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF"]


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _build_payload(
    user_id: str,
    amount: float,
    location: str,
    merchant_type: str | None = None,
    currency: str = "USD",
) -> dict:
    """Return a fully-formed transaction dictionary ready for Redis XADD."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(amount, 2),
        "currency": currency,
        "merchant_type": merchant_type or random.choice(MERCHANT_TYPES),
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _normal_transaction() -> dict:
    """Generate a routine low-value transaction for the general user pool."""
    user_id = f"USR_{random.randint(1, 1999)}"
    amount = random.uniform(5.0, 800.0)
    location = random.choice(NORMAL_LOCATIONS)
    return _build_payload(user_id, amount, location)


def _whale_transaction() -> dict:
    """Generate a single high-value anomalous transaction."""
    user_id = f"USR_{random.randint(2000, 2050)}"
    amount = random.uniform(5000.0, 30000.0)
    # Anomalous users often appear in high-risk geographies
    location = random.choice(ANOMALY_LOCATIONS + NORMAL_LOCATIONS)
    currency = random.choice(CURRENCIES)
    return _build_payload(user_id, amount, location, currency=currency)


def _rapid_velocity_swipes() -> list[dict]:
    """
    Generate 3-6 rapid-fire low-value transactions for the same anomalous
    user in a suspicious foreign location, simulating card-skimming.
    """
    user_id = f"USR_{random.randint(2000, 2050)}"
    location = random.choice(ANOMALY_LOCATIONS)
    count = random.randint(3, 6)
    return [
        _build_payload(
            user_id,
            amount=random.uniform(12.0, 199.0),
            location=location,
            merchant_type="ATM_WITHDRAWAL",
        )
        for _ in range(count)
    ]


# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------

def produce_events() -> None:
    """
    Continuously publish transaction events to 'ledgify:stream:tx'.

    Anomaly injection rate: 5 % of loop iterations trigger either a whale
    transaction or a rapid-velocity swipe cluster.
    """
    print(f"[stream_generator] Starting producer → {STREAM_NAME}")
    print(f"[stream_generator] Redis at {REDIS_HOST}:{REDIS_PORT}\n")

    published_count = 0

    while True:
        try:
            roll = random.random()  # 0.0 – 1.0

            if roll < 0.95:
                # ── Normal path (95 %) ─────────────────────────────────────
                payload = _normal_transaction()
                msg_id = r.xadd(STREAM_NAME, {"data": json.dumps(payload)})
                published_count += 1
                print(
                    f"[NORMAL #{published_count}] "
                    f"tx={payload['transaction_id'][:8]}… "
                    f"user={payload['user_id']} "
                    f"amount=${payload['amount']:.2f} "
                    f"loc={payload['location']} "
                    f"stream_id={msg_id}"
                )
                time.sleep(random.uniform(0.3, 1.2))

            else:
                # ── Anomaly path (5 %) ────────────────────────────────────
                anomaly_type = random.choice(["whale", "rapid_velocity"])

                if anomaly_type == "whale":
                    payload = _whale_transaction()
                    msg_id = r.xadd(STREAM_NAME, {"data": json.dumps(payload)})
                    published_count += 1
                    print(
                        f"[ANOMALY/WHALE #{published_count}] "
                        f"tx={payload['transaction_id'][:8]}… "
                        f"user={payload['user_id']} "
                        f"amount=${payload['amount']:.2f} "
                        f"loc={payload['location']} "
                        f"stream_id={msg_id}"
                    )
                    time.sleep(random.uniform(0.3, 1.2))

                else:  # rapid_velocity
                    swipes = _rapid_velocity_swipes()
                    print(
                        f"[ANOMALY/RAPID-VELOCITY] Firing {len(swipes)} swipes "
                        f"for {swipes[0]['user_id']} at {swipes[0]['location']}"
                    )
                    for payload in swipes:
                        msg_id = r.xadd(
                            STREAM_NAME, {"data": json.dumps(payload)}
                        )
                        published_count += 1
                        print(
                            f"  ↳ [SWIPE #{published_count}] "
                            f"tx={payload['transaction_id'][:8]}… "
                            f"amount=${payload['amount']:.2f} "
                            f"stream_id={msg_id}"
                        )
                        # Very tight inter-swipe interval – this IS the signal
                        time.sleep(random.uniform(0.05, 0.25))

                    # Longer pause after the burst so the pattern is visible
                    time.sleep(random.uniform(0.3, 1.2))

        except redis.exceptions.ConnectionError as exc:
            print(f"[stream_generator] Redis connection error: {exc}. Retrying in 3 s…")
            time.sleep(3)
        except KeyboardInterrupt:
            print(f"\n[stream_generator] Stopped. Total events published: {published_count}")
            break


if __name__ == "__main__":
    produce_events()