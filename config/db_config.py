"""
config/db_config.py
-------------------
Centralised database configuration for Ledgify.

Responsibilities:
  - SQLAlchemy declarative engine pool → PostgreSQL (transactional ledger)
  - ORM model: TransactionLedger
  - init_db() helper called once at startup to materialise the schema
  - MongoDB URI constant (used by workers and backend)
  - Redis connection helper
"""

import os
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# ---------------------------------------------------------------------------
# Connection strings (override via environment variables in production)
# ---------------------------------------------------------------------------

POSTGRES_DSN: str = os.getenv(
    "POSTGRES_DSN",
    "postgresql://ledgify_admin:ledgify@localhost:5432/ledgify_db",
)

MONGO_URI: str = os.getenv(
    "MONGO_URI",
    "mongodb://ledgify_doc_admin:ledgify@localhost:27017/?authSource=admin",
)

REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))

# ---------------------------------------------------------------------------
# SQLAlchemy engine & session factory
# ---------------------------------------------------------------------------

engine = create_engine(
    POSTGRES_DSN,
    pool_size=10,          # Max persistent connections in pool
    max_overflow=20,       # Extra connections allowed under load
    pool_pre_ping=True,    # Validate connections before checkout
    echo=False,            # Set True for SQL query logging during debug
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ORM model: TransactionLedger
# ---------------------------------------------------------------------------

class TransactionLedger(Base):
    """
    Maps to the `transaction_ledger` PostgreSQL table.

    Each row represents one financial transaction event as ingested from the
    Redis Stream by the ingestion worker.
    """

    __tablename__ = "transaction_ledger"

    id = Column(Integer, primary_key=True, autoincrement=True)

    transaction_id = Column(
        String(64),
        unique=True,
        index=True,
        nullable=False,
        comment="UUID-based canonical identifier for the transaction",
    )

    user_id = Column(
        String(32),
        index=True,
        nullable=False,
        comment="Originating user identifier, e.g. USR_1042",
    )

    amount = Column(
        Float,
        nullable=False,
        comment="Transaction value in the specified currency",
    )

    currency = Column(
        String(8),
        nullable=False,
        default="USD",
        comment="ISO-4217 currency code",
    )

    merchant_type = Column(
        String(64),
        nullable=True,
        comment="MCC-style merchant category, e.g. GROCERY, TRAVEL",
    )

    location = Column(
        String(128),
        nullable=True,
        comment="City or geo-identifier at point of transaction",
    )

    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="UTC wall-clock time the transaction was processed",
    )

    status = Column(
        String(16),
        nullable=False,
        default="SETTLED",
        comment="SETTLED | FLAGGED",
    )

    def to_dict(self) -> dict:
        """Serialise the row to a plain dictionary for API responses."""
        return {
            "id": self.id,
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "currency": self.currency,
            "merchant_type": self.merchant_type,
            "location": self.location,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "status": self.status,
        }


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Programmatically create all tables declared in the ORM metadata.

    Safe to call multiple times – SQLAlchemy uses `CREATE TABLE IF NOT EXISTS`
    semantics via `checkfirst=True` (the default).
    """
    Base.metadata.create_all(bind=engine)
    print("[init_db] PostgreSQL schema initialised successfully.")


# ---------------------------------------------------------------------------
# Convenience dependency for FastAPI (yields a session, auto-closes it)
# ---------------------------------------------------------------------------

def get_db():
    """
    FastAPI dependency that yields a SQLAlchemy session and guarantees
    the session is closed on exit, even if an exception occurs.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()