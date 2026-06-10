from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from pymongo import MongoClient
from typing import List, Dict, Any

# Import our PostgreSQL configuration setup
from config.db_setup import SessionLocal, TransactionLedger

app = FastAPI(title="FinGuard Stream Operational Gateway", version="1.0")

# --- Database Dependency Helpers ---
def get_db():
    """Yields a relational database session for a single API request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize MongoDB Client connection
mongo_client = MongoClient("mongodb://finguard_db:finguardstream@localhost:27017/")
mongo_db = mongo_client["compliance_audit_db"]
audit_collection = mongo_db["ai_audit_logs"]

# --- API Endpoints ---

@app.get("/")
def read_root():
    return {"status": "ONLINE", "system": "FinGuard Real-Time Core"}

@app.get("/api/transactions", response_model=List[Dict[str, Any]])
def get_recent_transactions(limit: int = 20, db: Session = Depends(get_db)):
    """Fetches the latest settled transactions directly from the PostgreSQL Ledger."""
    transactions = db.query(TransactionLedger).order_by(TransactionLedger.id.desc()).limit(limit).all()
    
    # Format database models into clean JSON dictionaries
    return [
        {
            "id": tx.id,
            "transaction_id": tx.transaction_id,
            "user_id": tx.user_id,
            "amount": tx.amount,
            "currency": tx.currency,
            "merchant_type": tx.merchant_type,
            "location": tx.location,
            "status": tx.status
        }
        for tx in transactions
    ]

@app.get("/api/compliance/alerts", response_model=List[Dict[str, Any]])
def get_ai_audit_logs(limit: int = 10):
    """Fetches unstructured local GenAI forensic fraud analysis logs from MongoDB."""
    try:
        # Fetch the latest logs, excluding the internal MongoDB '_id' field for clean parsing
        logs = list(audit_collection.find({}, {"_id": 0}).sort("audited_at", -1).limit(limit))
        return logs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database read error: {e}")

@app.on_event("shutdown")
def shutdown_db_client():
    mongo_client.close()