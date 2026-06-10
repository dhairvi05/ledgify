import time
import json
from redis import Redis
from pymongo import MongoClient
import ollama

# 1. Initialize Clients for our distributed systems
redis_client = Redis(host="localhost", port=6379, decode_responses=True)
COMPLIANCE_STREAM = "compliance:stream"
COMPLIANCE_GROUP = "ai_compliance_group"
WORKER_NAME = "ai_processor_1"

# Connect to MongoDB using your exact credentials from docker-compose.yml
mongo_client = MongoClient("mongodb://finguard_db:finguardstream@localhost:27017/")
mongo_db = mongo_client["compliance_audit_db"]
audit_collection = mongo_db["ai_audit_logs"]

def init_compliance_group():
    """Initializes an independent consumer group for the AI layer."""
    try:
        redis_client.xgroup_create(COMPLIANCE_STREAM, COMPLIANCE_GROUP, id="0", mkstream=True)
        print(f"AI Compliance Consumer Group '{COMPLIANCE_GROUP}' initialized.")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            print(f"ℹAI Compliance Group already active. Resuming...")
        else:
            print(f"Error setting up AI Group: {e}")

def run_ai_audit():
    init_compliance_group()
    print(f"{WORKER_NAME} is online and listening for flagged transactions via local Llama3...")
    
    try:
        while True:
            # Grab flagged messages from the secondary compliance stream
            response = redis_client.xreadgroup(COMPLIANCE_GROUP, WORKER_NAME, {COMPLIANCE_STREAM: ">"}, count=1, block=1000)
            
            if not response:
                continue
                
            for stream, messages in response:
                for message_id, message_data in messages:
                    tx_payload = json.loads(message_data["payload"])
                    print(f"[AI AUDITING] Analyzing flagged transaction: {tx_payload['transaction_id']}")
                    
                    # --- SDE Masterstroke: Prompt Engineering for Structured Outputs ---
                    prompt = f"""
                    You are an expert AI Forensic Financial Auditor. Analyze this flagged transaction for potential fraud or money laundering compliance violations:
                    
                    - Transaction ID: {tx_payload['transaction_id']}
                    - User ID: {tx_payload['user_id']}
                    - Amount: ${tx_payload['amount']} {tx_payload['currency']}
                    - Merchant Category: {tx_payload['merchant_type']}
                    - Location: {tx_payload['location']}
                    - ISO Timestamp: {tx_payload['timestamp']}
                    
                    Provide a concise compliance report containing EXACTLY these four sections:
                    1. RISK ASSESSMENT (High/Medium/Low based on amount and context)
                    2. POTENTIAL VIOLATION TYPE (e.g., Large Cash Withdrawal, Structured Velocity, High-Value Outlier)
                    3. INVESTIGATION BRIEF (A 2-sentence rationale summarizing the event)
                    4. RECOMMENDED ACTION (e.g., Freeze Account, Request KYC Verification, Safe to Clear)
                    """
                    
                    # Communicate directly with your local background Ollama instance
                    ai_response = ollama.generate(model="llama3", prompt=prompt)
                    report_text = ai_response["response"]
                    
                    # Create a rich, unstructured document for MongoDB
                    audit_document = {
                        "transaction_id": tx_payload["transaction_id"],
                        "user_id": tx_payload["user_id"],
                        "transaction_metadata": tx_payload,
                        "ai_analysis_report": report_text,
                        "audited_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    }
                    
                    # Write seamlessly to MongoDB document-store
                    inserted_id = audit_collection.insert_one(audit_document).inserted_id
                    print(f"Audit report safely archived in MongoDB Document ID: {inserted_id}")
                    
                    # Acknowledge the message so it clears out of our pending entries list
                    redis_client.xack(COMPLIANCE_STREAM, COMPLIANCE_GROUP, message_id)
                    print(f"AI Audit complete and acknowledged for message {message_id}.\n" + "-"*50)
                    
    except KeyboardInterrupt:
        print("\nAI Compliance Worker halted safely.")
    finally:
        mongo_client.close()

if __name__ == "__main__":
    run_ai_audit()