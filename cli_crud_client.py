import json
import os
from dotenv import load_dotenv

# Import components
from core.metadata_manager import MetadataManager
from core.crud_engine import CRUDEngine  # The NEW engine
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler

def main():
    load_dotenv()
    
    print("\n" + "="*50)
    print("  ASSIGNMENT 2: CRUD TEST CLIENT")
    print("="*50)

    # 1. Setup
    print("[Init] Connecting to Metadata & Databases...")
    meta = MetadataManager()
    
    # Check if metadata exists
    if not meta.global_schema.get("relational_structure"):
        print("\n⚠️  WARNING: Metadata is empty!")
        print("   Please runs 'python main.py' first to ingest simulated data.")
        print("   Then come back here to query it.\n")
    
    sql = SQLHandler()
    mongo = MongoHandler()
    
    # 2. Init Engine
    engine = CRUDEngine(sql, mongo, meta)
    
    print("[Init] Ready.")
    print("-" * 50)
    print("Instructions:")
    print("1. Find a UUID from your SQL 'root' table (using a DB viewer).")
    print("2. Paste it below to fetch the FULL reconstructed object.")
    print("-" * 50)

    while True:
        mode = input("\nSelect Mode: (r)ead, (d)elete, (q)uit: ").lower().strip()
        
        if mode == 'q':
            break
            
        uuid_input = input("Enter Root UUID: ").strip()
        if not uuid_input: continue

        req = {}
        if mode == 'r':
            req = {"operation": "read", "root_id": uuid_input}
        elif mode == 'd':
            req = {"operation": "delete", "root_id": uuid_input}
        else:
            print("Unknown mode.")
            continue

        print(f"\nProcessing request: {json.dumps(req)} ...")
        
        # EXECUTE
        response = engine.handle_request(req)
        
        print("\n>>> RESPONSE:")
        print(json.dumps(response, indent=2, default=str)) # default=str handles datetime objects

if __name__ == "__main__":
    main()