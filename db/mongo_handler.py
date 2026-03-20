from dotenv import load_dotenv

load_dotenv()

import os
from pymongo import MongoClient

class MongoHandler:
    def __init__(self):
        # Load Config
        self.uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("MONGO_DB_NAME", "adaptive_db")
        
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=2000)
            self.db = self.client[self.db_name]
            # Test connection
            self.client.server_info()
            print(f"[Mongo] Connected to {self.db_name}")
        except Exception as e:
            print(f"[Mongo] Connection Failed: {e}")
            self.db = None

    def insert_batch(self, collection_name_or_records, records=None):
        """
        Flexible signature to handle:
        - insert_batch(records) - backward compatible
        - insert_batch(collection_name, records) - new style
        """
        if not self.db:
            return

        # Determine which signature is being used
        if records is None:
            # Old signature: insert_batch(records)
            collection_name = "raw_data"
            records = collection_name_or_records
        else:
            # New signature: insert_batch(collection_name, records)
            collection_name = collection_name_or_records
        
        if not records:
            return

        try:
            coll = self.db[collection_name]
            coll.insert_many(records)
            print(f"[Mongo] Inserted {len(records)} into {collection_name}")
        except Exception as e:
            print(f"[Mongo] Batch Insert Error on {collection_name}: {e}")

    def insert_record(self, collection_name, record):
        if not self.db: return
        try:
            self.db[collection_name].insert_one(record)
        except Exception as e:
            print(f"[Mongo] Insert Error: {e}")