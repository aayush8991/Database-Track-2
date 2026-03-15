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
            # Initialize with connection pooling
            self.client = MongoClient(
                self.uri, 
                serverSelectionTimeoutMS=2000,
                minPoolSize=5,           # Minimum pool size
                maxPoolSize=50,          # Maximum pool size
                maxIdleTimeMS=45000      # Close idle connections after 45s
            )
            self.db = self.client[self.db_name]
            # Test connection
            self.client.server_info()
            print(f"[Mongo] Connected to {self.db_name} with connection pooling (min=5, max=50)")
            
            # Create strategic indexes
            self._ensure_indexes()
        except Exception as e:
            print(f"[Mongo] Connection Failed: {e}")
            self.db = None
    
    def _ensure_indexes(self):
        """Create strategic indexes for common queries."""
        if self.db is None:
            return
        
        try:
            # unstructured_data collection
            self.db["unstructured_data"].create_index("uuid", unique=True)
            self.db["unstructured_data"].create_index("username")
            self.db["unstructured_data"].create_index("timestamp")
            print("[Mongo] Indexes created successfully")
        except Exception as e:
            pass  # Indexes may already exist

    def insert_batch(self, collection_name_or_records, records=None):
        """
        Flexible signature to handle:
        - insert_batch(records) - backward compatible
        - insert_batch(collection_name, records) - new style
        """
        if self.db is None:
            return

        # Determine which signature is being used
        if records is None:
            # Old signature: insert_batch(records)
            collection_name = "unstructed_data"
            records = collection_name_or_records
        else:
            # New signature: insert_batch(collection_name, records)
            collection_name = collection_name_or_records

        if not records:
            return

        try:
            coll = self.db[collection_name]
            result = coll.insert_many(records)
        except Exception as e:
            print(f"[Mongo] BATCH INSERT FAILED for {collection_name}: {type(e).__name__}: {e}")

        if valid_records:
            try:
                self.collection.insert_many(valid_records, ordered=False)
                print(f"[Mongo Handler] ✓ Successfully inserted {len(valid_records)} records")
            except pymongo.errors.BulkWriteError as bwe:
                print(f"[Mongo Handler] Bulk Write Error: {bwe.details}")
            except Exception as e:
                print(f"[Mongo Handler] Insert Error: {e}")

    def reset_db(self):
        """Drop all collections to reset database for testing."""
        if self.db is None:
            return
        
        try:
            # Get all collections
            collections = self.db.list_collection_names()
            
            # Drop each collection
            for collection in collections:
                self.db[collection].drop()
                print(f"[Mongo] Dropped collection: {collection}")
            
            print("[Mongo] Database reset complete")
        except Exception as e:
            print(f"[Mongo] Reset failed: {e}")

    def find(self, query, collection='unstructured_data', limit=None):
        """Find documents in a collection."""
        if self.db is None:
            return []
        
        try:
            coll = self.db[collection]
            if limit:
                return list(coll.find(query).limit(limit))
            else:
                return list(coll.find(query))
        except Exception as e:
            print(f"[Mongo] Find Error: {e}")
            return []