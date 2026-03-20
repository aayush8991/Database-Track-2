import time
import json
import uuid
import threading
from datetime import datetime
from core.normalizer import Normalizer

class Router:
    def __init__(self, sql_handler, mongo_handler, analyzer=None):
        self.sql_handler = sql_handler
        self.mongo_handler = mongo_handler
        self.analyzer = analyzer
        self.previous_decisions = {}
        self.normalizer = Normalizer()
        self.lock = threading.Lock()

    def process_batch(self, batch, schema_decisions):
        """Routes batch to appropriate storage based on complexity."""
        # 1. Check for Schema Migration
        self._check_and_migrate(schema_decisions)
        
        with self.lock:
            self.previous_decisions.update(schema_decisions)

        # 2. Detect if batch needs SQL Normalization
        is_complex = False
        if batch:
            sample = batch[0]
            for v in sample.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    is_complex = True
                    break

        if is_complex:
            self._process_normalized_batch(batch)
        else:
            self._process_flat_batch(batch, schema_decisions)

    def _process_normalized_batch(self, batch):
        """Splits complex nested JSON into SQL tables."""
        aggregated_tables = {}
        
        for record in batch:
            # Shred logic returns { 'root': [row], 'root_orders': [rows] }
            shredded = self.normalizer.shred_record(record)
            
            # Merge into batch aggregations
            for table, rows in shredded.items():
                if table not in aggregated_tables:
                    aggregated_tables[table] = []
                aggregated_tables[table].extend(rows)
        
        # Insert into SQL using the dynamic handler
        self.sql_handler.insert_normalized_batch(aggregated_tables)
         
    def _process_flat_batch(self, batch, schema_decisions):
        """
        Handles flat records + Document Decomposition for MongoDB.
        Strategy: If field > 10% of total doc size -> Move to separate collection.
        """
        sql_inserts = []
        mongo_payloads = {"unstructed_data": []}

        for record in batch:
            sql_rec = {}
            mongo_rec = {}

            # Ensure UUID for consistent linking
            rec_uuid = record.get('uuid') or str(uuid.uuid4())
            record['uuid'] = rec_uuid

            common_keys = ['username', 'timestamp', 'sys_ingested_at', 'uuid']
            for k in common_keys:
                if k in record:
                    sql_rec[k] = record[k]
                    mongo_rec[k] = record[k]

            # Split fields based on routing decision
            for key, value in record.items():
                if key in common_keys:
                    continue

                decision = schema_decisions.get(key, {"target": "MONGO"})
                target = decision['target']

                if target == 'SQL':
                    sql_rec[key] = value
                elif target == 'MONGO':
                    mongo_rec[key] = value
                elif target == 'BOTH':
                    sql_rec[key] = value
                    mongo_rec[key] = value

            # --- DECOMPOSITION STRATEGY (10% Rule) ---
            try:
                # 1. Serialize to check total size
                doc_str = json.dumps(mongo_rec, default=str)
                total_size = len(doc_str)

                # Only decompose if document is substantial (> 1KB)
                if total_size > 1024:
                    keys_to_move = []

                    for k, v in list(mongo_rec.items()):
                        if k in common_keys:
                            continue

                        # Calculate Field Size
                        field_size = len(json.dumps(v, default=str))

                        # RULE: If field is > 10% of total size
                        if field_size > (total_size * 0.10):
                            target_coll = f"decomposed_{k}"
                            if target_coll not in mongo_payloads:
                                mongo_payloads[target_coll] = []

                            # Create linked document
                            child_payload = {
                                "parent_uuid": rec_uuid,
                                "data": v,
                                "created_at": datetime.now().isoformat()
                            }
                            mongo_payloads[target_coll].append(child_payload)

                            keys_to_move.append((k, target_coll))

                    # Replace migrated fields with References in Main Doc
                    for k, coll in keys_to_move:
                        mongo_rec[k] = f"REF::MONGO::{coll}::{rec_uuid}"

            except Exception as e:
                print(f"[Router] Decomposition Calc Error: {e}")

            if sql_rec:
                sql_inserts.append(sql_rec)

            mongo_payloads["unstructed_data"].append(mongo_rec)

        # Bulk SQL Insert
        if sql_inserts:
            self.sql_handler.insert_batch(sql_inserts)

        # Bulk Mongo Insert (Multi-collection)
        for coll, docs in mongo_payloads.items():
            if not docs:
                continue

            try:
                if self.mongo_handler is None or self.mongo_handler.db is None:
                    continue
                self.mongo_handler.insert_batch(coll, docs)
            except Exception as e:
                print(f"[Router] Mongo Insert Error ({coll}): {type(e).__name__}: {e}")

    def _check_and_migrate(self, new_decisions):
        """Detects schema drift and migrates if needed."""
        for field, decision in new_decisions.items():
            new_target = decision.get('target', 'MONGO')
            
            if field not in self.previous_decisions:
                continue

            old_target = self.previous_decisions[field].get('target', 'MONGO')

            if old_target == 'SQL' and new_target == 'MONGO':
                print(f"[Router] MIGRATION: '{field}' drifted from SQL to MongoDB.")
                # Migration logic would go here

    def _migrate_sql_to_mongo(self, field):
        """Migrates data from SQL to MongoDB."""
        pass

    def export_decisions(self):
        """Export previous decisions for persistence."""
        import copy
        with self.lock:
            return copy.deepcopy(self.previous_decisions)

    def load_decisions(self, decisions):
        """Restore previous decisions from persisted metadata."""
        import copy
        if decisions:
            with self.lock:
                self.previous_decisions = copy.deepcopy(decisions)