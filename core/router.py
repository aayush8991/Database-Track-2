import time
import json
import uuid
import threading
from datetime import datetime
from core.normalizer import Normalizer
from core.normalization_advanced import AdvancedNormalizer

class Router:
    def __init__(self, sql_handler, mongo_handler, analyzer=None):
        self.sql_handler = sql_handler
        self.mongo_handler = mongo_handler
        self.analyzer = analyzer
        self.previous_decisions = {}
        self.normalizer = Normalizer()
        self.advanced_normalizer = AdvancedNormalizer()  # Add advanced normalizer
        self.normalization_reports = []  # Track reports
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
        
        # 3. VALIDATE NORMALIZATION (Advanced Analysis)
        self._validate_normalization(batch)

    def _process_normalized_batch(self, batch):
        """Splits complex nested JSON into SQL tables with M:N support."""
        aggregated_tables = {}
        
        for record in batch:
            # Use enhanced shredding with M:N junction table support
            shredded = self.normalizer.shred_record_with_m2m(record)
            
            # Merge into batch aggregations
            for table, rows in shredded.items():
                if table not in aggregated_tables:
                    aggregated_tables[table] = []
                aggregated_tables[table].extend(rows)
        
        # Get schema with proper constraints and indexes
        schema = self.normalizer.get_schema_for_normalized_data(aggregated_tables)
        
        # Create tables with indexes and constraints
        self.sql_handler.create_tables_from_schema(schema)
        
        # Insert into SQL using the dynamic handler
        self.sql_handler.insert_normalized_batch(aggregated_tables)
         
    def _process_flat_batch(self, batch, schema_decisions):
        """
        Handles flat records + Document Decomposition for MongoDB.
        Strategy: If field > 10% of total doc size -> Move to separate collection.
        """
        # 1. EVOLVE SQL SCHEMA FIRST - ensure all SQL-target columns exist
        self.sql_handler.update_schema(schema_decisions)
        
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
    
    def _validate_normalization(self, batch):
        """
        Validates batch against normalization theory using AdvancedNormalizer.
        Runs advanced normalization analysis on batch.
        """
        if not batch:
            return
        
        try:
            # Run advanced normalization analysis
            analysis = self.advanced_normalizer.analyze_data_structure(batch)
            
            # Get the normalization report
            report = self.advanced_normalizer.get_normalization_report()
            
            # Store report for later access
            with self.lock:
                self.normalization_reports.append({
                    'timestamp': datetime.now(),
                    'batch_size': len(batch),
                    'analysis': analysis,
                    'report': report
                })
            
            # Print key findings
            #print("\n" + "=" * 70)
            #print("[Router] ADVANCED NORMALIZATION ANALYSIS")
            #print("=" * 70)
            
            # # Repeating groups
            # if analysis.get('repeating_groups'):
            #     print(f"✓ Found {len(analysis['repeating_groups'])} repeating groups:")
            #     for rg in analysis['repeating_groups']:
            #         print(f"  - {rg['group_name']}: {rg['attributes']}")
            
            # # Nesting levels
            # if analysis.get('nesting_levels', {}).get('deepest_level', 0) > 0:
            #     print(f"✓ Nesting depth: {analysis['nesting_levels']['deepest_level']} levels")
            
            # # Functional dependencies
            # if analysis.get('functional_dependencies'):
            #     print(f"✓ Found {len(analysis['functional_dependencies'])} functional dependencies:")
            #     for fd in analysis['functional_dependencies'][:3]:  # Show first 3
            #         print(f"  - {fd['determinant']} → {fd['dependents']}")
            
            # # M:N relationships
            # if analysis.get('many_to_many'):
            #     print(f"✓ Found {len(analysis['many_to_many'])} M:N relationships (need junction tables)")
            
            # # Primary key strategy
            # pk_strategy = analysis.get('primary_key_strategy', {})
            # print(f"✓ Primary Key Strategy: {pk_strategy.get('primary_key')} ({pk_strategy.get('strategy')})")
            
            # print("=" * 70 + "\n")
            
        except Exception as e:
            print(f"[Router] Normalization validation error: {e}")
            import traceback
            traceback.print_exc()