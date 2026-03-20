import json
import os
import threading
import copy
from datetime import datetime

class MetadataManager:
    def __init__(self, filepath="metadata/schema_map.json"):
        self.filepath = filepath
        self.lock = threading.Lock()
        
        # The Core State of your System with versioning
        self.global_schema = {
            "version": 2.0,
            "schema_version": 1,  # Track schema evolution
            "last_updated": None,
            "relational_structure": {"tables": {}},  # WRAPPED in "tables" key
            "collection_structure": {},
            "field_routing": {},
            "field_stats": {},
            "schema_history": []  # Track schema changes over time
        }
        
        self.load_metadata()

    def load_metadata(self):
        """Loads existing schema from JSON on startup."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    # Merge loaded data safely
                    self.global_schema.update(data)
                print(f"[Metadata] Loaded schema from {self.filepath}")
            except Exception as e:
                print(f"[Metadata] Failed to load schema: {e}")
    
    def restore_analyzer_state(self, analyzer):
        """Restores analyzer state from persisted metadata."""
        analyzer_state = self.global_schema.get("analyzer", {})
        if analyzer_state:
            analyzer.load_stats(analyzer_state)

    def record_schema_change(self, change_description):
        """Record schema evolution for audit trail."""
        if "schema_history" not in self.global_schema:
            self.global_schema["schema_history"] = []
        self.global_schema["schema_history"].append({
            "timestamp": datetime.now().isoformat(),
            "change": change_description,
            "schema_version": self.global_schema.get("schema_version", 1)
        })
        self.global_schema["schema_version"] = self.global_schema.get("schema_version", 1) + 1
        self.global_schema["last_updated"] = datetime.now().isoformat()

    def save_metadata(self):
        """Persists current state to JSON with versioning."""
        with self.lock:
            try:
                self.global_schema["last_updated"] = datetime.now().isoformat()
                os.makedirs(os.path.dirname(self.filepath) or "metadata", exist_ok=True)
                with open(self.filepath, 'w') as f:
                    json.dump(self.global_schema, f, indent=4, default=str)
                print(f"[Metadata] Schema saved to {self.filepath}")
            except Exception as e:
                print(f"[Metadata] Save failed: {e}")

    def sync_analyzer(self, analyzer):
        """Pulls latest structure and stats from Analyzer."""
        struct = analyzer.get_structure_map()
        # IMPORTANT: Wrap in "tables" key for consistency with Query Engine
        self.global_schema["relational_structure"] = {"tables": struct}
        
        # Export field stats and analyzer state
        analyzer_export = analyzer.export_stats()
        self.global_schema["analyzer"] = analyzer_export
        self.global_schema["field_stats"] = analyzer.get_schema_stats()

    def sync_router(self, router):
        """Pulls latest routing decisions from Router."""
        self.global_schema["field_routing"] = router.export_decisions()

    def get_table_info(self, table_name):
        """Used by Query Engine to know which columns are in a table."""
        tables = self.global_schema.get("relational_structure", {}).get("tables", {})
        return tables.get(table_name, {})

    def get_field_route(self, field_name):
        """Used by Query Engine to know if a field is in SQL or Mongo."""
        return self.global_schema.get("field_routing", {}).get(field_name, {}).get("target", "UNKNOWN")