"""Analyzes field statistics from incoming data."""
import copy
import threading
from datetime import datetime

class Analyzer:
    def __init__(self):
        self.field_stats = {}
        # Track structural relationships for Normalization
        self.structure_map = {
            "root": {"columns": set(), "children": []}
        }
        self.total_records_processed = 0
        self.lock = threading.Lock()

    def analyze_batch(self, batch):
        if not batch:
            return

        with self.lock:
            self.total_records_processed += len(batch)

            for record in batch:
                # 1. Existing Stat Analysis
                self._analyze_field_stats(record)
                
                # 2. Structure Analysis (Heuristic based)
                self._analyze_structure(record, "root")

    def _analyze_field_stats(self, record):
        """Analyzes field statistics."""
        for key, value in record.items():
            if key not in self.field_stats:
                self.field_stats[key] = {
                    "count": 0, 
                    "types": set(), 
                    "is_nested": False,
                    "unique_values": set(), 
                    "unique_capped_at": 0,
                }

            self.field_stats[key]["count"] += 1
            self.field_stats[key]["types"].add(type(value).__name__)

            if isinstance(value, (dict, list)):
                self.field_stats[key]["is_nested"] = True
            else:
                if len(self.field_stats[key]["unique_values"]) < 1000:
                    try:
                        self.field_stats[key]["unique_values"].add(str(value))
                    except TypeError:
                        pass

    def _analyze_structure(self, record, current_table):
        """Builds relational map based on JSON structure Heuristics."""
        # NOTE: Called from within lock context, don't try to acquire lock again
        if current_table not in self.structure_map:
            self.structure_map[current_table] = {"columns": set(), "children": []}

        for key, value in record.items():
            # HEURISTIC 1: List of Objects -> Child Table (1:N)
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                child_table = f"{current_table}_{key}"
                
                # Register relationship
                if child_table not in self.structure_map[current_table]["children"]:
                    self.structure_map[current_table]["children"].append(child_table)
                
                # Recurse
                for item in value:
                    self._analyze_structure(item, child_table)

            # HEURISTIC 2: Nested Dict -> Flatten (1:1)
            elif isinstance(value, dict):
                for sub_k, sub_v in value.items():
                    self.structure_map[current_table]["columns"].add(f"{key}_{sub_k}")

            # HEURISTIC 3: Primitives -> Columns
            elif not isinstance(value, list):
                self.structure_map[current_table]["columns"].add(key)

    def get_structure_map(self):
        with self.lock:
            # Export clean serializable map
            export = copy.deepcopy(self.structure_map)
            for tbl, data in export.items():
                data["columns"] = list(data["columns"])
            return export

    def get_schema_stats(self):
        with self.lock:
            summary = {}
            for key, stats in self.field_stats.items():
                freq_ratio = 0.0
                if self.total_records_processed > 0:
                    freq_ratio = stats["count"] / self.total_records_processed

                unique_types = list(stats["types"])
                is_stable = (len(unique_types) == 1)
                detected_type = unique_types[0] if is_stable else "mixed"

                total_unique = len(stats["unique_values"])
                unique_ratio = (total_unique / stats["count"]) if stats["count"] > 0 else 0.0
                
                summary[key] = {
                    "frequency_ratio": freq_ratio,
                    "type_stability": "stable" if is_stable else "unstable",
                    "detected_type": detected_type,
                    "is_nested": stats["is_nested"],
                    "unique_ratio": unique_ratio,
                    "count": stats["count"]
                }
            return summary

    def export_stats(self):
        with self.lock:
            return {
                "field_stats": {
                    k: {
                        "count": v["count"],
                        "types": list(v["types"]),  # Convert set to list
                        "is_nested": v["is_nested"],
                        "unique_values": list(v["unique_values"]),  # Convert set to list
                        "unique_capped_at": v["unique_capped_at"]
                    }
                    for k, v in self.field_stats.items()
                },
                "total_records_processed": self.total_records_processed
            }

    def load_stats(self, loaded_data):
        with self.lock:
            if loaded_data and "field_stats" in loaded_data:
                for key, data in loaded_data["field_stats"].items():
                    self.field_stats[key] = {
                        "count": data.get("count", 0),
                        "types": set(data.get("types", [])),
                        "is_nested": data.get("is_nested", False),
                        "unique_values": set(data.get("unique_values", [])),
                        "unique_capped_at": data.get("unique_capped_at", 0),
                    }
            self.total_records_processed = loaded_data.get("total_records_processed", 0) if loaded_data else 0