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
        
        # NEW: Per-table statistics tracking
        self.table_stats = {}  # { "root": {...}, "root_orders": {...} }
        self.table_row_counts = {}  # Track row count per table

    def analyze_batch(self, batch, table_name=None):
        """
        Analyze a batch of records.
        
        Args:
            batch: List of records to analyze
            table_name: If provided, analyze per-table. Otherwise, use global analysis.
        """
        if not batch:
            return

        with self.lock:
            if table_name:
                # Per-table analysis
                self._analyze_batch_per_table(batch, table_name)
            else:
                # Global analysis (backward compatible)
                self.total_records_processed += len(batch)
                for record in batch:
                    self._analyze_field_stats(record)
                    self._analyze_structure(record, "root")
    
    def _analyze_batch_per_table(self, batch, table_name):
        """Analyze a batch for a specific table."""
        # Initialize table stats if needed
        if table_name not in self.table_stats:
            self.table_stats[table_name] = {}
        
        # Track row count
        if table_name not in self.table_row_counts:
            self.table_row_counts[table_name] = 0
        self.table_row_counts[table_name] += len(batch)
        
        # Analyze each record
        for record in batch:
            self._analyze_field_stats_for_table(record, table_name)

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

    def _analyze_field_stats_for_table(self, record, table_name):
        """Analyze field statistics for a specific table."""
        table_schema = self.table_stats[table_name]
        
        for key, value in record.items():
            # Skip uuid and system fields
            if key == "uuid":
                continue
            
            if key not in table_schema:
                table_schema[key] = {
                    "count": 0,
                    "types": set(),
                    "is_nested": False,
                    "unique_values": set(),
                    "unique_capped_at": 0,
                }
            
            table_schema[key]["count"] += 1
            table_schema[key]["types"].add(type(value).__name__)
            
            if isinstance(value, (dict, list)):
                table_schema[key]["is_nested"] = True
            else:
                if len(table_schema[key]["unique_values"]) < 1000:
                    try:
                        table_schema[key]["unique_values"].add(str(value))
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

    def get_schema_stats(self, table_name=None):
        """
        Get schema statistics.
        
        Args:
            table_name: If provided, return stats for specific table. Otherwise return global stats.
        
        Returns:
            Dictionary of field statistics
        """
        with self.lock:
            if table_name:
                return self._process_table_stats(table_name)
            
            # Old behavior - global stats (backward compatible)
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
    
    def _process_table_stats(self, table_name):
        """Process statistics for a specific table."""
        if table_name not in self.table_stats:
            return {}
        
        table_schema = self.table_stats[table_name]
        row_count = self.table_row_counts.get(table_name, 1)
        
        summary = {}
        for key, stats in table_schema.items():
            freq_ratio = 0.0
            if row_count > 0:
                freq_ratio = stats["count"] / row_count
            
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