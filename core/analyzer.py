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

            for record in batch:
                for key, value in record.items():
                    if key not in self.field_stats:
                        self.field_stats[key] = {
                            "count": 0,
                            "types": set(),  
                            "is_nested": False,
                            "unique_values": set(),
                            "base_unique_count": 0,
                            "_unique_capped": False,
                            "db": None
                        }

            self.field_stats[key]["count"] += 1
            self.field_stats[key]["types"].add(type(value).__name__)

            if isinstance(value, (dict, list)):
                self.field_stats[key]["is_nested"] = True
            else:
                # Only use temp set for counting, don't persist raw values
                if len(self.field_stats[key]["unique_values_set"]) < 100000:
                    self.field_stats[key]["unique_values_set"].add(str(value))
                
                # Keep only 5 sample values for documentation
                if len(self.field_stats[key]["sample_values"]) < 5:
                    sample = str(value)[:100]  # Truncate to 100 chars
                    if sample not in self.field_stats[key]["sample_values"]:
                        self.field_stats[key]["sample_values"].append(sample)

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
                    "unique_values_count": 0,
                    "unique_values_set": set(),  # Temporary set for calculation only
                    "sample_values": [],  # Max 5 sample values for documentation
                }
            
            table_schema[key]["count"] += 1
            table_schema[key]["types"].add(type(value).__name__)
            
            if isinstance(value, (dict, list)):
                table_schema[key]["is_nested"] = True
            else:
                # Only use temp set for counting, don't persist raw values
                if len(table_schema[key]["unique_values_set"]) < 100000:
                    table_schema[key]["unique_values_set"].add(str(value))
                
                # Keep only 5 sample values for documentation
                if len(table_schema[key]["sample_values"]) < 5:
                    sample = str(value)[:100]  # Truncate to 100 chars
                    if sample not in table_schema[key]["sample_values"]:
                        table_schema[key]["sample_values"].append(sample)

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
        Get schema statistics for persistence.
        IMPORTANT: Converts temp sets to ratios for JSON serialization.
        Does NOT include raw field values.
        
        Args:
            table_name: If provided, return stats for specific table. Otherwise return global stats.
        
        Returns:
            Dictionary of field statistics (JSON-serializable, no raw values)
        """
        with self.lock:
            if table_name:
                return self._process_table_stats(table_name)
            
            # Global stats - clean version without raw values
            summary = {}
            for key, stats in self.field_stats.items():
                freq_ratio = 0.0
                if self.total_records_processed > 0:
                    freq_ratio = stats["count"] / self.total_records_processed

                unique_types = list(stats["types"])
                is_stable = (len(unique_types) == 1)
                detected_type = unique_types[0] if is_stable else "mixed"

                # Use the temporary set to count unique values (then discard the set)
                total_unique = len(stats["unique_values_set"])
                unique_ratio = (total_unique / stats["count"]) if stats["count"] > 0 else 0.0
                
                summary[key] = {
                    "frequency_ratio": freq_ratio,
                    "type_stability": "stable" if is_stable else "unstable",
                    "detected_type": detected_type,
                    "is_nested": stats["is_nested"],
                    "unique_ratio": unique_ratio,
                    "unique_count": total_unique,
                    "count": stats["count"],
                    "sample_values": stats.get("sample_values", [])  # Max 5 sample values
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
            
            # Use the temporary set to count unique values
            total_unique = len(stats["unique_values_set"])
            unique_ratio = (total_unique / stats["count"]) if stats["count"] > 0 else 0.0
            
            summary[key] = {
                "frequency_ratio": freq_ratio,
                "type_stability": "stable" if is_stable else "unstable",
                "detected_type": detected_type,
                "is_nested": stats["is_nested"],
                "unique_ratio": unique_ratio,
                "unique_count": total_unique,
                "count": stats["count"],
                "sample_values": stats.get("sample_values", [])  # Max 5 sample values
            }
        
        return summary

    def export_stats(self):
        """
        Export stats for persistence - does NOT include raw values.
        """
        with self.lock:
            return {
                "field_stats": {
                    k: {
                        "count": v["count"],
                        "types": list(v["types"]),  # Convert set to list
                        "is_nested": v["is_nested"],
                        "sample_values": v.get("sample_values", [])  # Only sample values, no raw data
                    }
                    for k, v in self.field_stats.items()
                },
                "total_records_processed": self.total_records_processed
            }
            
            for key, stats in self.field_stats.items():
                export_stats = copy.deepcopy(stats)
                export_stats["types"] = list(export_stats["types"])
                
                total_unique = stats["base_unique_count"] + len(stats["unique_values"])
                export_stats["base_unique_count"] = total_unique
                
                if len(stats["unique_values"]) > 20:
                    export_stats["unique_values"] = []
                    if len(stats["unique_values"]) >= 1000:
                        stats["_unique_capped"] = True
                        export_stats["_unique_capped"] = True
                else:
                    export_stats["unique_values"] = [
                        v.isoformat() if isinstance(v, datetime) else v 
                        for v in stats["unique_values"]
                    ]
                
                export_stats["db"] = stats.get("db", None)
                export_data["field_stats"][key] = export_stats
                    
            return export_data

    def load_stats(self, loaded_data):
        """
        Restore stats from persisted data.
        Note: Temporary sets (unique_values_set) are NOT restored, they'll be rebuilt during ingestion.
        """
        with self.lock:
            self.field_stats = {}
            for key, stats in data_stats.items():
                self.field_stats[key] = stats
                stats["types"] = set(stats["types"])
                
                is_capped = stats.get("_unique_capped", False)
                restored_set = set(stats.get("unique_values", []))
                stats["unique_values"] = restored_set
                
                total_from_json = stats.get("base_unique_count", 0)
                
                if is_capped:
                    # If it was already capped, we trick the logic to keep it capped
                    if len(restored_set) < 1000:
                        stats["unique_values"] = set(range(1000))
                    stats["base_unique_count"] = total_from_json - 1000
                else:
                    stats["base_unique_count"] = total_from_json - len(restored_set)

    def update_db_assignment(self, schema_decisions):
        """Update field_stats with db assignment from routing decisions."""
        with self.lock:
            for field, decision in schema_decisions.items():
                if field in self.field_stats:
                    # Get db from decision, fallback to target
                    db_assignment = decision.get('db', decision.get('target', 'MONGO'))
                    self.field_stats[field]["db"] = db_assignment
