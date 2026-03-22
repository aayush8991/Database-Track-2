"""Normalizes raw JSON data from the API."""
import re
import uuid
from datetime import datetime
from collections import defaultdict

class Normalizer:
    def __init__(self):
        self.m2m_mappings = {}  # Track which entity↔value pairs already exist
        self.junction_tables = {}  # Track all created junction tables

    def _to_snake_case(self, key):
        """Helper to standardize column names"""
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def normalize_record(self, record):
        """Cleans keys of a flat record and adds sys_ingested_at timestamp."""
        cleaned = {}
        for key, value in record.items():
            clean_key = self._to_snake_case(key)
            cleaned[clean_key] = value
        
        # Add system ingestion timestamp if not already present
        if 'sys_ingested_at' not in cleaned:
            cleaned['sys_ingested_at'] = datetime.now()
        
        return cleaned

    def shred_record(self, record):
        """Cleans keys of a flat record (backward compatible)."""
        normalized_output = {}
        root_id = str(uuid.uuid4())
        # Start recursive splitting with depth=0
        self._recursive_shred(record, "root", root_id, normalized_output, parent_table=None, parent_id=None, depth=0)
        return normalized_output
    
    def shred_record_with_m2m(self, record, entity_name="root"):
        """
        Shreds a record into normalized form, including M:N junction tables.
        
        Returns:
        {
            "root": [{"uuid": "...", "name": "...", "email": "..."}],
            "root_orders": [{"uuid": "...", "root_id": "...", "items": [...]}],
            "tags": [{"tag_id": "...", "tag_name": "python"}],  # Dimension table
            "root_tags": [  # Junction table
                {"root_id": "...", "tag_id": "..."},
                {"root_id": "...", "tag_id": "..."}
            ]
        }
        """
        normalized_output = {}
        root_id = str(uuid.uuid4())
        
        # First pass: recursive shred for 1:1 and 1:N relationships
        self._recursive_shred(record, entity_name, root_id, normalized_output, 
                             parent_table=None, parent_id=None, depth=0)
        
        # Second pass: handle M:N relationships
        self._extract_m2m_relationships(record, entity_name, root_id, normalized_output)
        
        return normalized_output

    def _recursive_shred(self, data, table_name, row_id, output, parent_table=None, parent_id=None, depth=0):
        """Recursively shred nested data into relational format.
        
        Now supports multiple levels of nesting (depth tracking).
        """
        if depth > 10:  # Prevent infinite recursion
            print(f"[Normalizer] Max nesting depth (10) exceeded at {table_name}")
            return
        
        if table_name not in output:
            output[table_name] = []

        row = {"uuid": row_id}

        # Link to parent if exists
        if parent_table and parent_id:
            fk_col = f"{parent_table}_id"
            row[fk_col] = parent_id  # Store the parent's UUID reference

        for key, value in data.items():
            clean_key = self._to_snake_case(key)

            # RULE 1: List of Dicts -> Child Table (1:N) - WITH RECURSION FOR DEPTH
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                child_table = f"{table_name}_{clean_key}"
                for item in value:
                    child_id = str(uuid.uuid4())
                    # RECURSE WITH DEPTH INCREMENT
                    self._recursive_shred(item, child_table, child_id, output, parent_table=table_name, parent_id=row_id, depth=depth+1)

            # RULE 1b: List of Primitives -> SKIP (handle as M:N separately)
            elif isinstance(value, list) and (len(value) == 0 or not isinstance(value[0], dict)):
                # Skip - will be handled by _extract_m2m_relationships
                pass

            # RULE 2: Nested Dict -> Flatten (1:1)
            elif isinstance(value, dict):
                for sub_k, sub_v in value.items():
                    flat_key = f"{clean_key}_{self._to_snake_case(sub_k)}"
                    # PRESERVE ORIGINAL TYPE - don't convert to string
                    row[flat_key] = sub_v

            # RULE 3: Primitives -> Columns
            else:
                row[clean_key] = value

        # Add timestamp if missing
        if 'sys_ingested_at' not in row and table_name == 'root':
            row['sys_ingested_at'] = datetime.now()

        output[table_name].append(row)
    
    def _extract_m2m_relationships(self, data, entity_name, entity_id, output):
        """
        Extract arrays of primitives and create M:N junction tables.
        
        Example:
        Input: {"tags": ["python", "database", "systems"]}
        Output creates:
        - "tags" table: [{"tag_id": uuid1, "tag_name": "python"}, ...]
        - "entity_tags" junction table: [{"entity_id": uuid, "tag_id": uuid1}, ...]
        """
        for key, value in data.items():
            clean_key = self._to_snake_case(key)
            
            # Check if this is an array of primitives (M:N relationship)
            if not isinstance(value, list) or len(value) == 0:
                continue
            
            # Skip if first element is dict (handled as 1:N)
            if isinstance(value[0], dict):
                continue
            
            # Skip if not primitive types
            if not isinstance(value[0], (str, int, float, bool)):
                continue
            
            # ✓ This is an M:N relationship!
            
            # Singularize the key (tags → tag)
            singular = clean_key.rstrip('s') if clean_key.endswith('s') else clean_key
            
            # Initialize tables if needed
            dimension_table = singular  # "tag"
            junction_table = f"{entity_name}_{clean_key}"  # "root_tags"
            
            if dimension_table not in output:
                output[dimension_table] = []
            if junction_table not in output:
                output[junction_table] = []
            
            # Track which dimension values we've already inserted
            if dimension_table not in self.m2m_mappings:
                self.m2m_mappings[dimension_table] = {}
            
            # Process each value in the array
            for primitive_value in value:
                # Create or retrieve dimension record
                dim_value_str = str(primitive_value)
                
                if dim_value_str not in self.m2m_mappings[dimension_table]:
                    # New dimension value - create record
                    dim_id = str(uuid.uuid4())
                    self.m2m_mappings[dimension_table][dim_value_str] = dim_id
                    
                    output[dimension_table].append({
                        f"{singular}_id": dim_id,
                        f"{singular}_name": primitive_value
                    })
                else:
                    # Reuse existing dimension ID
                    dim_id = self.m2m_mappings[dimension_table][dim_value_str]
                
                # Create junction record
                output[junction_table].append({
                    f"{entity_name}_id": entity_id,
                    f"{singular}_id": dim_id
                })
    
    def get_schema_for_normalized_data(self, normalized_data):
        """
        Generate SQL schema creation statements from normalized data.
        
        Returns:
        {
            "root": {
                "columns": {"uuid": "UUID", "name": "VARCHAR(255)"},
                "primary_key": "uuid",
                "foreign_keys": []
            },
            "tags": {
                "columns": {"tag_id": "UUID", "tag_name": "VARCHAR(255)"},
                "primary_key": "tag_id",
                "unique_constraints": [["tag_name"]]
            },
            "root_tags": {
                "columns": {"root_id": "UUID", "tag_id": "UUID"},
                "primary_key": ["root_id", "tag_id"],  # Composite key
                "foreign_keys": [...]
            }
        }
        """
        schema = {}
        
        for table_name, rows in normalized_data.items():
            if not rows:
                continue
            
            schema[table_name] = {
                "columns": {},
                "primary_key": None,
                "foreign_keys": [],
                "indexes": [],
                "unique_constraints": []
            }
            
            # Analyze first row to get column types
            sample_row = rows[0]
            for col_name, value in sample_row.items():
                # Detect type
                if col_name.endswith("_id") or col_name == "uuid":
                    col_type = "VARCHAR(36)"
                elif isinstance(value, bool):
                    col_type = "BOOLEAN"
                elif isinstance(value, int):
                    col_type = "BIGINT"
                elif isinstance(value, float):
                    col_type = "FLOAT"
                else:
                    col_type = "VARCHAR(255)"
                
                schema[table_name]["columns"][col_name] = col_type
            
            # Infer keys and relationships
            if "uuid" in sample_row:
                schema[table_name]["primary_key"] = "uuid"
            elif f"{table_name}_id" in sample_row:
                schema[table_name]["primary_key"] = f"{table_name}_id"
            elif "_id" in sample_row:
                # Composite key for junction tables
                id_columns = [k for k in sample_row.keys() if k.endswith("_id")]
                if len(id_columns) > 1:
                    schema[table_name]["primary_key"] = id_columns
            
            # Detect foreign keys
            for col_name in sample_row.keys():
                if col_name.endswith("_id") and col_name not in ["uuid", f"{table_name}_id"]:
                    # This looks like a foreign key
                    ref_table = col_name.replace("_id", "")
                    schema[table_name]["foreign_keys"].append({
                        "column": col_name,
                        "references": f"{ref_table}(uuid)"
                    })
            
            # Add indexes
            if isinstance(schema[table_name]["primary_key"], str):
                schema[table_name]["indexes"].append({
                    "type": "PRIMARY",
                    "columns": [schema[table_name]["primary_key"]]
                })
            
            # Foreign key columns should be indexed
            for fk in schema[table_name]["foreign_keys"]:
                schema[table_name]["indexes"].append({
                    "type": "INDEX",
                    "columns": [fk["column"]],
                    "name": f"idx_{table_name}_{fk['column']}"
                })
            
            # Dimension tables should have unique constraints on value columns
            if "_name" in sample_row and table_name != f"{table_name}_name":
                # This is likely a dimension table
                schema[table_name]["unique_constraints"].append(
                    [col for col in sample_row.keys() if col.endswith("_name")]
                )
        
        return schema
