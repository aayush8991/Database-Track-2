"""Normalizes raw JSON data from the API."""
import re
import uuid
from datetime import datetime

class Normalizer:
    def __init__(self):
        pass

    def _to_snake_case(self, key):
        """Helper to standardize column names"""
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def normalize_record(self, record):
        """Cleans keys of a flat record."""
        cleaned = {}
        for key, value in record.items():
            clean_key = self._to_snake_case(key)
            cleaned[clean_key] = value
        return cleaned

    def shred_record(self, record):
        """
        Splits a nested JSON record into relational rows.
        Returns: { 'table_name': [ {row_dict}, ... ] }
        """
        normalized_output = {}
        root_id = str(uuid.uuid4())
        
        # Start recursive splitting
        self._recursive_shred(record, "root", root_id, normalized_output, parent_table=None)
        
        return normalized_output

    def _recursive_shred(self, data, table_name, row_id, output, parent_table=None):
        """Recursively shred nested data into relational format."""
        if table_name not in output:
            output[table_name] = []

        row = {"uuid": row_id}
        
        # Link to parent if exists
        if parent_table:
            # FK column name: parent_table_id (e.g., "root_id", "root_orders_id")
            fk_col = f"{parent_table}_id"
            row[fk_col] = parent_table  # Store the parent's UUID reference
        
        for key, value in data.items():
            clean_key = self._to_snake_case(key)
            
            # RULE 1: List of Dicts -> Child Table
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                child_table = f"{table_name}_{clean_key}"
                for item in value:
                    child_id = str(uuid.uuid4())
                    self._recursive_shred(item, child_table, child_id, output, parent_table=table_name)

            # RULE 2: Nested Dict -> Flatten (1:1)
            elif isinstance(value, dict):
                for sub_k, sub_v in value.items():
                    flat_key = f"{clean_key}_{self._to_snake_case(sub_k)}"
                    row[flat_key] = str(sub_v)

            # RULE 3: Primitives -> Columns
            elif not isinstance(value, list):
                row[clean_key] = value

        # Add timestamp if missing
        if 'sys_ingested_at' not in row and table_name == 'root':
            row['sys_ingested_at'] = datetime.now()

        output[table_name].append(row)