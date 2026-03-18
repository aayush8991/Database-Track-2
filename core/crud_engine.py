import json
from sqlalchemy import text
from datetime import datetime

class CRUDEngine:
    def __init__(self, sql_handler, mongo_handler, metadata_manager):
        self.sql = sql_handler
        self.mongo = mongo_handler
        self.meta = metadata_manager

    def handle_request(self, request_json):
        """
        Entry point for JSON CRUD requests.
        Example: { "operation": "read", "root_id": "uuid-123..." }
        """
        op = request_json.get("operation", "").lower()
        
        if op == "read":
            return self._execute_read(request_json)
        elif op == "delete":
            return self._execute_delete(request_json)
        else:
            return {"status": "error", "message": f"Operation '{op}' not supported"}

    def _execute_read(self, request):
        target_id = request.get("root_id")
        if not target_id:
            return {"status": "error", "message": "Missing 'root_id'"}

        # 1. Get Schema Structure from Metadata
        structure = self.meta.global_schema.get("relational_structure", {}).get("tables", {})
        
        if not structure:
            return {"status": "error", "message": "Schema metadata not found. Ingest data first."}
        
        # 2. Fetch Root Record (SQL)
        root_data = self._fetch_sql_row("root", "uuid", target_id)
        
        if not root_data:
            return {"status": "404", "message": "Record not found in SQL root table"}

        # 3. Dynamic Join: Fetch Children Tables
        children_tables = structure.get("root", {}).get("children", [])
        
        for child_table in children_tables:
            # FK column name: parent_table_id (e.g., "root_id" for children of root)
            fk_col = f"root_id"
            child_rows = self._fetch_sql_rows(child_table, fk_col, target_id)
            
            # Formatting: 'root_orders' -> 'orders' in the final JSON
            simple_key = child_table.replace("root_", "")
            root_data[simple_key] = child_rows

        return {"status": "success", "data": root_data}

    def _execute_delete(self, request):
        target_id = request.get("root_id")
        if not target_id:
            return {"status": "error", "message": "Missing 'root_id'"}

        structure = self.meta.global_schema.get("relational_structure", {}).get("tables", {})
        children_tables = structure.get("root", {}).get("children", [])
        
        try:
            with self.sql.engine.connect() as conn:
                # 1. Delete Children (Cascade)
                for child in children_tables:
                    fk_col = f"root_id"
                    conn.execute(text(f"DELETE FROM `{child}` WHERE `{fk_col}` = :uid"), {"uid": target_id})
                
                # 2. Delete Root
                res = conn.execute(text(f"DELETE FROM `root` WHERE uuid = :uid"), {"uid": target_id})
                conn.commit()
                
                if res.rowcount > 0:
                    return {"status": "success", "message": f"Deleted record {target_id}"}
                else:
                    return {"status": "404", "message": "ID not found"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # --- SQL Helpers ---
    def _fetch_sql_row(self, table, col, val):
        try:
            with self.sql.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{table}` WHERE `{col}` = :v"), {"v": val}).fetchone()
                if result:
                    return dict(result._mapping)
                return None
        except Exception as e:
            print(f"[CRUD] Error reading {table}: {e}")
            return None

    def _fetch_sql_rows(self, table, col, val):
        try:
            with self.sql.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{table}` WHERE `{col}` = :v"), {"v": val}).fetchall()
                return [dict(row._mapping) for row in result]
        except Exception:
            return []