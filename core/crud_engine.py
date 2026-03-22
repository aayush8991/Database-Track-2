import json
from sqlalchemy import text
from datetime import datetime
from pymongo.errors import PyMongoError
import uuid as uuid_lib
from core.reference_resolver import ReferenceResolver
from core.performance_monitor import track_performance

class CRUDEngine:
    def __init__(self, sql_handler, mongo_handler, metadata_manager):
        self.sql = sql_handler
        self.mongo = mongo_handler
        self.meta = metadata_manager
        self.field_routing = self.meta.global_schema.get("field_routing", {})
        self.mongo_structure = self.meta.global_schema.get("mongo_structure", {})
        self.ref_resolver = ReferenceResolver(mongo_handler)
        self.generated_queries = {}  # Track queries for exposure
        self._ensure_root_table_exists()

    def _ensure_root_table_exists(self):
        """Ensure the root table exists, create if not."""
        try:
            with self.sql.engine.connect() as conn:
                # Check if root table exists
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = DATABASE() AND table_name = 'root'
                """))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    # Create root table with basic schema
                    conn.execute(text("""
                        CREATE TABLE `root` (
                            `uuid` VARCHAR(36) PRIMARY KEY,
                            `username` VARCHAR(255),
                            `email` VARCHAR(255),
                            `age` INT,
                            `timestamp` DATETIME,
                            `sys_ingested_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_username (username),
                            INDEX idx_email (email)
                        )
                    """))
                    conn.commit()
                    print("[CRUDEngine] Created root table")
        except Exception as e:
            print(f"[CRUDEngine] Warning: Could not ensure root table: {e}")

    def _ensure_columns_exist(self, table_name, columns):
        """Ensure all columns exist in the table, create if not."""
        try:
            with self.sql.engine.connect() as conn:
                # Get existing columns
                result = conn.execute(text(f"DESCRIBE `{table_name}`"))
                existing = {row[0] for row in result.fetchall()}
                
                # Add missing columns
                for col in columns:
                    if col not in existing:
                        # Infer type based on value (simple heuristic)
                        col_type = "TEXT"
                        alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {col_type}"
                        conn.execute(text(alter_sql))
                        print(f"[CRUDEngine] Added column '{col}' to table '{table_name}'")
                
                conn.commit()
        except Exception as e:
            print(f"[CRUDEngine] Warning: Could not ensure columns: {e}")

    def _validate_record_against_schema(self, data):
        """Validate data against current schema metadata."""
        self._refresh_routing()
        
        errors = []
        
        for field, value in data.items():
            # Check if field is known (warning, not error)
            if field not in self.field_routing and field != "uuid":
                pass  # Allow unknown fields but log
            
            # Type validation basic check
            if value is not None and field != "uuid":
                routing = self.field_routing.get(field, {})
                if isinstance(routing, dict):
                    expected_type = routing.get("expected_type", None)
                    is_unique = routing.get("is_unique", False)
                    
                    # Uniqueness validation (basic check against existing data)
                    if is_unique and value is not None:
                        try:
                            existing = self._fetch_sql_rows("root", field, str(value))
                            if existing:
                                errors.append(f"Field '{field}' must be unique, value already exists")
                        except:
                            pass  # If query fails, continue
        
        return errors

    def _refresh_routing(self):
        """Refresh field routing from metadata (for schema evolution)."""
        self.field_routing = self.meta.global_schema.get("field_routing", {})
        self.mongo_structure = self.meta.global_schema.get("mongo_structure", {})

    def _build_sql_where_clause(self, filter_dict):
        """
        Build SQL WHERE clause from filter dictionary.
        
        Supports:
        - {"field": "value"} → field = 'value'
        - {"field": {"$eq": "value"}} → field = 'value'
        - {"field": {"$gt": 10}} → field > 10
        - {"field": {"$in": [1,2,3]}} → field IN (1,2,3)
        - {"field1": "val1", "field2": "val2"} → field1 = 'val1' AND field2 = 'val2'
        """
        conditions = []
        params = {}
        
        for field, value in filter_dict.items():
            clean_field = field.replace("`", "").replace("'", "")  # Sanitize
            
            if isinstance(value, dict):
                # Operator format: {"$gt": 10}, {"$in": [...]}
                for op, op_value in value.items():
                    if op == "$eq":
                        conditions.append(f"`{clean_field}` = :{field}_eq")
                        params[f"{field}_eq"] = op_value
                    elif op == "$gt":
                        conditions.append(f"`{clean_field}` > :{field}_gt")
                        params[f"{field}_gt"] = op_value
                    elif op == "$gte":
                        conditions.append(f"`{clean_field}` >= :{field}_gte")
                        params[f"{field}_gte"] = op_value
                    elif op == "$lt":
                        conditions.append(f"`{clean_field}` < :{field}_lt")
                        params[f"{field}_lt"] = op_value
                    elif op == "$lte":
                        conditions.append(f"`{clean_field}` <= :{field}_lte")
                        params[f"{field}_lte"] = op_value
                    elif op == "$ne":
                        conditions.append(f"`{clean_field}` != :{field}_ne")
                        params[f"{field}_ne"] = op_value
                    elif op == "$in":
                        if not isinstance(op_value, list):
                            op_value = [op_value]
                        placeholders = ", ".join([f":{field}_in_{i}" for i in range(len(op_value))])
                        conditions.append(f"`{clean_field}` IN ({placeholders})")
                        for i, v in enumerate(op_value):
                            params[f"{field}_in_{i}"] = v
            else:
                # Simple equality
                conditions.append(f"`{clean_field}` = :{field}")
                params[field] = value
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params

    def handle_request(self, request_json):
        """
        Entry point for JSON CRUD requests.
        Supports:
          - { "operation": "read", "root_id": "uuid-123..." }
          - { "operation": "read", "filter": {"username": "user2"} }
          - { "operation": "list", "field": "username" }
        """
        op = request_json.get("operation", "").lower()
        if op == "read":
            return self._execute_read(request_json)
        elif op == "insert":
            return self._execute_insert(request_json)
        elif op == "delete":
            return self._execute_delete(request_json)
        elif op == "update":
            return self._execute_update(request_json)
        elif op == "list":
            return self._execute_list(request_json)
        else:
            return {"status": "error", "message": f"Operation '{op}' not supported"}
    
    def handle_request_with_plan(self, request_json: dict, expose_plan: bool = True) -> dict:
        """
        Handle CRUD request and optionally expose the query plan.
        
        Supports field projection and includes query plan in response.
        """
        self.generated_queries = {}  # Reset for this request
        expose = request_json.get("include_plan", expose_plan)
        
        op = request_json.get("operation", "").lower()
        start_time = datetime.now()
        
        # Delegate to appropriate handler
        result = self.handle_request(request_json)
        
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Add query plan if requested
        if expose:
            result["query_plan"] = {
                "operation": op,
                "queries": self.generated_queries,
                "execution_time_ms": round(execution_time, 2)
            }
        
        return result
    
    @track_performance("crud_insert")
    def _execute_insert(self, request):
        """Insert a new record, split into SQL/Mongo based on metadata routing."""
        data = request.get("data")
        if not data or not isinstance(data, dict):
            return {"status": "error", "message": "Missing or invalid 'data' for insert."}
        
        # Validate against schema
        validation_errors = self._validate_record_against_schema(data)
        if validation_errors:
            return {
                "status": "error",
                "message": "Schema validation failed",
                "errors": validation_errors
            }
        
        self._refresh_routing()
        record_uuid = data.get("uuid") or str(uuid_lib.uuid4())
        data["uuid"] = record_uuid
        sql_data = {}
        mongo_data = {"uuid": record_uuid}
        try:
            # Split data based on field routing from metadata
            for field, value in data.items():
                if field == "uuid":
                    sql_data["uuid"] = value
                    continue
                
                routing = self.field_routing.get(field, {})
                # Handle both dict and string routing formats
                if isinstance(routing, dict):
                    target = routing.get("target", "sql").lower()
                else:
                    target = str(routing).lower()
                
                if target in ["sql", "both"]:
                    sql_data[field] = value
                if target in ["mongo", "both"]:
                    mongo_data[field] = value
            
            # Insert into SQL if data present
            if sql_data:
                # Ensure all columns exist before insert
                self._ensure_columns_exist("root", sql_data.keys())
                
                cols = ', '.join(f'`{k}`' for k in sql_data.keys())
                vals = ', '.join([f':{k}' for k in sql_data.keys()])
                with self.sql.engine.connect() as conn:
                    conn.execute(text(f"INSERT INTO `root` ({cols}) VALUES ({vals})"), sql_data)
                    conn.commit()
            
            # Insert into MongoDB if data present
            if mongo_data:
                self.mongo.db["unstructured_data"].insert_one(mongo_data)
            
            return {"status": "success", "message": "Inserted record.", "uuid": record_uuid}
        except Exception as e:
            return {"status": "error", "message": f"Insert failed: {str(e)}"}

    @track_performance("crud_update")
    def _execute_update(self, request):
        """Update record(s) by filter, split into SQL/Mongo based on metadata routing."""
        filter_dict = request.get("filter")
        update_data = request.get("data")
        if not filter_dict or not isinstance(filter_dict, dict) or not update_data:
            return {"status": "error", "message": "Missing or invalid 'filter' or 'data' for update."}
        if len(filter_dict) != 1:
            return {"status": "error", "message": "Only single-field filter supported for update."}
        field, value = next(iter(filter_dict.items()))
        self._refresh_routing()
        try:
            # Split update data by metadata routing
            sql_updates = {}
            mongo_updates = {}
            for k, v in update_data.items():
                routing = self.field_routing.get(k, {})
                # Handle both dict and string routing formats
                target = routing.get("target", "sql") if isinstance(routing, dict) else routing
                if target.lower() in ["sql", "both"]:
                    sql_updates[k] = v
                if target.lower() in ["mongo", "both"]:
                    mongo_updates[k] = v
            
            # Update SQL records
            updated_count = 0
            if sql_updates:
                set_clause = ', '.join([f'`{k}` = :{k}' for k in sql_updates.keys()])
                params = dict(sql_updates)
                params['v'] = value
                with self.sql.engine.connect() as conn:
                    res = conn.execute(text(f"UPDATE `root` SET {set_clause} WHERE `{field}` = :v"), params)
                    conn.commit()
                    updated_count = res.rowcount
            
            # Update MongoDB documents (fix typo: unstructed -> unstructured)
            if mongo_updates:
                self.mongo.db["unstructured_data"].update_many({field: value}, {"$set": mongo_updates})
            
            return {"status": "success", "message": f"Updated {updated_count} record(s) in SQL, MongoDB updated."}
        except Exception as e:
            return {"status": "error", "message": f"Update failed: {str(e)}"}

    @track_performance("crud_read")
    def _execute_read(self, request):
        """Read by UUID or filter, reconstruct complete document from SQL/MongoDB."""
        target_id = request.get("root_id")
        filter_dict = request.get("filter")
        structure = self.meta.global_schema.get("relational_structure", {}).get("tables", {})
        if not structure:
            return {"status": "error", "message": "Schema metadata not found. Ingest data first."}
        self._refresh_routing()
        try:
            if target_id:
                # Fetch from SQL (primary normalized source)
                root_data = self._fetch_sql_row("root", "uuid", target_id)
                if not root_data:
                    return {"status": "404", "message": "Record not found"}
                # Enrich with MongoDB fields (handles decomposed collections)
                try:
                    mongo_doc = self.mongo.db["unstructured_data"].find_one({"uuid": target_id})
                    if mongo_doc:
                        mongo_doc.pop("_id", None)  # Remove MongoDB internal ID
                        # RESOLVE REFERENCES BEFORE MERGING
                        mongo_doc = self.ref_resolver.resolve_all_references(mongo_doc)
                        root_data.update(mongo_doc)
                except PyMongoError:
                    pass  # MongoDB may not have this record, continue
                # Fetch and reconstruct normalized child tables
                children_tables = structure.get("root", {}).get("children", [])
                for child_table in children_tables:
                    fk_col = "root_id"
                    child_rows = self._fetch_sql_rows(child_table, fk_col, target_id)
                    simple_key = child_table.replace("root_", "")
                    root_data[simple_key] = child_rows
                return {"status": "success", "data": root_data}
            elif filter_dict:
                if not isinstance(filter_dict, dict):
                    return {"status": "error", "message": "Filter must be a dictionary"}
                
                # Support complex filters
                where_clause, params = self._build_sql_where_clause(filter_dict)
                try:
                    with self.sql.engine.connect() as conn:
                        query = f"SELECT * FROM `root` WHERE {where_clause}"
                        result = conn.execute(text(query), params).fetchall()
                        results = [dict(row._mapping) for row in result]
                except Exception as e:
                    return {"status": "error", "message": f"Read failed: {str(e)}"}
                
                if not results:
                    return {"status": "404", "message": f"No records found for filter"}
                
                # Enrich each result with MongoDB data
                for row in results:
                    try:
                        mongo_doc = self.mongo.db["unstructured_data"].find_one({"uuid": row["uuid"]})
                        if mongo_doc:
                            mongo_doc.pop("_id", None)
                            # RESOLVE REFERENCES BEFORE MERGING
                            mongo_doc = self.ref_resolver.resolve_all_references(mongo_doc)
                            row.update(mongo_doc)
                    except PyMongoError:
                        pass
                # Fetch children for each result
                children_tables = structure.get("root", {}).get("children", [])
                for row in results:
                    for child_table in children_tables:
                        fk_col = "root_id"
                        child_rows = self._fetch_sql_rows(child_table, fk_col, row["uuid"])
                        simple_key = child_table.replace("root_", "")
                        row[simple_key] = child_rows
                return {"status": "success", "data": results}
            else:
                return {"status": "error", "message": "Missing 'root_id' or 'filter' in read request."}
        except Exception as e:
            return {"status": "error", "message": f"Read failed: {str(e)}"}

    @track_performance("crud_list")
    def _execute_list(self, request):
        # List all values for a given field in the root table
        field = request.get("field")
        if not field:
            return {"status": "error", "message": "Missing 'field' in list request."}
        try:
            with self.sql.engine.connect() as conn:
                result = conn.execute(text(f"SELECT `{field}` FROM `root` LIMIT 100")).fetchall()
                values = [row[0] for row in result if row[0] is not None]
            return {"status": "success", "field": field, "values": values}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @track_performance("crud_delete")
    def _execute_delete(self, request):
        """Delete by UUID or filter, cascading across SQL and MongoDB."""
        target_id = request.get("root_id")
        filter_dict = request.get("filter")
        structure = self.meta.global_schema.get("relational_structure", {}).get("tables", {})
        children_tables = structure.get("root", {}).get("children", [])
        try:
            with self.sql.engine.connect() as conn:
                if target_id:
                    # Cascade delete from SQL
                    for child in children_tables:
                        fk_col = "root_id"
                        conn.execute(text(f"DELETE FROM `{child}` WHERE `{fk_col}` = :uid"), {"uid": target_id})
                    res = conn.execute(text(f"DELETE FROM `root` WHERE uuid = :uid"), {"uid": target_id})
                    conn.commit()
                    # Cascade delete from MongoDB
                    try:
                        self.mongo.db["unstructed_data"].delete_one({"uuid": target_id})
                    except PyMongoError:
                        pass
                    if res.rowcount > 0:
                        return {"status": "success", "message": f"Deleted record {target_id}"}
                    else:
                        return {"status": "404", "message": "ID not found"}
                elif filter_dict and isinstance(filter_dict, dict) and len(filter_dict) == 1:
                    field, value = next(iter(filter_dict.items()))
                    # Find all matching uuids
                    uuids = [row['uuid'] for row in self._fetch_sql_rows("root", field, value)]
                    deleted = 0
                    for uid in uuids:
                        # Delete children (cascade)
                        for child in children_tables:
                            fk_col = "root_id"
                            conn.execute(text(f"DELETE FROM `{child}` WHERE `{fk_col}` = :uid"), {"uid": uid})
                        # Delete root
                        res = conn.execute(text(f"DELETE FROM `root` WHERE uuid = :uid"), {"uid": uid})
                        deleted += res.rowcount
                    conn.commit()
                    # Delete from MongoDB
                    try:
                        self.mongo.db["unstructed_data"].delete_many({field: value})
                    except PyMongoError:
                        pass
                    return {"status": "success", "message": f"Deleted {deleted} record(s) for {field}={value}"}
                else:
                    return {"status": "error", "message": "Missing 'root_id' or valid 'filter' for delete."}
        except Exception as e:
            return {"status": "error", "message": f"Delete failed: {str(e)}"}

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
    
    def _project_fields(self, record: dict, fields: list) -> dict:
        """Project only requested fields from a record."""
        if not fields:
            return record
        
        projected = {}
        for field in fields:
            if field in record:
                projected[field] = record[field]
        
        return projected
    
    def _log_query(self, query_type: str, query: str):
        """Log generated query for exposure."""
        if query_type not in self.generated_queries:
            self.generated_queries[query_type] = []
        self.generated_queries[query_type].append(query)
