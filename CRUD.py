#!/usr/bin/env python3
"""
CRUD.py - Command-line CRUD operations for hybrid SQL/MongoDB databases
Supports read, create, and delete commands based on schema_map.json metadata
"""

import json
import os
import sys
import mysql.connector
import pymongo
from datetime import datetime
from typing import Dict, List, Any, Set, Tuple
from dotenv import load_dotenv

load_dotenv()

class CRUDOperations:
    def __init__(self):
        """Initialize database handlers and load schema metadata"""
        self.schema_file = "metadata/schema_map.json"
        self.schema_map = self._load_schema()
        self.sql_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.output_file = "output.txt"
        self._setup_connections()
    
    def _load_schema(self) -> Dict:
        """Load schema_map.json to understand field storage locations"""
        if not os.path.exists(self.schema_file):
            raise FileNotFoundError(f"Schema file {self.schema_file} not found")
        
        with open(self.schema_file, 'r') as f:
            data = json.load(f)
        
        # Extract field stats from analyzer section
        return data.get("analyzer", {}).get("field_stats", {})
    
    def _setup_connections(self):
        """Establish connections to SQL and MongoDB"""
        try:
            # Setup SQL connection
            self.sql_config = {
                'host': os.getenv("SQL_HOST"),
                'port': int(os.getenv("SQL_PORT", 3306)),
                'user': os.getenv("SQL_USER"),
                'password': os.getenv("SQL_PASSWORD"),
                'database': os.getenv("SQL_DB_NAME")
            }
            
            self.sql_conn = mysql.connector.connect(**self.sql_config)
            print("[INFO] Connected to SQL database")
        except mysql.connector.Error as err:
            print(f"[WARNING] Could not connect to SQL database: {err}")
            self.sql_conn = None
        
        try:
            # Setup MongoDB connection
            mongo_uri = os.getenv("MONGO_URI")
            if mongo_uri:
                self.mongo_client = pymongo.MongoClient(mongo_uri)
                self.mongo_db = self.mongo_client[os.getenv("MONGO_DB_NAME", "adaptive_db")]
                print("[INFO] Connected to MongoDB")
            else:
                self.mongo_client = None
                self.mongo_db = None
        except Exception as err:
            print(f"[WARNING] Could not connect to MongoDB: {err}")
            self.mongo_client = None
            self.mongo_db = None
    
    def _get_field_storage_location(self, field: str) -> str:
        """
        Determine where a field is stored based on schema_map.json
        Returns: "SQL", "MONGO", or "BOTH"
        """
        if field not in self.schema_map:
            raise ValueError(f"Field '{field}' not found in schema")
        
        return self.schema_map[field].get("db", "BOTH")
    
    def _parse_read_command(self, command: str) -> List[str]:
        """
        Parse read command format: "read: field1, field2, field3"
        Returns list of field names
        """
        if not command.lower().startswith("read:"):
            raise ValueError("Invalid read command format. Use: read: field1, field2, ...")
        
        fields_str = command[5:].strip()  # Remove "read:" prefix
        fields = [f.strip() for f in fields_str.split(",") if f.strip()]
        
        if not fields:
            raise ValueError("No fields specified after 'read:'")
        
        # Validate fields exist in schema
        for field in fields:
            if field not in self.schema_map:
                raise ValueError(f"Field '{field}' not found in schema")
        
        return fields
    
    def _read_from_sql(self, fields: List[str]) -> List[Dict]:
        """Query requested fields from SQL database"""
        if self.sql_conn is None:
            return []
        
        try:
            cursor = self.sql_conn.cursor(dictionary=True)
            # Get available columns in SQL table
            cursor.execute("DESCRIBE structured_data")
            available_cols = {row['Field'] for row in cursor.fetchall()}
            
            # Filter fields that exist in SQL
            sql_fields = [f for f in fields if f in available_cols]
            
            if not sql_fields:
                return []
            
            query = f"SELECT {', '.join(sql_fields)} FROM structured_data LIMIT 1000"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return results
        except mysql.connector.Error as err:
            print(f"[ERROR] SQL Query Error: {err}")
            return []
    
    def _read_from_mongo(self, fields: List[str]) -> List[Dict]:
        """Query requested fields from MongoDB"""
        if self.mongo_client is None or self.mongo_db is None:
            return []
        
        try:
            collection = self.mongo_db["unstructured_data"]
            
            # Create projection for requested fields
            projection = {field: 1 for field in fields}
            projection['_id'] = 0  # Exclude MongoDB ID
            
            results = list(collection.find({}, projection).limit(1000))
            
            return results
        except Exception as err:
            print(f"[ERROR] MongoDB Query Error: {err}")
            return []
    
    def read(self, command: str) -> Dict[str, List[Dict]]:
        """
        Execute read command with specified fields
        Automatically routes to SQL, MongoDB, or both based on schema_map.json
        """
        try:
            fields = self._parse_read_command(command)
        except ValueError as e:
            print(f"[ERROR] {e}")
            return {}
        
        # Categorize fields by storage location
        sql_fields = []
        mongo_fields = []
        
        for field in fields:
            location = self._get_field_storage_location(field)
            if location in ["SQL", "BOTH"]:
                sql_fields.append(field)
            if location in ["MONGO", "BOTH"]:
                mongo_fields.append(field)
        
        results = {}
        
        # Query SQL if needed
        if sql_fields:
            print(f"[INFO] Querying SQL for fields: {', '.join(sql_fields)}")
            sql_results = self._read_from_sql(sql_fields)
            if sql_results:
                results['SQL'] = sql_results
                print(f"[INFO] Retrieved {len(sql_results)} records from SQL")
        
        # Query MongoDB if needed
        if mongo_fields:
            print(f"[INFO] Querying MongoDB for fields: {', '.join(mongo_fields)}")
            mongo_results = self._read_from_mongo(mongo_fields)
            if mongo_results:
                results['MONGO'] = mongo_results
                print(f"[INFO] Retrieved {len(mongo_results)} records from MongoDB")
        
        return results
    
    def _parse_create_command(self) -> Dict[str, Any]:
        """
        Read record data from stdin in JSON format
        Format: JSON string on a single line
        """
        print("[INFO] Enter data as JSON (single line):")
        try:
            json_input = input().strip()
            data = json.loads(json_input)
            
            if not isinstance(data, dict):
                raise ValueError("Input must be a JSON object")
            
            return data
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
    
    def _prepare_record_for_db(self, record: Dict) -> Tuple[Dict, Dict]:
        """
        Separate record into SQL and MongoDB parts based on schema_map
        Returns (sql_record, mongo_record)
        """
        sql_record = {}
        mongo_record = {}
        
        for field, value in record.items():
            if field not in self.schema_map:
                print(f"[WARNING] Field '{field}' not in schema, adding to MongoDB only")
                mongo_record[field] = value
                continue
            
            location = self._get_field_storage_location(field)
            
            if location in ["SQL", "BOTH"]:
                sql_record[field] = value
            if location in ["MONGO", "BOTH"]:
                mongo_record[field] = value
        
        return sql_record, mongo_record
    
    def _create_in_sql(self, record: Dict) -> bool:
        """Insert record into SQL database"""
        if self.sql_conn is None or not record:
            return False
        
        try:
            cursor = self.sql_conn.cursor()
            
            # Get available columns
            cursor.execute("DESCRIBE structured_data")
            available_cols = {row[0] for row in cursor.fetchall()}
            
            # Filter record to only include available columns
            filtered_record = {k: v for k, v in record.items() if k in available_cols}
            
            if not filtered_record:
                print("[WARNING] No valid fields for SQL insertion")
                return False
            
            columns = ', '.join(filtered_record.keys())
            placeholders = ', '.join(['%s'] * len(filtered_record))
            values = list(filtered_record.values())
            
            query = f"INSERT INTO structured_data ({columns}) VALUES ({placeholders})"
            cursor.execute(query, values)
            self.sql_conn.commit()
            cursor.close()
            
            print(f"[INFO] Successfully inserted record into SQL")
            return True
        except mysql.connector.Error as err:
            print(f"[ERROR] SQL Insert Error: {err}")
            return False
    
    def _create_in_mongo(self, record: Dict) -> bool:
        """Insert record into MongoDB"""
        if self.mongo_client is None or self.mongo_db is None or not record:
            return False
        
        try:
            collection = self.mongo_db["unstructured_data"]
            result = collection.insert_one(record)
            print(f"[INFO] Successfully inserted record into MongoDB (ID: {result.inserted_id})")
            return True
        except Exception as err:
            print(f"[ERROR] MongoDB Insert Error: {err}")
            return False
    
    def create(self) -> bool:
        """
        Create and insert new record(s) into appropriate databases
        Reads JSON data from stdin
        """
        try:
            record = self._parse_create_command()
        except ValueError as e:
            print(f"[ERROR] {e}")
            return False
        
        # Add metadata
        record['sys_ingested_at'] = datetime.now().isoformat()
        
        # Separate into SQL and MongoDB records
        sql_record, mongo_record = self._prepare_record_for_db(record)
        
        success = True
        
        # Insert into SQL if record has SQL fields
        if sql_record:
            success &= self._create_in_sql(sql_record)
        
        # Insert into MongoDB if record has MongoDB fields
        if mongo_record:
            success &= self._create_in_mongo(mongo_record)
        
        if not sql_record and not mongo_record:
            print("[ERROR] No valid fields to insert")
            return False
        
        return success
    
    def _parse_delete_command(self, command: str) -> Tuple[str, Any]:
        """
        Parse delete command format: "delete: field=value"
        Returns tuple of (field_name, value)
        """
        if not command.lower().startswith("delete:"):
            raise ValueError("Invalid delete command format. Use: delete: field=value")
        
        criteria_str = command[7:].strip()  # Remove "delete:" prefix
        
        if "=" not in criteria_str:
            raise ValueError("Invalid criteria format. Use: delete: field=value")
        
        field, value = criteria_str.split("=", 1)
        field = field.strip()
        value = value.strip()
        
        # Handle quoted values
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        
        # Try to convert to appropriate type
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.lower() == "null" or value.lower() == "none":
            value = None
        else:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass  # Keep as string
        
        # Validate field exists in schema
        if field not in self.schema_map:
            raise ValueError(f"Field '{field}' not found in schema")
        
        return field, value
    
    def _delete_from_sql(self, field: str, value: Any) -> int:
        """Delete records from SQL database matching criteria"""
        if self.sql_conn is None:
            return 0
        
        try:
            cursor = self.sql_conn.cursor()
            
            # Get available columns in SQL table
            cursor.execute("DESCRIBE structured_data")
            available_cols = {row[0] for row in cursor.fetchall()}
            
            # Check if field exists in SQL
            if field not in available_cols:
                return 0
            
            # Build safe delete query
            query = f"DELETE FROM structured_data WHERE {field} = %s"
            cursor.execute(query, (value,))
            self.sql_conn.commit()
            
            deleted_count = cursor.rowcount
            cursor.close()
            
            return deleted_count
        except mysql.connector.Error as err:
            print(f"[ERROR] SQL Delete Error: {err}")
            return 0
    
    def _delete_from_mongo(self, field: str, value: Any) -> int:
        """Delete records from MongoDB matching criteria"""
        if self.mongo_client is None or self.mongo_db is None:
            return 0
        
        try:
            collection = self.mongo_db["unstructured_data"]
            result = collection.delete_many({field: value})
            return result.deleted_count
        except Exception as err:
            print(f"[ERROR] MongoDB Delete Error: {err}")
            return 0
    
    def delete(self, command: str) -> bool:
        """
        Delete records matching criteria from appropriate databases
        """
        try:
            field, value = self._parse_delete_command(command)
        except ValueError as e:
            print(f"[ERROR] {e}")
            return False
        
        # Determine storage location
        location = self._get_field_storage_location(field)
        
        total_deleted = 0
        
        # Delete from SQL if needed
        if location in ["SQL", "BOTH"]:
            print(f"[INFO] Deleting from SQL where {field}={value}")
            sql_deleted = self._delete_from_sql(field, value)
            total_deleted += sql_deleted
            if sql_deleted > 0:
                print(f"[INFO] Deleted {sql_deleted} records from SQL")
        
        # Delete from MongoDB if needed
        if location in ["MONGO", "BOTH"]:
            print(f"[INFO] Deleting from MongoDB where {field}={value}")
            mongo_deleted = self._delete_from_mongo(field, value)
            total_deleted += mongo_deleted
            if mongo_deleted > 0:
                print(f"[INFO] Deleted {mongo_deleted} records from MongoDB")
        
        if total_deleted == 0:
            print(f"[WARNING] No records found matching {field}={value}")
        else:
            print(f"[INFO] Total deleted: {total_deleted} records")
        
        return True
    
    def _save_output(self, results: Dict[str, List[Dict]]):
        """Save results to output.txt file"""
        with open(self.output_file, 'w') as f:
            f.write(f"=== CRUD Operation Results ===\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"{'=' * 50}\n\n")
            
            if not results:
                f.write("No results found.\n")
                return
            
            for source, records in results.items():
                f.write(f"\n### {source} Database Results ###\n")
                f.write(f"Total Records: {len(records)}\n")
                f.write("-" * 50 + "\n")
                
                for idx, record in enumerate(records, 1):
                    f.write(f"\nRecord {idx}:\n")
                    for key, value in record.items():
                        f.write(f"  {key}: {value}\n")
            
            f.write(f"\n{'=' * 50}\n")
            f.write(f"Output saved at: {datetime.now().isoformat()}\n")
        
        print(f"[INFO] Results saved to {self.output_file}")
    
    def close(self):
        """Close all database connections"""
        if self.sql_conn is not None:
            self.sql_conn.close()
        if self.mongo_client is not None:
            self.mongo_client.close()


def display_schema():
    """Display available fields from schema_map.json"""
    crud = CRUDOperations()
    
    print("\n=== Available Fields ===")
    print(f"Total fields: {len(crud.schema_map)}\n")
    
    # Group by storage location
    sql_only = []
    mongo_only = []
    both = []
    
    for field, stats in crud.schema_map.items():
        location = stats.get("db", "BOTH")
        if location == "SQL":
            sql_only.append(field)
        elif location == "MONGO":
            mongo_only.append(field)
        else:
            both.append(field)
    
    print("SQL Only:")
    print(f"  {', '.join(sorted(sql_only))}\n")
    
    print("MongoDB Only:")
    print(f"  {', '.join(sorted(mongo_only))}\n")
    
    print("Both Databases:")
    print(f"  {', '.join(sorted(both))}\n")
    
    crud.close()


def main():
    """Main CLI interface"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python CRUD.py schema              - Display available fields")
        print("  python CRUD.py read: field1, field2, ...  - Read records")
        print("  python CRUD.py create              - Create new record (reads JSON from stdin)")
        print("  python CRUD.py delete: field=value - Delete records")
        print("\nExamples:")
        print("  python CRUD.py read: username, ip_address")
        print("  python CRUD.py read: device_model, spo2")
        print("  python CRUD.py create")
        print("  python CRUD.py delete: username=john_doe")
        print("  python CRUD.py delete: spo2=98")
        sys.exit(1)
    
    command = " ".join(sys.argv[1:])
    
    try:
        if command.lower() == "schema":
            display_schema()
        elif command.lower().startswith("read:"):
            crud = CRUDOperations()
            results = crud.read(command)
            crud._save_output(results)
            crud.close()
        elif command.lower() == "create":
            crud = CRUDOperations()
            success = crud.create()
            crud.close()
            if not success:
                sys.exit(1)
        elif command.lower().startswith("delete:"):
            crud = CRUDOperations()
            success = crud.delete(command)
            crud.close()
            if not success:
                sys.exit(1)
        else:
            print(f"[ERROR] Unknown command: {command}")
            sys.exit(1)
    
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
