# CRUD.py - Architecture and Integration Guide

## System Overview

CRUD.py integrates with the existing Database-Track-2 system to provide command-line CRUD operations across hybrid SQL/MongoDB databases. It leverages existing database handlers and schema metadata.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    CRUD.py (CLI Interface)                   │
│  - Read Commands: Query fields from SQL/MongoDB              │
│  - Create Commands: Insert new records intelligently         │
│  - Delete Commands: Remove records from SQL/MongoDB          │
└────────────┬────────────────────────────────────┬────────────┘
             │                                    │
             ▼                                    ▼
    ┌────────────────────┐           ┌──────────────────────┐
    │   SQL Handler      │           │  MongoDB Handler     │
    │ (mysql connector)  │           │  (pymongo)           │
    └────────┬───────────┘           └──────────┬───────────┘
             │                                  │
             ▼                                  ▼
    ┌────────────────────┐           ┌──────────────────────┐
    │  MySQL Database    │           │    MongoDB           │
    │ (structured_data)  │           │ (unstructured_data)  │
    └────────────────────┘           └──────────────────────┘

             ▲                                  ▲
             │                                  │
             └──────────────┬───────────────────┘
                            │
                   ┌─────────▬────────┐
                   │  Schema Metadata │
                   │ (schema_map.json)│
                   └──────────────────┘
```

## Component Integration

### 1. Schema Metadata Integration

**File**: `metadata/schema_map.json`

CRUD.py reads this file to determine field storage locations:

```json
{
    "analyzer": {
        "field_stats": {
            "username": {
                "count": 86950,
                "types": ["str"],
                "is_nested": false,
                "db": "BOTH"              // ← Storage location
            },
            "device_model": {
                "count": 74155,
                "types": ["str"],
                "db": "SQL"               // ← SQL only
            },
            "temperature_c": {
                "count": 52118,
                "types": ["float"],
                "db": "MONGO"             // ← MongoDB only
            }
        }
    }
}
```

**Key Fields**:
- `db`: Indicates storage location ("SQL", "MONGO", or "BOTH")
- `types`: Data type(s) for type conversion
- `count`: Number of records with this field

### 2. SQL Database Integration

**Integration Point**: `db/sql_handler.py`

CRUD.py directly uses MySQL connector (not reusing SQLHandler class):

```python
# Direct Connection
self.sql_config = {
    'host': os.getenv("SQL_HOST"),
    'port': int(os.getenv("SQL_PORT", 3306)),
    'user': os.getenv("SQL_USER"),
    'password': os.getenv("SQL_PASSWORD"),
    'database': os.getenv("SQL_DB_NAME")
}
self.sql_conn = mysql.connector.connect(**self.sql_config)

# Table: structured_data
# Standard columns: id (INT, PK, AI), username, timestamp, sys_ingested_at
# Dynamic columns: Added based on schema decisions
```

**Read Operations**:
```sql
SELECT field1, field2, ... FROM structured_data LIMIT 1000
```

**Create Operations**:
```sql
INSERT INTO structured_data (field1, field2, ...) VALUES (?, ?, ...)
```

### 3. MongoDB Integration

**Integration Point**: `db/mongo_handler.py`

CRUD.py directly uses pymongo:

```python
# Direct Connection
mongo_uri = os.getenv("MONGO_URI")
self.mongo_client = pymongo.MongoClient(mongo_uri)
self.mongo_db = self.mongo_client[os.getenv("MONGO_DB_NAME", "adaptive_db")]

# Collection: unstructured_data
# Flexible schema - stores all non-SQL fields
```

**Read Operations**:
```python
collection.find({}, projection).limit(1000)
```

**Create Operations**:
```python
collection.insert_one(record)
```

## Data Flow Diagrams

### Read Operation Flow

```
User Command: "read: username, ip_address, device_model"
    │
    ▼
Parse Fields & Validate Schema
    │
    ├─ username → db: "BOTH"
    ├─ ip_address → db: "MONGO"
    └─ device_model → db: "SQL"
    │
    ▼
Categorize by Storage Location
    │
    ├─ SQL Fields: [device_model]
    └─ MongoDB Fields: [username, ip_address]
    │
    ├──────────────────────┬──────────────────────┐
    ▼                      ▼
SQL Query:              MongoDB Query:
SELECT device_model     find({}, projection)
    │                      │
    ▼                      ▼
SQL Results            MongoDB Results
    │                      │
    └──────────────┬───────┘
                   │
                   ▼
           Combine Results
                   │
                   ▼
         Save to output.txt
```

### Create Operation Flow

```
User Input: {"username": "john", "device_model": "iPhone", "temperature_c": 37.5}
    │
    ▼
Parse JSON & Validate
    │
    ▼
Add System Metadata
("sys_ingested_at": "2026-03-19T...")
    │
    ▼
Map Fields to Databases
    │
    ├─ SQL Fields: {"username": "john", "device_model": "iPhone", "sys_ingested_at": "..."}
    └─ MongoDB Fields: {"username": "john", "temperature_c": 37.5, "sys_ingested_at": "..."}
    │
    ├─────────────────────┬────────────────────┐
    ▼                     ▼
Insert to SQL:        Insert to MongoDB:
INSERT INTO           insert_one(record)
structured_data       │
    │                 ▼
    ▼           Success/Failure
Success/Failure
    │                 │
    └────────┬────────┘
             │
             ▼
    Report Results
```

### Delete Operation Flow

```
User Command: "delete: username=john_doe"
    │
    ▼
Parse Field & Value
    │
    ▼
Validate Field in Schema
    │
    ▼
Look Up Storage Location
    │
    ├─ SQL Storage?
    │   │
    │   ├─Yes → Delete from SQL
    │   │        └─ DELETE FROM structured_data WHERE username='john_doe'
    │   │
    │   └─No (Skip SQL)
    │
    ├─ MongoDB Storage?
    │   │
    │   ├─Yes → Delete from MongoDB
    │   │        └─ delete_many({username: 'john_doe'})
    │   │
    │   └─No (Skip MongoDB)
    │
    ▼
Count Deleted Records
    │
    ├─ From SQL: N records
    └─ From MongoDB: M records
    │
    ▼
Report Total Deleted: N + M
```

## Environment Configuration

CRUD.py requires these environment variables (typically in `.env`):

```bash
# MySQL Configuration
SQL_HOST=localhost
SQL_PORT=3306
SQL_USER=root
SQL_PASSWORD=your_password
SQL_DB_NAME=adaptive_db

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=adaptive_db
```

## Class Structure

### CRUDOperations Class

```
CRUDOperations
├── __init__()
│   ├── Load schema_map.json
│   └── Establish database connections
│
├── _load_schema()
│   └── Parse metadata/schema_map.json
│
├── _setup_connections()
│   ├── Connect to MySQL
│   └── Connect to MongoDB
│
├── _get_field_storage_location(field: str) -> str
│   └── Look up "db" property in schema
│
├── READ Operations
│   ├── read(command: str) -> Dict
│   │   ├── Parse "read: field1, field2" format
│   │   ├── Categorize fields by storage location
│   │   ├── Query appropriate databases
│   │   └── Combine & save results
│   │
│   ├── _parse_read_command(command: str) -> List[str]
│   │   └── Extract field names from command
│   │
│   ├── _read_from_sql(fields: List) -> List[Dict]
│   │   └── Query SQL database
│   │
│   └── _read_from_mongo(fields: List) -> List[Dict]
│       └── Query MongoDB
│
├── CREATE Operations
│   ├── create() -> bool
│   │   ├── Read JSON from stdin
│   │   ├── Separate into SQL/MongoDB records
│   │   └── Insert to both databases
│   │
│   ├── _parse_create_command() -> Dict
│   │   └── Read JSON from stdin
│   │
│   ├── _prepare_record_for_db(record: Dict) -> Tuple[Dict, Dict]
│   │   └── Split record for SQL vs MongoDB
│   │
│   ├── _create_in_sql(record: Dict) -> bool
│   │   └── INSERT into structured_data
│   │
│   └── _create_in_mongo(record: Dict) -> bool
│       └── insert_one() into unstructured_data
│├── DELETE Operations
│   ├── delete(command: str) -> bool
│   │   ├── Parse "delete: field=value" format
│   │   ├── Look up field storage location
│   │   ├── Delete from appropriate databases
│   │   └── Report deletion counts
│   │
│   ├── _parse_delete_command(command: str) -> Tuple[str, Any]
│   │   └── Extract field name and value, handle type conversion
│   │
│   ├── _delete_from_sql(field: str, value: Any) -> int
│   │   └── DELETE from structured_data WHERE field=value
│   │
│   └── _delete_from_mongo(field: str, value: Any) -> int
│       └── delete_many() from unstructured_data
│├── Output Operations
│   ├── _save_output(results: Dict)
│   │   └── Write results to output.txt
│   │
│   └── close()
│       └── Close all DB connections
│
└── Utility Functions
    ├── display_schema()
    │   └── Print available fields
    │
    └── main()
        └── CLI entry point
```

## Field Storage Categories

Based on `schema_map.json` "db" property:

### SQL Only Fields (18 fields)
- Structured entities: device_model, spo2, purchase_value
- Optimized for relational queries

### MongoDB Only Fields (~50+ fields)
- Semi-structured data: Locations, sensor readings, user preferences
- Flexible schema for varied data types

### Both Databases (3 fields)
- Core identifiers: username, timestamp, sys_ingested_at
- Replicated for query performance across both databases

## Output Format

Results saved to `output.txt`:

```
=== CRUD Operation Results ===
Timestamp: 2026-03-19T14:30:45.123456

### SQL Database Results ###
Total Records: 100
--------------------------------------------------
Record 1:
  device_model: iPhone 14
  spo2: 98

Record 2:
  device_model: Samsung S23
  spo2: 97

### MONGO Database Results ###
Total Records: 100
--------------------------------------------------
Record 1:
  ip_address: 192.168.1.1
  temperature_c: 37.5

Record 2:
  ip_address: 10.0.0.1
  temperature_c: 36.8
```

## Error Handling Strategy

1. **Schema Validation**
   - Field existence check
   - Type verification

2. **Database Connection**
   - Graceful failure if DB unavailable
   - Warnings for disabled connections

3. **Query Execution**
   - Try-catch for SQL errors
   - Try-catch for MongoDB errors

4. **Input Validation**
   - JSON format validation
   - Command format validation

## Integration with Existing Components

### Relationship with sql_handler.py
- **SQLHandler**: Used for batch inserts during streaming operations
- **CRUD.py**: Direct connection for interactive queries
- **Reason**: CRUD.py needs independent connection lifecycle

### Relationship with mongo_handler.py
- **MongoHandler**: Used for batch inserts during streaming
- **CRUD.py**: Direct connection for interactive operations
- **Reason**: Separate connection management

### Relationship with analyzer.py, classifier.py, etc.
- These components work on streaming data
- CRUD.py provides query interface to stored data
- Schema metadata produced by analyzer used by CRUD

## Performance Considerations

1. **Query Limits**: 1000 record limit per database
   - Prevents memory issues with large result sets
   - Can be adjusted in code if needed

2. **Index Usage**: 
   - SQL: Indexes on (sys_ingested_at, username)
   - MongoDB: Default indexes

3. **Connection Pooling**:
   - Direct connections (not pooled)
   - Open/close per command
   - Can optimize later if needed

## Future Enhancements

1. **Update Operations**: Modify existing records
2. **Delete Operations**: Remove records
3. **Advanced Queries**: Filtering, sorting, aggregation
4. **Batch Operations**: Bulk read/create
5. **Index Optimization**: Analyze and create indices
6. **Query Caching**: Cache schema metadata
7. **Connection Pooling**: Reuse connections
8. **Pagination**: Support for large result sets

## Testing

Use `test_crud.py` to test:
```bash
python3 test_crud.py 1   # Schema display
python3 test_crud.py 2   # Read SQL fields
python3 test_crud.py 3   # Read MongoDB fields
python3 test_crud.py 4   # Read mixed fields
python3 test_crud.py 5   # Create new record
python3 test_crud.py 6   # Error handling
```

## Summary

CRUD.py provides an intelligent command-line interface for querying and modifying hybrid SQL/MongoDB data. It:
- Uses schema metadata to route queries appropriately
- Combines results from multiple databases
- Handles create operations with automatic field mapping
- Provides clear output and error handling
- Integrates seamlessly with existing components
