# Adaptive Ingestion System 

> **Course Project:** CS 432 Databases (Assignment 1)  
> **Topic:** Adaptive Ingestion & Hybrid Backend Placement
> **Assingment-2 Video Link:** https://youtu.be/1UnupiQ_ETQ

##  Overview
This project implements an **autonomous data ingestion engine** that dynamically routes incoming JSON records to the optimal storage backend (**MySQL** or **MongoDB**) based on data characteristics.

It features a **3-Stage Producer-Consumer Pipeline** capable of handling high-throughput data streams, detecting schema drift in real-time, and **automatically migrating data** between SQL and NoSQL stores when stability criteria change.

##  Key Features
*   **Hybrid Storage**: Automatically splits a single record into Structured (SQL) and Semi-Structured (MongoDB) components.
*   **Adaptive Classification**: Uses heuristics (Frequency, Type Stability, Nesting, Uniqueness) to decide storage target.
*   **Schema Evolution**: Automatically `ALTERs` SQL tables to add new columns.
*   **Automated Migration**: If a field becomes "unstable" (e.g., changes type), the system **migrates existing data from SQL to MongoDB** and drops the SQL column to preserve integrity.
*   **Concurrency**: Multi-threaded architecture (Ingestor, Processor, Router) ensures ingestion never blocks processing.
*   **Zero Data Potential Loss**: Uses thread-safe Queues and Backpressure.

##  Architecture
The system follows a threaded pipeline architecture:
`Ingestion Thread` $\rightarrow$ `Raw Queue` $\rightarrow$ `Processing Thread` $\rightarrow$ `Write Queue` $\rightarrow$ `Router Thread`

See [architecture.txt](architecture.txt) for a diagram and [system_concepts.md](system_concepts.md) for detailed logic of each component.

##  Prerequisites
*   **Python 3.8+**
*   **MySQL Server** (Running locally or remotely)
*   **MongoDB** (Running locally or remotely)

##  Installation
1.  **Clone the repository**:
    ```bash
    git clone <repo-url>
    cd Database-Track-2
    ```

##  Quick Start

**Prerequisites:** MySQL and MongoDB must be running locally.

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment
Create a `.env` file (or edit the existing one):
```env
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=adaptive_db
SQL_HOST=localhost
SQL_PORT=3306
SQL_USER=root
SQL_PASSWORD=your_password
SQL_DB_NAME=adaptive_db
```

### Step 3: Start Simulation Server
Open a **separate terminal** and run:
```bash
uvicorn simulation_code:app --reload --port 8000
```
Leave this running. You should see: `Uvicorn running on http://127.0.0.1:8000`

### Step 4: Run the Adaptive Engine
In the **original terminal**, run:
```bash
python3 main.py
```

The system will:
- ✓ Check if the simulation server is running
- ✓ Connect to MySQL and MongoDB
- ✓ Load previous metadata (if any)
- ✓ Start processing data streams

### Step 5: Interact with the System
Once you see `SYSTEM READY`, you can type commands in the prompt:

#### Available Commands

| Command | Description | Example |
|---------|-------------|---------|
| `status` | Shows system uptime, total records processed, and active field count | `>> status` |
| `stats <field>` | Displays detailed analytics for a specific field including frequency ratio, type stability, uniqueness, and detected type | `>> stats age` |
| `queue` | Shows the number of records currently waiting in the ingestion buffer | `>> queue` |
| `help` | Lists all available commands with brief descriptions | `>> help` |
| `exit` | Gracefully shuts down all worker threads and closes database connections | `>> exit` |

**Example Session:**
```bash
>> status
System Uptime: 45 seconds
Total Records Processed: 2150
Active Fields Tracked: 38

>> stats device_model
Stats for 'device_model': {'frequency_ratio': 0.92, 'type_stability': 'stable', 
'detected_type': 'str', 'is_nested': False, 'unique_ratio': 0.004, 'count': 1978}

>> queue
Current Queue Size: 12 records pending processing.

>> exit
Initiating shutdown...
```

**Note:** The system automatically adapts as data arrives. Watch for messages like:
- `[SQL Handler] Evolving Schema: Adding column 'field_name'`
- `[Router] MIGRATION: Field drifted from SQL to MongoDB`

##  Logic & Heuristics
*   **Nested Data** $\rightarrow$ MongoDB (Always)
*   **Unstable Types** (e.g., Int then String) $\rightarrow$ MongoDB (Always)
*   **Sparse Data** (Frequency < 80%) $\rightarrow$ MongoDB
*   **High Cardinality** (Unique Ratio = 1.0) $\rightarrow$ SQL (as `UNIQUE` column)
*   **Standard** $\rightarrow$ SQL


##  Normalization

The **Normalizer** component ensures consistent data formatting and handles complex data transformations before ingestion.

### Key Features
- **Key Standardization**: Converts CamelCase and PascalCase keys to snake_case for consistent column naming
- **Record Normalization**: Cleans incoming JSON records and adds system metadata
- **Record Shredding**: Decomposes complex/nested records into normalized relational structures
- **M:N Relationship Handling**: Automatically creates junction tables for many-to-many relationships

### Normalization Process

#### 1. Basic Record Normalization
Cleans keys and adds system ingestion timestamp:
```python
record = {"firstName": "John", "lastName": "Doe"}
normalized = normalizer.normalize_record(record)
# Result: {"first_name": "John", "last_name": "Doe", "sys_ingested_at": <timestamp>}
```

#### 2. Record Shredding
Breaks down complex nested structures into multiple normalized tables:
```python
record = {
    "customer_id": 1,
    "name": "Alice",
    "orders": [
        {"order_id": 101, "amount": 250},
        {"order_id": 102, "amount": 150}
    ]
}

shredded = normalizer.shred_record_with_m2m(record)
# Result:
# {
#   "root": [{"uuid": "...", "customer_id": 1, "name": "Alice"}],
#   "orders": [
#     {"uuid": "...", "order_id": 101, "amount": 250, "root_id": "..."},
#     {"uuid": "...", "order_id": 102, "amount": 150, "root_id": "..."}
#   ]
# }
```

### Core Methods
- `normalize_record(record)` - Cleans keys and adds system metadata
- `shred_record(record)` - Decomposes nested structures into normalized tables
- `shred_record_with_m2m(record)` - Advanced shredding with M:N junction table creation

---

##  Metadata Manager

The **Metadata Manager** maintains the system's schema state, field routing information, and historical tracking of schema evolution.

### Key Features
- **Schema Versioning**: Tracks schema changes and evolution over time
- **Field Routing**: Maintains mapping of fields to their storage backend (SQL/MongoDB)
- **Field Statistics**: Preserves analyzer statistics for each field
- **Persistence**: Saves and loads schema metadata from `metadata/schema_map.json`
- **Thread-Safe Operations**: Uses locking to ensure consistency in concurrent environments

### Metadata Structure
```json
{
  "version": 2.0,
  "schema_version": 1,
  "last_updated": "2026-03-22T10:30:00",
  "relational_structure": {
    "tables": {
      "root": {
        "columns": ["id", "name", "email", "sys_ingested_at"],
        "types": ["INT", "VARCHAR(255)", "VARCHAR(255)", "TIMESTAMP"]
      }
    }
  },
  "collection_structure": {
    "customer_data": ["_id", "metadata", "nested_field"]
  },
  "field_routing": {
    "name": {"backend": "sql", "type": "str", "is_unique": false},
    "tags": {"backend": "mongo", "type": "array", "is_unique": false}
  },
  "field_stats": {
    "name": {"frequency_ratio": 0.98, "type_stability": "stable"}
  },
  "schema_history": [
    {"timestamp": "2026-03-20T08:00:00", "change": "Added column email to root"},
    {"timestamp": "2026-03-21T14:30:00", "change": "Migrated tags field to MongoDB"}
  ]
}
```

### API Methods
- `save_metadata()` - Persist schema to disk
- `load_metadata()` - Load schema from disk on startup
- `record_field(table, field, field_type, backend)` - Register a new field
- `record_schema_change(change_description)` - Log schema evolution
- `get_field_routing()` - Retrieve field-to-backend mappings
- `restore_analyzer_state(analyzer)` - Restore statistical data

---

##  CRUD Operations

### Setup: Running the CLI CRUD Client

To execute CRUD operations, run the CLI client in a **new terminal**:
```bash
python3 cli_crud_client.py
```

This starts an interactive CLI where you can submit JSON queries. You should see a prompt:
```
Connected to CRUD Engine
>> 
```

### Query Format

All CRUD operations use JSON format. Type a JSON query at the prompt and press Enter.

### Create Operation (INSERT)

Insert a single record:
```json
{"operation": "insert", "data": {"username": "demo_user", "email": "demo@example.com", "profile_bio": "User profile information"}}
```

**CLI Example:**
```
>> {"operation": "insert", "data": {"username": "demo_user", "email": "demo@example.com"}}
Status: success
UUID: 550e8400-e29b-41d4-a716-446655440000
```

### Read Operations

#### Fetch record by UUID
```json
{"operation": "read", "root_id": "550e8400-e29b-41d4-a716-446655440000"}
```

**CLI Example:**
```
>> {"operation": "read", "root_id": "550e8400-e29b-41d4-a716-446655440000"}
Status: success
Record: {"uuid": "550e8400-e29b-41d4-a716-446655440000", "username": "demo_user", "email": "demo@example.com"}
```

#### Query by filter (match condition)
```json
{"operation": "read", "filter": {"username": "demo_user"}}
```

#### List all values for a field
```json
{"operation": "list", "field": "username"}
```

**CLI Example:**
```
>> {"operation": "list", "field": "username"}
Status: success
Values: ["demo_user", "alice", "bob", "charlie"]
Count: 4
```

### Update Operation

Update records matching a filter:
```json
{"operation": "update", "filter": {"username": "demo_user"}, "data": {"email": "updated_demo@example.com"}}
```

**CLI Example:**
```
>> {"operation": "update", "filter": {"username": "demo_user"}, "data": {"email": "updated_demo@example.com"}}
Status: success
Updated: 1 record(s)
```

### Delete Operation

Delete by UUID:
```json
{"operation": "delete", "root_id": "550e8400-e29b-41d4-a716-446655440000"}
```

Delete by filter:
```json
{"operation": "delete", "filter": {"status": "archived"}}
```

**CLI Example:**
```
>> {"operation": "delete", "root_id": "550e8400-e29b-41d4-a716-446655440000"}
Status: success
Deleted: 1 record(s)
```

### Automated Demo

To see a complete end-to-end CRUD workflow without manual input:
```bash
python3 test_crud_demo.py
```

This demonstrates:
1. INSERT → Create new record
2. READ (by UUID) → Retrieve the record
3. READ (by filter) → Query by condition
4. UPDATE → Modify existing record
5. LIST → Show all values for a field
6. DELETE → Remove the record

##  Testing

Run the complete test suite:
```bash
pytest tests/ -v
```

Run specific test modules:
```bash
pytest tests/test_crud_engine.py -v        # Test CRUD operations
pytest tests/test_normalizer.py -v         # Test data normalization
pytest tests/test_metadata_manager.py -v   # Test metadata management
pytest tests/test_integration.py -v        # Test end-to-end flows
```
