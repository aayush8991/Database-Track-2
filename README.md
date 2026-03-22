# Adaptive Ingestion System 

> **Course Project:** CS 432 Databases (Assignment 1)  
> **Topic:** Adaptive Ingestion & Hybrid Backend Placement

## 📖 Overview
This project implements an **autonomous data ingestion engine** that dynamically routes incoming JSON records to the optimal storage backend (**MySQL** or **MongoDB**) based on data characteristics.

It features a **3-Stage Producer-Consumer Pipeline** capable of handling high-throughput data streams, detecting schema drift in real-time, and **automatically migrating data** between SQL and NoSQL stores when stability criteria change.

## ✨ Key Features
*   **Hybrid Storage**: Automatically splits a single record into Structured (SQL) and Semi-Structured (MongoDB) components.
*   **Adaptive Classification**: Uses heuristics (Frequency, Type Stability, Nesting, Uniqueness) to decide storage target.
*   **Schema Evolution**: Automatically `ALTERs` SQL tables to add new columns.
*   **Automated Migration**: If a field becomes "unstable" (e.g., changes type), the system **migrates existing data from SQL to MongoDB** and drops the SQL column to preserve integrity.
*   **Concurrency**: Multi-threaded architecture (Ingestor, Processor, Router) ensures ingestion never blocks processing.
*   **Zero Data Potential Loss**: Uses thread-safe Queues and Backpressure.

## 🏗 Architecture
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

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment**:
    Create a `.env` file in the root directory:
    ```env
    # MongoDB
    MONGO_URI=mongodb://localhost:27017/
    MONGO_DB_NAME=adaptive_db

    # MySQL
    SQL_HOST=localhost
    SQL_PORT=3306
    SQL_USER=root
    SQL_PASSWORD=password
    SQL_DB_NAME=adaptive_db

    # Data Source (Simulation)
    STREAM_URL=http://127.0.0.1:8000/record
    ```

## 🚀 Quick Start

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

## 🧠 Logic & Heuristics
*   **Nested Data** $\rightarrow$ MongoDB (Always)
*   **Unstable Types** (e.g., Int then String) $\rightarrow$ MongoDB (Always)
*   **Sparse Data** (Frequency < 80%) $\rightarrow$ MongoDB
*   **High Cardinality** (Unique Ratio = 1.0) $\rightarrow$ SQL (as `UNIQUE` column)
*   **Standard** $\rightarrow$ SQL


## 🔄 Normalization

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

## 📊 Metadata Manager

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

## 🛠 CRUD Operations

The **CRUD Engine** provides a unified interface for Create, Read, Update, and Delete operations across both SQL and MongoDB backends.

### Overview
The CRUD Engine abstracts away backend complexity, automatically routing operations to the appropriate database based on metadata configuration.

### Create Operations

#### Insert Single Record
```python
record = {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "tags": ["python", "database"]
}

crud_engine.create(record)
# Automatically splits and routes:
# - Structured fields (name, email, age) → MySQL
# - Unstructured/array fields (tags) → MongoDB
```

#### Batch Insert
```python
records = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Charlie", "email": "charlie@example.com"}
]

crud_engine.batch_create(records)
# Returns: Number of successfully inserted records
```

### Read Operations

#### Fetch Single Record by ID
```python
record = crud_engine.read(record_uuid)
# Example response:
# {
#   "uuid": "550e8400-e29b-41d4-a716-446655440000",
#   "name": "Alice",
#   "email": "alice@example.com",
#   "age": 30,
#   "tags": ["python", "database"]
# }
```

#### Query Records with Filtering
```python
records = crud_engine.read_where("name", "Alice")
# Returns all records matching the condition

records = crud_engine.read_range("age", 25, 35)
# Returns records where age is between 25 and 35
```

#### Full Table Scan
```python
all_records = crud_engine.read_all()
# Returns complete dataset from both backends (joins automatically)
```

### Update Operations

#### Update Record
```python
updates = {
    "email": "alice.new@example.com",
    "age": 31
}

crud_engine.update(record_uuid, updates)
# Intelligently routes:
# - SQL-backed fields → MySQL UPDATE
# - MongoDB-backed fields → MongoDB document update
```

#### Bulk Update
```python
filter_condition = {"status": "active"}
updates = {"status": "inactive"}

crud_engine.bulk_update(filter_condition, updates)
# Updates multiple records matching condition
```

### Delete Operations

#### Delete Single Record
```python
crud_engine.delete(record_uuid)
# Removes record from:
# 1. Root table in MySQL
# 2. Corresponding documents in MongoDB
# 3. All related references and junctions
```

#### Delete with Filter
```python
crud_engine.delete_where("status", "archived")
# Deletes all records matching the condition
```

### Advanced Features

#### Schema Validation
```python
errors = crud_engine.validate_record(record)
# Returns list of validation errors (if any):
# - Type mismatches
# - Uniqueness constraint violations
# - Missing required fields
```

#### Reference Resolution
```python
# Automatically resolve cross-backend references
full_record = crud_engine.get_with_references(record_uuid)
# Combines SQL data + MongoDB nested fields + resolved relationships
```

#### Performance Tracking
```python
# All CRUD operations are tracked for performance monitoring
stats = crud_engine.get_operation_stats()
# Returns execution times, success rates, and resource usage
```

### Routing Rules for CRUD Operations

| Field Type | SQL | MongoDB | Treatment |
|------------|-----|---------|-----------|
| Scalar (int, str, bool) | ✓ | | Standard columns |
| Sparse Data | | ✓ | Infrequent fields |
| Nested Objects | | ✓ | Complex structures |
| Arrays | | ✓ | Multi-value fields |
| High Cardinality | ✓ | | UNIQUE constraint |
| Type Unstable | | ✓ | Type changes detected |

### Error Handling

The CRUD Engine provides comprehensive error handling:
```python
try:
    crud_engine.create(record)
except ValidationError as e:
    print(f"Invalid record: {e}")
except UniqueConstraintError as e:
    print(f"Duplicate value: {e}")
except ConnectionError as e:
    print(f"Database connection failed: {e}")
```

---

## 🧪 Testing

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

With coverage report:
```bash
pytest tests/ --cov=core --cov-report=html
```