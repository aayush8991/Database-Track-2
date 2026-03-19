# CRUD.py - User Guide

## Overview
The CRUD.py file provides command-line functionality for reading from and creating records in a hybrid SQL/MongoDB database system. It intelligently routes queries based on field storage locations defined in `metadata/schema_map.json`.

## Key Features

### 1. **Schema Awareness**
- Automatically determines if fields are stored in SQL, MongoDB, or both
- References `metadata/schema_map.json` for field metadata

### 2. **Dual Database Support**
- **SQL Database**: Structured data (defined columns)
- **MongoDB**: Semi-structured and flexible data
- **Both**: Fields available in both databases

### 3. **Unified Output**
- Results from all queried databases are combined into `output.txt`
- Clearly labeled by database source

## Usage Commands

### 1. View Available Fields
Display all fields and their storage locations:

```bash
python3 CRUD.py schema
```

Output shows fields grouped by storage location:
- **SQL Only**: Fields stored only in SQL database
- **MongoDB Only**: Fields stored only in MongoDB
- **Both Databases**: Fields available in both

### 2. Read Records
Query specific fields from appropriate databases:

```bash
python3 CRUD.py read: field1, field2, field3
```

**Example:**
```bash
python3 CRUD.py read: username, ip_address
python3 CRUD.py read: device_model, spo2
python3 CRUD.py read: temperature_c, humidity, timestamp
```

**What happens:**
1. Parses field names from command
2. Looks up each field in schema_map.json to find storage location
3. Queries SQL for SQL/BOTH fields
4. Queries MongoDB for MONGO/BOTH fields
5. Combines results and saves to `output.txt`

### 3. Create Records
Insert new records into appropriate databases:

```bash
python3 CRUD.py create
```

**Input Format:**
After running the command, enter data as a single-line JSON object:

```json
{"username": "john_doe", "ip_address": "192.168.1.1", "device_model": "iPhone 14", "spo2": 98, "temperature_c": 37.5}
```

**What happens:**
1. Reads JSON input from stdin
2. Determines which fields go to SQL vs MongoDB
3. Automatically adds `sys_ingested_at` timestamp
4. Inserts to SQL for SQL/BOTH fields
5. Inserts to MongoDB for MONGO/BOTH fields
6. Reports success/failure for each database

### 4. Delete Records
Delete records matching a specific field value:

```bash
python3 CRUD.py delete: field=value
```

**Example:**
```bash
python3 CRUD.py delete: username=john_doe
python3 CRUD.py delete: device_model=iPhone14
python3 CRUD.py delete: ip_address=192.168.1.1
python3 CRUD.py delete: spo2=98
```

**What happens:**
1. Parses field name and value from command
2. Looks up field in schema_map.json
3. Deletes from SQL if field is stored there
4. Deletes from MongoDB if field is stored there
5. Reports how many records were deleted from each database

**Value Format:**
- String values: `python3 CRUD.py delete: username=john_doe`
- Quoted strings: `python3 CRUD.py delete: username="john doe"`
- Numbers: `python3 CRUD.py delete: spo2=98`
- Booleans: `python3 CRUD.py delete: charging=true`
- Null: `python3 CRUD.py delete: field=null`

## Database Field Mapping

From the schema_map.json, fields are categorized as:

### SQL Only Fields:
- device_model
- spo2
- purchase_value

### MongoDB Only Fields:
- phone, ip_address, device_id
- altitude, speed, direction
- city, country, session_id, steps
- temperature_c, humidity, air_quality
- item, payment_status, language, timezone
- ram_usage, error_code, comment, avatar_url
- last_seen, name, weather, action, subscription
- friends_count, app_version, cpu_usage, disk_usage
- email, os, charging, sleep_hours, signal_strength
- is_active, metadata, network, gps_lon, gps_lat
- postal_code, mood, retry_count, stress_level
- heart_rate, age, battery
- And more...

### Both Databases:
- sys_ingested_at
- username
- timestamp

## Output Format

After running a read command, results are saved to `output.txt` with this structure:

```
=== CRUD Operation Results ===
Timestamp: 2026-03-19T...

### SQL Database Results ###
Total Records: 100
--------------------------------------------------
Record 1:
  field1: value1
  field2: value2

### MONGO Database Results ###
Total Records: 100
--------------------------------------------------
Record 1:
  field1: value1
  field3: value3
```

## Example Workflows

### Workflow 1: Query User Data
```bash
# Check what fields are available
python3 CRUD.py schema

# Query user information (routes to appropriate databases)
python3 CRUD.py read: username, country, device_model

# Results saved to output.txt
cat output.txt
```

### Workflow 2: Create New Records
```bash
# Create a new record with user and sensor data
python3 CRUD.py create

# When prompted, enter:
{"username": "user123", "ip_address": "10.0.0.1", "device_model": "Samsung S23", "spo2": 97, "temperature_c": 36.8, "humidity": 55, "timestamp": "2026-03-19T10:30:00"}

# Record is inserted to both SQL (for device_model, spo2) and MongoDB (for other fields)
```

### Workflow 3: Query Health Metrics
```bash
python3 CRUD.py read: spo2, temperature_c, heart_rate, humidity, timestamp

# Results combined from both databases and saved to output.txt
```

## Error Handling

- **Missing fields**: Will show which fields are not in schema
- **Database connection issues**: Operations gracefully handle missing database connections
- **Invalid JSON**: Create command will report JSON parsing errors
- **Invalid commands**: Shows usage information

## Implementation Details

### CRUDOperations Class

**Key Methods:**
- `read(command)` - Parse and execute READ operations
- `create()` - INSERT new records
- `_get_field_storage_location(field)` - Look up field storage in schema
- `_read_from_sql(fields)` - Query SQL database
- `_read_from_mongo(fields)` - Query MongoDB
- `_create_in_sql(record)` - Insert into SQL
- `_create_in_mongo(record)` - Insert into MongoDB
- `_save_output(results)` - Write results to output.txt

### Schema Discovery
The tool automatically reads `metadata/schema_map.json` to determine:
- Which fields exist
- Where each field is stored (SQL/MONGO/BOTH)
- Field data types
- Field statistics

## Requirements

The following environment variables must be set (.env file):
- `SQL_HOST` - MySQL host
- `SQL_PORT` - MySQL port (default: 3306)
- `SQL_USER` - MySQL username
- `SQL_PASSWORD` - MySQL password
- `SQL_DB_NAME` - MySQL database name
- `MONGO_URI` - MongoDB connection URI
- `MONGO_DB_NAME` - MongoDB database name

## Notes

- Read command limits results to 1000 records per database
- Create command automatically adds system ingestion timestamp
- All database connections are properly closed after operations
- Results include metadata about query execution
