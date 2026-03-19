# CRUD.py - Quick Reference

## One-Liner Commands

### Display Schema
```bash
python3 CRUD.py schema
```

### Read Single Field
```bash
python3 CRUD.py read: username
python3 CRUD.py read: device_model
python3 CRUD.py read: temperature_c
```

### Read Multiple Fields
```bash
python3 CRUD.py read: username, ip_address
python3 CRUD.py read: device_model, spo2, purchase_value
python3 CRUD.py read: temperature_c, humidity, device_model
```

### Create Record with JSON Piping
```bash
echo '{"username": "test_user", "ip_address": "192.168.1.1"}' | python3 CRUD.py create
```

### Create Record with Interactive Input
```bash
python3 CRUD.py create
# Then paste your JSON when prompted
```

### Delete Records
```bash
python3 CRUD.py delete: username=test_user
python3 CRUD.py delete: device_model=iPhone14
python3 CRUD.py delete: ip_address=192.168.1.1
python3 CRUD.py delete: spo2=98
```

## Field Reference

### SQL-Only Fields (Query these for structured data)
```
device_model, spo2, purchase_value
```

### MongoDB-Only Fields (Query these for flexible data)
```
phone, ip_address, device_id, altitude, speed, direction, city, country,
session_id, steps, temperature_c, humidity, air_quality, item, payment_status,
language, timezone, ram_usage, error_code, comment, avatar_url, last_seen, name,
weather, action, subscription, friends_count, app_version, cpu_usage, disk_usage,
email, os, charging, sleep_hours, signal_strength, is_active, metadata, network,
gps_lon, gps_lat, postal_code, mood, retry_count, stress_level, heart_rate, age, battery
```

### Available in Both Databases
```
sys_ingested_at, username, timestamp
```

## Real-World Examples

### 1. Query User Sessions
```bash
python3 CRUD.py read: username, timestamp, session_id, action
```

### 2. Query Device Information
```bash
python3 CRUD.py read: device_model, os, device_id
```

### 3. Query Health Metrics
```bash
python3 CRUD.py read: spo2, heart_rate, temperature_c, humidity
```

### 4. Query Location Data
```bash
python3 CRUD.py read: city, country, gps_lat, gps_lon, altitude
```

### 5. Query Payment Information
```bash
python3 CRUD.py read: purchase_value, payment_status, item, timestamp
```

### 6. Insert Health Record
```bash
echo '{"username": "john_doe", "spo2": 98, "heart_rate": 72, "temperature_c": 37.2, "humidity": 55}' | python3 CRUD.py create
```

### 7. Insert Location Data
```bash
echo '{"username": "jane_doe", "city": "New York", "country": "USA", "gps_lat": 40.7128, "gps_lon": -74.0060}' | python3 CRUD.py create
```

### 8. Create Complex Record
```bash
python3 CRUD.py create
# Enter:
{"username": "user123", "device_model": "iPhone 14", "spo2": 97, "temperature_c": 36.8, "humidity": 60, "action": "login", "network": "WiFi"}
```

### 9. Delete by Username
```bash
python3 CRUD.py delete: username=john_doe
```

### 10. Delete by Device Model
```bash
python3 CRUD.py delete: device_model=iPhone14
```

### 11. Delete by Health Metric
```bash
python3 CRUD.py delete: spo2=98
```

### 12. Delete by IP Address
```bash
python3 CRUD.py delete: ip_address=192.168.1.1
```

## Output Location
All read results are saved to: `output.txt`

## Workflow Examples

### Workflow: Collect User Device Data
```bash
# 1. Check available fields
python3 CRUD.py schema | grep -i device

# 2. Query device information
python3 CRUD.py read: device_model, os, device_id

# 3. View results
cat output.txt
```

### Workflow: Track Health Metrics
```bash
# 1. Insert health record
echo '{"username": "patient1", "spo2": 98, "heart_rate": 75, "temperature_c": 37.0}' | python3 CRUD.py create

# 2. Query historical metrics
python3 CRUD.py read: spo2, heart_rate, temperature_c, timestamp

# 3. Check results
cat output.txt
```

### Workflow: Analyze Payment Data
```bash
# 1. Query purchases
python3 CRUD.py read: username, item, purchase_value, payment_status, timestamp

# 2. Insert new transaction
echo '{"username": "buyer1", "item": "laptop", "purchase_value": 999.99, "payment_status": "success"}' | python3 CRUD.py create

# 3. Review output
cat output.txt
```

### Workflow: Clean Up Test Data
```bash
# 1. Query test records
python3 CRUD.py read: username

# 2. Delete test user records
python3 CRUD.py delete: username=test_user
python3 CRUD.py delete: username=user123
python3 CRUD.py delete: username=jane_doe

# 3. Verify deletion
python3 CRUD.py read: username
```

## Common Field Combinations

### User Profile Data
```bash
python3 CRUD.py read: username, email, country, language, subscription
```

### Device & Performance
```bash
python3 CRUD.py read: device_model, device_id, os, cpu_usage, ram_usage, disk_usage
```

### Location & Movement
```bash
python3 CRUD.py read: city, country, gps_lat, gps_lon, altitude, speed, direction
```

### Session Activity
```bash
python3 CRUD.py read: username, timestamp, session_id, action, is_active
```

### Environmental Metrics
```bash
python3 CRUD.py read: temperature_c, humidity, air_quality, weather, network
```

## Error Solutions

### "Field not found in schema"
- Use `python3 CRUD.py schema` to see available fields
- Check field name spelling

### "Could not connect to SQL database"
- Check `.env` file for SQL_* variables
- Verify MySQL service is running

### "Could not connect to MongoDB"
- Check `.env` file for MONGO_URI
- Verify MongoDB service is running

### "Invalid JSON in create command"
- Ensure JSON is valid on single line
- Use double quotes for string values
- Example: `{"field": "value"}`

### "No results found"
- The databases may be empty
- Check if services are properly initialized

## Tips & Tricks

1. **Pipe output to grep**: `cat output.txt | grep "Record"`
2. **Search results**: `cat output.txt | grep "field_name"`
3. **Count records**: `cat output.txt | grep "Total Records"`
4. **Extract JSON with jq** (if installed): `python3 CRUD.py read: field1 | jq`
5. **Batch inserts**: Create a loop to insert multiple records
6. **Combine with other tools**: Redirect output for analysis

## Performance Notes

- Each read returns up to 1000 records per database
- Create commands return immediately after insertion
- Schema display is fast (reads JSON file)
- Connection overhead is minimal

## Key Concepts

| Concept | Explanation |
|---------|------------|
| **Schema Map** | JSON file defining where each field is stored |
| **Dual Database** | Some fields in SQL, some in MongoDB, some in both |
| **Automatic Routing** | CRUD.py picks right database based on schema |
| **Combined Output** | Results from multiple databases merged into output.txt |
| **sys_ingested_at** | Auto-added timestamp for all new records |

## Files Modified/Created

- `CRUD.py` - Main CRUD implementation (450+ lines)
- `test_crud.py` - Test suite and examples
- `CRUD_USAGE_GUIDE.md` - Detailed user guide
- `CRUD_ARCHITECTURE.md` - Technical architecture
- `CRUD_QUICK_REFERENCE.md` - This file

---

For detailed information, see `CRUD_USAGE_GUIDE.md` and `CRUD_ARCHITECTURE.md`
