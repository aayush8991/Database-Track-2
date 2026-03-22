# Assignment 2 Video Script - 5 Minutes
## What to Speak About (Comprehensive Guide)

---

## **SECTION 1: NORMALIZATION STRATEGY** (1 minute 15 seconds)

### **What We Do:**
"Building on Assignment 1, we now add intelligent **SQL Normalization**. Instead of storing everything flat, when we detect nested arrays or complex structures, we automatically decompose them into multiple normalized relational tables."

### **The Three Rules We Implement:**

**RULE 1: List of Dicts → Create Child Tables (1:N Relationship)**
- **What it detects**: Arrays of objects (e.g., comments, orders, transactions)
- **What happens**: Each array becomes a separate table with foreign keys
- **Example**:
  ```
  Input: {
    "username": "alice",
    "comments": [
      {"text": "great", "timestamp": 123},
      {"text": "awesome", "timestamp": 456}
    ]
  }
  
  Output:
  - root table: username, uuid
  - root_comments table: uuid, parent_id (FK), text, timestamp
  ```

**RULE 2: Nested Objects → Flatten (1:1 Relationship)**
- **What it detects**: Nested JSON objects (e.g., location, metadata, sensor_data)
- **What happens**: We flatten by prefixing (location_city, location_country)
- **Example**:
  ```
  Input: {
    "username": "bob",
    "location": {
      "city": "NYC",
      "country": "USA"
    }
  }
  
  Output: root table with columns: username, location_city, location_country
  ```

**RULE 3: Arrays of Primitives → M:N Junction Tables**
- **What it detects**: Simple arrays like tags, categories, skills
- **What happens**: Creates dimension table + junction table
- **Example**:
  ```
  Input: {
    "username": "charlie",
    "tags": ["python", "database", "systems"]
  }
  
  Output:
  - tags dimension table: tag_id, tag_name
  - root_tags junction table: root_id (FK), tag_id (FK)
  ```

### **Why This Matters:**
- **Eliminates Data Redundancy**: Each value stored once
- **Enables Referential Integrity**: Foreign keys link tables
- **Prevents Anomalies**: Update, delete, insert anomalies avoided
- **Fully Automatic**: No manual schema design needed

---

## **SECTION 2: MONGODB DOCUMENT STRATEGY** (1 minute)

### **What We Do:**
"For semi-structured data that goes to MongoDB, we apply intelligent **Document Decomposition**. We don't just store everything as one big document—we split large fields into separate collections."

### **The 10% Rule:**
```
IF field_size > (total_document_size × 10%)
  THEN move field to separate collection
ELSE keep embedded in main document
```

### **Two Strategies:**

**Strategy 1: Embedding (For Small Fields)**
- Keep data inside main document
- Good for: Small nested objects, rarely updated
- Example: `metadata: { device: "iPhone", os: "iOS" }`

**Strategy 2: Referencing (For Large Fields)**
- Move to separate collection, store reference in parent
- Good for: Large arrays, frequently updated, reusable data
- Example:
  ```
  Main Doc: {
    "username": "alice",
    "comments": "REF::MONGO::decomposed_comments::uuid-123"
  }
  
  Separate Collection (decomposed_comments):
    { parent_uuid: "uuid-123", data: [...100 items] }
  ```

### **When Each Strategy is Chosen:**
- **Embedding**: metadata, small nested objects, configuration
- **Referencing**: comments array (>1KB), large history, bulk data

### **Why This Matters:**
- **Query Performance**: Decomposed collections query faster
- **Document Size Limits**: MongoDB has 16MB limit per document
- **Flexible Updates**: Can update large fields without rewriting entire doc
- **Automatic**: System calculates and decides based on actual data

---

## **SECTION 3: METADATA SYSTEM** (1 minute)

### **What We Store:**

**The metadata file (schema_map.json) is the BRAIN of our system:**

```json
{
  "version": 2.0,
  "schema_version": 1,
  
  // ① RELATIONAL STRUCTURE - SQL Tables and Columns
  "relational_structure": {
    "tables": {
      "root": {
        "columns": ["username", "email", "age", ...]
      },
      "root_comments": {
        "columns": ["comment_id", "root_id", "text", ...]
      }
    }
  },
  
  // ② COLLECTION STRUCTURE - MongoDB Collections
  "collection_structure": {
    "unstructured_data": ["sparse", "semi-structured"],
    "decomposed_comments": ["large_arrays"]
  },
  
  // ③ FIELD ROUTING - Where Each Field Lives
  "field_routing": {
    "username": { "target": "SQL", "is_unique": true },
    "age": { "target": "SQL", "type": "int" },
    "comments": { "target": "MONGO", "decomposed": true },
    "metadata": { "target": "MONGO", "embedded": true }
  },
  
  // ④ FIELD STATISTICS - Analytics on Each Field
  "field_stats": {
    "username": {
      "frequency_ratio": 0.99,  // appears 99% of time
      "type_stability": "stable",  // always string
      "unique_ratio": 1.0,  // every value unique
      "count": 5000
    }
  },
  
  // ⑤ SCHEMA HISTORY - Audit Trail of Changes
  "schema_history": [
    {
      "timestamp": "2026-03-22T10:30:00",
      "change": "Added column 'new_field' to root table"
    }
  ]
}
```

### **How It's Used:**

1. **At Startup**: System loads metadata to understand current schema
2. **During Classification**: Router checks field_routing to decide SQL/MongoDB
3. **For CRUD**: Query engine uses relational_structure to build joins
4. **For Evolution**: When new field arrives, schema_history tracks it
5. **For Reconstruction**: When reading, system knows how to reassemble data

### **Why This Matters:**
- **Single Source of Truth**: Everything references this metadata
- **Zero Manual Schema**: Metadata auto-updates as data flows in
- **Enables Automation**: CRUD engine uses metadata to generate queries
- **Audit Trail**: Complete history of schema changes

---

## **SECTION 4: CRUD OPERATIONS** (1.5 minutes)

### **What We Do:**
"Once data is normalized and decomposed, we provide a simple **JSON interface** for users to perform all database operations. The system automatically:"
1. **Routes** to correct table/collection
2. **Generates SQL/MongoDB queries**
3. **Executes joins** if needed
4. **Merges results** back into JSON

### **CREATE (Insert)**
```json
{
  "operation": "insert",
  "data": {
    "username": "alice",
    "email": "alice@test.com",
    "age": 28,
    "comments": [
      {"text": "nice", "time": 123},
      {"text": "great", "time": 456}
    ]
  }
}
```

**What Happens:**
1. Splits into SQL: root table gets username, email, age
2. Creates child rows: root_comments table for each comment
3. Inserts into MongoDB: semi-structured data stored
4. Returns UUID for tracking

---

### **READ (Query)**
```json
{
  "operation": "read",
  "filter": {"username": "alice"},
  "include_plan": true
}
```

**What Happens:**
1. Checks metadata: "username is in SQL root table"
2. Generates SQL query: `SELECT * FROM root WHERE username = 'alice'`
3. Joins with child tables: `SELECT * FROM root_comments WHERE root_id = ?`
4. Fetches from MongoDB: `db.unstructured_data.find({username: 'alice'})`
5. **Merges both results** back into original JSON structure
6. Returns query plan showing exactly which queries were generated

---

### **UPDATE (Modify)**
```json
{
  "operation": "update",
  "filter": {"username": "alice"},
  "data": {
    "age": 29,
    "email": "newemail@test.com"
  }
}
```

**What Happens:**
1. Finds all records matching filter
2. Updates SQL fields in root table
3. Updates MongoDB fields in unstructured_data
4. Returns count of records updated

---

### **DELETE (Remove)**
```json
{
  "operation": "delete",
  "root_id": "uuid-12345"
}
```

**What Happens:**
1. Deletes from root table (parent record)
2. **Cascades**: Automatically deletes all root_comments child rows
3. Deletes from MongoDB unstructured_data and decomposed collections
4. **Zero orphaned data**: Everything cleaned up

---

### **LIST (Get Unique Values)**
```json
{
  "operation": "list",
  "field": "username"
}
```

**Response:**
```json
{
  "status": "success",
  "data": ["alice", "bob", "charlie"],
  "count": 3
}
```

---

### **Key Features of CRUD Engine:**

| Feature | Benefit |
|---------|---------|
| **Automatic Field Routing** | Knows where each field lives (SQL/MongoDB) |
| **Automatic Joins** | Reconstructs nested data from normalized tables |
| **Query Plan Exposure** | Users see exactly which queries ran |
| **Cascading Deletes** | No orphaned data across backends |
| **Type Conversion** | Handles type differences between SQL and MongoDB |
| **Uniqueness Validation** | Enforces unique constraints from metadata |

---

## **SECTION 5: End-to-End Example (30 seconds)**

### **Show Live:**

1. **Insert Complex Record:**
   ```json
   {
     "operation": "insert",
     "data": {
       "username": "demo_user",
       "email": "demo@test.com",
       "metadata": {"device": "iPhone", "version": "v2"},
       "tags": ["python", "database"],
       "comments": [
         {"text": "comment1", "time": 123}
       ]
     }
   }
   ```

2. **What Happens Behind Scenes:**
   - ✓ SQL root table: username, email
   - ✓ SQL root_comments: comment text and time
   - ✓ Dimension table: tags with IDs
   - ✓ Junction table: links root to tags
   - ✓ MongoDB: metadata stored as decomposed collection
   - ✓ Metadata updated with new schema

3. **Read it Back:**
   ```json
   {
     "operation": "read",
     "filter": {"username": "demo_user"},
     "include_plan": true
   }
   ```

4. **System shows:**
   - Generated SQL query with JOINs
   - MongoDB queries
   - Full reconstructed JSON
   - Execution time

---

## **TALKING POINTS - Quick Reference**

### **Normalization:**
- "We detect nested structures and automatically create tables"
- "3 rules: child tables for arrays, flattening for objects, junction tables for primitives"
- "Zero redundancy, full referential integrity"

### **MongoDB:**
- "We don't just embed everything—we're smart about document size"
- "10% rule: large fields go to separate collections"
- "References between collections for flexibility"

### **Metadata:**
- "This JSON file is the brain—it knows everything about the schema"
- "5 components: table structure, collections, field routing, stats, history"
- "Auto-updates as data flows in"

### **CRUD:**
- "Simple JSON interface hides complexity"
- "System generates all SQL/MongoDB queries automatically"
- "Query plan exposure shows what's happening under the hood"
- "Cascading deletes keep data consistent"

---

## **Anticipated Questions You Might Get:**

**Q: How does it handle schema evolution?**
A: When a new field arrives, the Analyzer detects it, Router classifies it, and Metadata auto-updates. Next query uses the new schema.

**Q: What if a document exceeds 16MB?**
A: The 10% decomposition rule ensures most large fields go to separate collections before they cause issues.

**Q: How does deletion work across SQL and MongoDB?**
A: When you delete a root record, we cascade through all foreign key relationships in SQL, then delete from all MongoDB collections that reference it.

**Q: Can users see the queries being generated?**
A: Yes! With `"include_plan": true`, we return the exact SQL and MongoDB queries that were executed.

**Q: How is performance?**
A: The decomposition strategy reduces document size, making queries faster. Indexes on foreign keys speed up joins.

---

## **DEMO CHECKLIST**

Before recording:
- [ ] Reset databases: `python3 reset_databases.py --reset-all --reset-metadata`
- [ ] Start simulation: `uvicorn simulation_code:app --reload --port 8000`
- [ ] Start engine: `python3 main.py`
- [ ] Wait for "SYSTEM READY"
- [ ] Have curl/Python ready to test CRUD
- [ ] Have MySQL and MongoDB tools ready to show tables/collections
- [ ] Have schema_map.json open for reference
