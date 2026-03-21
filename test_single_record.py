#!/usr/bin/env python3
"""
Test script to send a single JSON record through your pipeline.
Useful for debugging and observing every component of the project.
"""
import json
import requests
from core.normalizer import Normalizer
from core.analyzer import Analyzer
from core.classifier import Classifier
from core.router import Router
from core.metadata_manager import MetadataManager
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler

# Sample record with MULTI-LEVEL NESTING to test proper decomposition
# This should be shredded into multiple relational tables
SAMPLE_RECORD = {
    "username": "john_doe_premium",
    "timestamp": "2026-03-22T14:30:00.000000",
    "email": "john@example.com",
    "subscription_tier": "premium",
    "is_active": True,
    
    # LEVEL 1 NESTING: Device Information (1:1 relationship, should be flattened)
    "device_info": {
        "device_id": "device-uuid-12345",
        "device_model": "iPhone 14 Pro",
        "os_name": "iOS",
        "os_version": "17.2.1",
        "app_version": "v5.2.1"
    },
    
    # LEVEL 1 NESTING: Location Data (1:1, should be flattened)
    "location": {
        "city": "San Francisco",
        "country": "United States",
        "gps_lat": 37.7749,
        "gps_lon": -122.4194,
        "altitude": 47.5
    },
    
    # LEVEL 1->2 NESTING: Orders (1:N relationship, should create child table)
    "orders": [
        {
            "order_id": "order-001",
            "order_date": "2026-03-20T10:00:00",
            "total_amount": 299.99,
            "payment_status": "completed",
            # LEVEL 2 NESTING within order: Items (1:N within 1:N)
            "items": [
                {
                    "item_id": "item-001",
                    "product_name": "Laptop",
                    "quantity": 1,
                    "unit_price": 199.99,
                    "category": "Electronics"
                },
                {
                    "item_id": "item-002",
                    "product_name": "Mouse",
                    "quantity": 2,
                    "unit_price": 50.00,
                    "category": "Accessories"
                }
            ]
        },
        {
            "order_id": "order-002",
            "order_date": "2026-03-22T15:30:00",
            "total_amount": 89.99,
            "payment_status": "pending",
            "items": [
                {
                    "item_id": "item-003",
                    "product_name": "Keyboard",
                    "quantity": 1,
                    "unit_price": 89.99,
                    "category": "Accessories"
                }
            ]
        }
    ],
    
    # LEVEL 1->2 NESTING: Health Metrics (1:N relationship, should create child table)
    "health_metrics": [
        {
            "metric_timestamp": "2026-03-22T14:00:00",
            "heart_rate": 72,
            "spo2": 98,
            "sleep_hours": 7.5,
            "stress_level": "low"
        },
        {
            "metric_timestamp": "2026-03-22T08:00:00",
            "heart_rate": 65,
            "spo2": 99,
            "sleep_hours": 0,
            "stress_level": "low"
        }
    ],
    
    # LEVEL 1->2 NESTING: System Metrics (1:N, should create child table)
    "system_metrics": [
        {
            "metric_time": "2026-03-22T14:30:00",
            "cpu_usage": 35,
            "ram_usage": 60,
            "disk_usage": 45,
            "battery_level": 85
        }
    ]
}

def process_single_record():
    """Process a single record through all pipeline components"""
    
    print("\n" + "="*70)
    print("  SINGLE RECORD TEST - RELATIONAL DECOMPOSITION")
    print("="*70)
    
    # Step 1: Show the raw record
    print("\n[1] RAW INPUT RECORD (Nested Structure):")
    print("-" * 70)
    print(json.dumps(SAMPLE_RECORD, indent=2))
    
    # Step 2: Normalize (snake_case conversion)
    print("\n[2] AFTER NORMALIZATION (Snake Case Conversion):")
    print("-" * 70)
    normalizer = Normalizer()
    normalized = normalizer.normalize_record(SAMPLE_RECORD)
    print(json.dumps(normalized, indent=2, default=str))
    
    # Step 3: SHREDDING - Break nested structure into multiple tables
    print("\n[3] AFTER SHREDDING (Relational Decomposition into Tables):")
    print("-" * 70)
    shredded = normalizer.shred_record(SAMPLE_RECORD)
    
    for table_name, rows in shredded.items():
        print(f"\n  📋 TABLE: {table_name}")
        print(f"  {'─' * 66}")
        for row in rows:
            print(f"    {json.dumps(row, indent=6, default=str)}")
    
    print("\n  📊 SHREDDING SUMMARY:")
    print(f"  {'─' * 66}")
    print(f"    Total Tables Created: {len(shredded)}")
    for table_name, rows in shredded.items():
        print(f"    - {table_name}: {len(rows)} row(s)")
    
    # Step 4: Analyze the root table only (after shredding)
    print("\n[4] SCHEMA ANALYSIS (Per-Table - Correct Approach):")
    print("-" * 70)
    
    # Create a separate analyzer instance for each table
    # This is the KEY FIX - don't accumulate stats across tables!
    table_analyses = {}
    
    for table_name, rows in shredded.items():
        print(f"\n  📊 Analyzing table: {table_name}")
        print(f"  {'─' * 66}")
        
        if rows:
            # Create FRESH analyzer per table
            table_analyzer = Analyzer()
            # Analyze with table_name parameter (enables per-table tracking)
            table_analyzer.analyze_batch(rows, table_name=table_name)
            table_stats = table_analyzer.get_schema_stats(table_name=table_name)
            table_analyses[table_name] = (table_stats, table_analyzer)
            
            # Show stats for this table
            print(f"    Rows: {len(rows)}")
            print(f"    Fields: {len(table_stats)}")
            
            # Show sample fields with their stats
            sample_fields = list(table_stats.items())[:3]
            for field, stats in sample_fields:
                freq = stats.get('frequency_ratio')
                dtype = stats.get('detected_type')
                print(f"      • {field}: {dtype} (freq: {freq:.2f})")
        else:
            print(f"    ⚠️  No data in this table")
    
    print(f"\n  {'─' * 66}")
    print(f"  Total tables analyzed: {len(table_analyses)}")
    print(f"  Analysis method: ✅ Per-Table (NOT mixed)")
    
    # Get stats from root table for next step
    schema_stats = table_analyses.get("root", ({}, None))[0]
    analyzer = table_analyses.get("root", ({}, None))[1]
    
    # Step 5: Classify Based on Per-Table Stats
    print("\n[5] SCHEMA CLASSIFICATION (Per-Table Routing):")
    print("-" * 70)
    classifier = Classifier()
    
    # Classify each table separately
    table_decisions_all = {}
    all_sql_fields = []
    all_mongo_fields = []
    
    for table_name, (table_stats, _) in table_analyses.items():
        print(f"\n  🔀 TABLE: {table_name}")
        print(f"  {'─' * 66}")
        
        # Classify with table context
        table_decisions = classifier.decide_schema(table_stats, table_name=table_name)
        table_decisions_all[table_name] = table_decisions
        
        sql_fields = [k for k, v in table_decisions.items() if v.get("target") in ["SQL", "BOTH"]]
        mongo_fields = [k for k, v in table_decisions.items() if v.get("target") == "MONGO"]
        
        all_sql_fields.extend(sql_fields)
        all_mongo_fields.extend(mongo_fields)
        
        print(f"    SQL Fields ({len(sql_fields)}):")
        for field in sorted(sql_fields):
            sql_type = table_decisions[field].get('sql_type', 'N/A')
            is_fk = " [FK]" if table_decisions[field].get('is_foreign_key') else ""
            is_pk = " [PK]" if table_decisions[field].get('is_primary_key') else ""
            print(f"      ✅ {field} → {sql_type}{is_fk}{is_pk}")
        
        if mongo_fields:
            print(f"    MongoDB Fields ({len(mongo_fields)}):")
            for field in sorted(mongo_fields):
                print(f"      📦 {field}")
    
    # Summary
    print(f"\n  {'─' * 66}")
    print(f"  📊 GLOBAL SUMMARY:")
    print(f"    Total SQL Fields: {len(set(all_sql_fields))}")
    print(f"    Total MongoDB Fields: {len(set(all_mongo_fields))}")
    
    # For backward compatibility, use root table decisions
    schema_decisions = table_decisions_all.get("root", {})
    
    # Step 6: Structure Map
    print("\n[6] RELATIONAL STRUCTURE MAP:")
    print("-" * 70)
    structure_map = analyzer.get_structure_map()
    print(json.dumps(structure_map, indent=2, default=str))
    
    # Step 7: Show all created tables
    print("\n[7] DATABASE SCHEMA SUMMARY:")
    print("-" * 70)
    print(f"  Root Table: root")
    print(f"  Child Tables (1:N Relationships):")
    for table_name in shredded.keys():
        if table_name != "root":
            print(f"    🔗 {table_name}")
    
    print("\n" + "="*70)
    print("  DECOMPOSITION COMPLETE")
    print("="*70 + "\n")

def process_from_simulation_api():
    """Fetch one record from the simulation API /test endpoint"""
    
    print("\n" + "="*70)
    print("  FETCHING SINGLE RECORD FROM SIMULATION API")
    print("="*70)
    
    try:
        response = requests.get("http://127.0.0.1:8000/test", stream=True, timeout=5)
        
        # Parse SSE response
        for line in response.iter_lines():
            if line:
                line = line.decode('utf-8')
                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    record = json.loads(data_str)
                    
                    print("\n[1] RAW RECORD FROM API:")
                    print("-" * 70)
                    print(json.dumps(record, indent=2))
                    
                    # Step 2: Normalize
                    print("\n[2] AFTER NORMALIZATION:")
                    print("-" * 70)
                    normalizer = Normalizer()
                    normalized = normalizer.normalize_record(record)
                    print(json.dumps(normalized, indent=2, default=str))
                    
                    # Step 3: Analyze Batch
                    print("\n[3] AFTER ANALYSIS (Batch Analysis):")
                    print("-" * 70)
                    analyzer = Analyzer()
                    batch = [normalized]
                    analyzer.analyze_batch(batch)
                    schema_stats = analyzer.get_schema_stats()
                    print(json.dumps(schema_stats, indent=2, default=str))
                    
                    # Step 4: Classify
                    print("\n[4] SCHEMA CLASSIFICATION:")
                    print("-" * 70)
                    classifier = Classifier()
                    schema_decisions = classifier.decide_schema(schema_stats)
                    print(json.dumps(schema_decisions, indent=2, default=str))
                    
                    print("\n" + "="*70)
                    print("  TEST COMPLETE")
                    print("="*70 + "\n")
                    break
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to simulation server at http://127.0.0.1:8000")
        print("   Make sure to run: python3 -m uvicorn simulation_code:app --reload")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--from-api":
        process_from_simulation_api()
    else:
        process_single_record()
