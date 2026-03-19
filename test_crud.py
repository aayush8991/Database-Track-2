#!/usr/bin/env python3
"""
Example usage and tests for CRUD.py
Demonstrates read and create operations
"""

import subprocess
import json
import sys

def run_command(cmd):
    """Run command and show output"""
    print(f"\n{'='*60}")
    print(f"Running: {cmd}")
    print('='*60)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    return result.returncode

def test_schema():
    """Test schema command to see available fields"""
    print("\n### TEST 1: Display Available Fields ###")
    run_command("python3 CRUD.py schema")

def test_read_sql_fields():
    """Test reading SQL-only fields"""
    print("\n### TEST 2: Read SQL Fields (device_model, spo2) ###")
    run_command("python3 CRUD.py read: device_model, spo2")
    print("\nResults should be in output.txt")
    run_command("head -50 output.txt")

def test_read_mongo_fields():
    """Test reading MongoDB-only fields"""
    print("\n### TEST 3: Read MongoDB Fields (ip_address, temperature_c) ###")
    run_command("python3 CRUD.py read: ip_address, temperature_c")
    print("\nResults should be in output.txt")
    run_command("head -50 output.txt")

def test_read_mixed_fields():
    """Test reading fields from both databases"""
    print("\n### TEST 4: Read Mixed Fields (username, ip_address, device_model) ###")
    run_command("python3 CRUD.py read: username, ip_address, device_model")
    print("\nResults should be in output.txt")
    run_command("head -50 output.txt")

def test_create():
    """Test creating a new record"""
    print("\n### TEST 5: Create New Record ###")
    
    # Example record
    record = {
        "username": "test_user",
        "ip_address": "203.0.113.42",
        "device_model": "Pixel 8",
        "spo2": 98,
        "temperature_c": 37.2,
        "humidity": 60,
        "timestamp": "2026-03-19T14:30:00"
    }
    
    print(f"Inserting record: {json.dumps(record)}")
    
    # Use echo to pipe JSON to CRUD.py
    cmd = f"echo '{json.dumps(record)}' | python3 CRUD.py create"
    run_command(cmd)

def test_delete():
    """Test deleting records"""
    print("\n### TEST 7: Delete Records ###")
    
    print("\nTest 7a: Delete by SQL field (device_model)")
    run_command("python3 CRUD.py delete: device_model=iPhone14")
    
    print("\nTest 7b: Delete by MongoDB field (ip_address)")
    run_command("python3 CRUD.py delete: ip_address=192.168.1.1")
    
    print("\nTest 7c: Delete by field in both databases (username)")
    run_command("python3 CRUD.py delete: username=test_user")

def test_error_handling():
    """Test error handling"""
    print("\n### TEST 8: Error Handling ###")
    
    print("\nTest 8a: Invalid field name")
    run_command("python3 CRUD.py read: nonexistent_field")
    
    print("\nTest 8b: Invalid read command format")
    run_command("python3 CRUD.py write: field1")
    
    print("\nTest 8c: Invalid delete command format")
    run_command("python3 CRUD.py delete: no_equals_sign")

def display_available_fields():
    """Display the fields that exist in schema"""
    import json
    try:
        with open("metadata/schema_map.json", 'r') as f:
            schema = json.load(f)
        
        fields = schema.get("analyzer", {}).get("field_stats", {})
        
        print("\n### Available Fields in Schema ###")
        print(f"Total fields: {len(fields)}\n")
        
        # Show first 20 fields as examples
        for i, (field, stats) in enumerate(list(fields.items())[:20]):
            location = stats.get("db", "BOTH")
            count = stats.get("count", 0)
            types = stats.get("types", [])
            print(f"{i+1:2}. {field:20} - {location:6} - {count:6} records - {types}")
        
        if len(fields) > 20:
            print(f"\n... and {len(fields) - 20} more fields")
    except Exception as e:
        print(f"Error reading schema: {e}")

if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════╗
║           CRUD.py - Example Tests and Usage                ║
║                                                            ║
║ This script demonstrates how to use CRUD.py for:          ║
║   - Viewing available fields                              ║
║   - Reading records with intelligent database routing      ║
║   - Creating new records                                  ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    # Display available fields
    display_available_fields()
    
    if len(sys.argv) > 1:
        test_num = sys.argv[1]
        if test_num == "1":
            test_schema()
        elif test_num == "2":
            test_read_sql_fields()
        elif test_num == "3":
            test_read_mongo_fields()
        elif test_num == "4":
            test_read_mixed_fields()
        elif test_num == "5":
            test_create()
        elif test_num == "7":
            test_delete()
        elif test_num == "8":
            test_error_handling()
        else:
            print(f"Invalid test number: {test_num}")
            print("Usage: python3 test_crud.py [1-5,7-8]")
    else:
        print("\nTo run specific tests, use:")
        print("  python3 test_crud.py 1   - Display schema")
        print("  python3 test_crud.py 2   - Read SQL fields")
        print("  python3 test_crud.py 3   - Read MongoDB fields")
        print("  python3 test_crud.py 4   - Read mixed fields")
        print("  python3 test_crud.py 5   - Create new record")
        print("  python3 test_crud.py 7   - Delete records")
        print("  python3 test_crud.py 8   - Test error handling")
