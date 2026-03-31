#!/usr/bin/env python3
"""
Database Reset and Cleanup Utility

Provides functions to reset both SQL and MongoDB for clean testing.
Useful before each test run to ensure no data contamination.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler

class DatabaseCleaner:
    """Utility for cleaning up databases for testing."""
    
    def __init__(self):
        self.sql_handler = None
        self.mongo_handler = None
    
    def connect(self):
        """Connect to both databases."""
        try:
            self.sql_handler = SQLHandler()
            print("✓ Connected to MySQL")
        except Exception as e:
            print(f"✗ Failed to connect to MySQL: {e}")
            return False
        
        try:
            self.mongo_handler = MongoHandler()
            print("✓ Connected to MongoDB")
        except Exception as e:
            print(f"✗ Failed to connect to MongoDB: {e}")
            return False
        
        return True
    
    def reset_mysql(self):
        """Reset all MySQL tables."""
        if not self.sql_handler:
            print("✗ MySQL not connected")
            return False
        
        try:
            cursor = self.sql_handler.cursor
            conn = self.sql_handler.conn
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            if not tables:
                print("ℹ️  MySQL: No tables to drop")
                return True
            
            print(f"\n📊 MySQL Database: Resetting {len(tables)} table(s)")
            
            # Disable foreign key checks temporarily
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Drop each table
            for table in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                    print(f"  ✓ Dropped table: {table}")
                except Exception as e:
                    print(f"  ⚠️  Failed to drop table {table}: {e}")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            
            print("✓ MySQL reset complete")
            return True
            
        except Exception as e:
            print(f"✗ MySQL reset failed: {e}")
            return False
    
    def reset_mongodb(self):
        """Reset all MongoDB collections."""
        if not self.mongo_handler:
            print("✗ MongoDB not connected")
            return False
        
        try:
            db = self.mongo_handler.db
            
            # Get all collections
            collections = db.list_collection_names()
            
            if not collections:
                print("ℹ️  MongoDB: No collections to drop")
                return True
            
            print(f"\n📊 MongoDB Database: Resetting {len(collections)} collection(s)")
            
            # Drop each collection
            for collection in collections:
                try:
                    db[collection].drop()
                    print(f"  ✓ Dropped collection: {collection}")
                except Exception as e:
                    print(f"  ⚠️  Failed to drop collection {collection}: {e}")
            
            print("✓ MongoDB reset complete")
            return True
            
        except Exception as e:
            print(f"✗ MongoDB reset failed: {e}")
            return False
    
    def reset_metadata(self, metadata_file="metadata/schema_map.json"):
        """Reset metadata file."""
        try:
            if os.path.exists(metadata_file):
                os.remove(metadata_file)
                print(f"✓ Removed metadata file: {metadata_file}")
            else:
                print(f"ℹ️  Metadata file not found: {metadata_file}")
            return True
        except Exception as e:
            print(f"✗ Failed to remove metadata: {e}")
            return False
    
    def reset_all(self, include_metadata=False):
        """Reset all databases and optionally metadata."""
        print("="*70)
        print("🔄 DATABASE CLEANUP AND RESET")
        print("="*70)
        
        if not self.connect():
            print("\n✗ Failed to connect to databases")
            return False
        
        results = []
        
        # Reset MySQL
        results.append(self.reset_mysql())
        
        # Reset MongoDB
        results.append(self.reset_mongodb())
        
        # Reset Metadata
        if include_metadata:
            results.append(self.reset_metadata())
        
        print("\n" + "="*70)
        if all(results):
            print("✅ All databases reset successfully!")
        else:
            print("⚠️  Some databases failed to reset")
        print("="*70 + "\n")
        
        return all(results)
    
    def get_stats(self):
        """Get current database statistics."""
        print("="*70)
        print("📊 DATABASE STATISTICS")
        print("="*70)
        
        # MySQL Stats
        if self.sql_handler:
            try:
                cursor = self.sql_handler.cursor
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                print(f"\n✓ MySQL ({len(tables)} tables):")
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    count = cursor.fetchone()[0]
                    print(f"  - {table_name}: {count} rows")
                
            except Exception as e:
                print(f"✗ Failed to get MySQL stats: {e}")
        
        # MongoDB Stats
        if self.mongo_handler:
            try:
                db = self.mongo_handler.db
                collections = db.list_collection_names()
                
                print(f"\n✓ MongoDB ({len(collections)} collections):")
                for collection in collections:
                    count = db[collection].count_documents({})
                    print(f"  - {collection}: {count} documents")
                
            except Exception as e:
                print(f"✗ Failed to get MongoDB stats: {e}")
        
        print("\n" + "="*70 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database cleanup and reset utility")
    parser.add_argument(
        "--reset-all",
        action="store_true",
        help="Reset both MySQL and MongoDB"
    )
    parser.add_argument(
        "--reset-mysql",
        action="store_true",
        help="Reset MySQL only"
    )
    parser.add_argument(
        "--reset-mongo",
        action="store_true",
        help="Reset MongoDB only"
    )
    parser.add_argument(
        "--reset-metadata",
        action="store_true",
        help="Reset metadata file"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics"
    )
    
    args = parser.parse_args()
    
    cleaner = DatabaseCleaner()
    
    # If no arguments, show help and stats
    if not any([args.reset_all, args.reset_mysql, args.reset_mongo, args.reset_metadata, args.stats]):
        parser.print_help()
        print("\n" + "="*70)
        print("💡 USAGE EXAMPLES:")
        print("="*70)
        print("""
# Reset everything before testing
python3 reset_databases.py --reset-all

# Reset only MySQL
python3 reset_databases.py --reset-mysql

# Reset only MongoDB
python3 reset_databases.py --reset-mongo

# Reset and also clear metadata
python3 reset_databases.py --reset-all --reset-metadata

# Show current database statistics
python3 reset_databases.py --stats

# Common workflow
python3 reset_databases.py --reset-all --reset-metadata  # Clean slate
python3 run_step_by_step_tests.py                        # Run tests
python3 reset_databases.py --stats                        # Check results
""")
        return
    
    if args.stats:
        cleaner.connect()
        cleaner.get_stats()
    
    if args.reset_all:
        cleaner.reset_all(include_metadata=args.reset_metadata)
    else:
        if not cleaner.connect():
            return
        
        if args.reset_mysql:
            cleaner.reset_mysql()
        
        if args.reset_mongo:
            cleaner.reset_mongodb()
        
        if args.reset_metadata:
            cleaner.reset_metadata()


if __name__ == "__main__":
    main()