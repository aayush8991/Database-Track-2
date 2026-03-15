import mysql.connector
import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

def reset_sql_db():
    """Reset MySQL database"""
    config = {
        'host': os.getenv("SQL_HOST"),
        'port': int(os.getenv("SQL_PORT", 3306)),
        'user': os.getenv("SQL_USER"),
        'password': os.getenv("SQL_PASSWORD"),
        'database': os.getenv("SQL_DB_NAME")
    }

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        table_name = "structured_data"
        
        # Drop the table to start perfectly fresh
        print(f"[SQL] Dropping table {table_name}...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        print("[SQL] ✓ MySQL database reset complete.")
        conn.commit()
    except mysql.connector.Error as err:
        print(f"[SQL] Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def reset_mongo_db():
    """Reset MongoDB database"""
    try:
        uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME", "adaptive_db")
        
        if not uri:
            raise ValueError("MONGO_URI not found in .env file")
        
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        
        print(f"[MongoDB] Dropping database '{db_name}'...")
        client.drop_database(db_name)
        
        print("[MongoDB] ✓ MongoDB database reset complete.")
        client.close()
    except Exception as e:
        print(f"[MongoDB] Error: {e}")

if __name__ == "__main__":
    print("="*50)
    print("RESETTING DATABASES")
    print("="*50)
    reset_sql_db()
    reset_mongo_db()
    print("="*50)
    print("✓ All databases reset successfully!")
    print("="*50)
