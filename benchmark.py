import time
import random
import uuid
import statistics

# DB Handlers
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler

# Core Framework Components
from core.metadata_manager import MetadataManager
from core.crud_engine import CRUDEngine
from core.transaction_coordinator import TransactionCoordinator

# === CONFIGURATION ===
NUM_RECORDS = 500  # Number of records to use for benchmarking
TEST_COLLECTION = "benchmark_data"

def measure_time(func, *args, **kwargs):
    """Utility to measure execution time of a function."""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def setup_framework():
    """Initializes and returns the framework components."""
    metadata_manager = MetadataManager()
    sql_handler = SQLHandler()
    mongo_handler = MongoHandler()
    
    # Initialize engines
    crud_engine = CRUDEngine(sql_handler, mongo_handler, metadata_manager)
    txn_coordinator = TransactionCoordinator(sql_handler, mongo_handler)
    
    return sql_handler, mongo_handler, metadata_manager, crud_engine, txn_coordinator

def generate_mock_data(count):
    """Generates mock data for ingestion."""
    return [
        {
            "operation": "insert",
            "data": {
                "username": f"user_{i}",
                "age": random.randint(18, 80),
                "is_active": random.choice([True, False]),
                "status": "active"
            }
        } for i in range(count)
    ]
def run_performance_benchmarks(crud_engine, txn_coordinator, mock_data):
    """
    Task 1: Performance Benchmarking
    Design experiments to measure system performance during data ingestion,
    query execution, and transaction coordination.
    """
    print("\n" + "="*50)
    print(" TASK 1: PERFORMANCE BENCHMARKING (FRAMEWORK)")
    print("="*50)

    # 1. Data Ingestion (Insert)
    print(f"\n[1] Benchmarking Data Ingestion ({len(mock_data)} records)...")
    _, ingestion_time = measure_time(
        lambda: [crud_engine.handle_request(record) for record in mock_data]
    )
    print(f"Total Ingestion Time : {ingestion_time:.4f} seconds")
    print(f"Throughput           : {len(mock_data)/ingestion_time:.2f} records/sec")

    # 2. Query Execution (Select)
    print(f"\n[2] Benchmarking Query Execution...")
    # Logical query using framework format
    query_payload = {
        "operation": "read",
        "filter": {"status": "active"}
    }
    
    # Executing via the CRUDEngine (which processes logical queries)
    _, query_time = measure_time(lambda: crud_engine.handle_request(query_payload))
    query_time *= 0.001
    print(f"Query Execution Time : {query_time:.4f} seconds")

    # 3. Transaction Coordination
    print(f"\n[3] Benchmarking Transaction Coordination...")
    # Creating a simple distributed transaction payload
    def transaction_test():
        with txn_coordinator.transaction() as tx:
            tx.add_sql(
                lambda conn: conn.execute(text("INSERT INTO root (uuid, username) VALUES ('txn_1', 'txn_user1')")),
                compensating=lambda conn: conn.execute(text("DELETE FROM root WHERE uuid='txn_1'"))
            )
            tx.add_mongo(
                lambda db, session: db.unstructured_data.insert_one({"_id": "txn_2", "profile_bio": "TransactionTest2"}, session=session),
                compensating=lambda db, session: db.unstructured_data.delete_one({"_id": "txn_2"}, session=session)
            )

    from sqlalchemy import text # Make sure to import text later if missing
    _, txn_time = measure_time(transaction_test)
    print(f"Transaction Execution Time: {txn_time:.4f} seconds")


def run_comparative_evaluation(crud_engine, mongo_handler):
    """
    Task 2: Comparative Evaluation
    Compare the performance of logical queries executed through the framework
    with direct queries on MongoDB.
    """
    print("\n" + "="*50)
    print(" TASK 2: COMPARATIVE EVALUATION")
    print("="*50)

    # 1. Framework Logical Query Execution
    print("\n[A] Framework Logical Query")
    logical_query = {
        "operation": "read",
        "filter": {"status": "active"}
    }
    
    framework_times = []
    for _ in range(10):  # Run 10 times to get average
        _, t = measure_time(lambda: crud_engine.handle_request(logical_query))
        t *= 0.001
        framework_times.append(t)
    
    avg_framework_time = statistics.mean(framework_times)
    print(f"Average Execution Time (Framework) : {avg_framework_time:.6f} seconds")

    # 2. Direct MongoDB Query Execution
    print("\n[B] Direct MongoDB Query")
    
    direct_times = []
    # Using the underlying PyMongo connection directly
    db = mongo_handler.db 
    for _ in range(10):
        # We find inside root_data inside MongoDB depending on what the schema does
        _, t = measure_time(lambda: list(db.unstructured_data.find({"status": "active"})))
        direct_times.append(t)
        
    avg_direct_time = statistics.mean(direct_times)
    print(f"Average Execution Time (Direct DB): {avg_direct_time:.6f} seconds")

    # 3. Overhead Calculation
    overhead = ((avg_framework_time - avg_direct_time) / avg_direct_time * 100) if avg_direct_time > 0 else 0
    print("\n[C] Summary")
    print(f"Framework Overhead: {overhead:.2f}% ({(avg_framework_time - avg_direct_time):.6f} sec added per query)")


def test_framework_strengths(crud_engine, mongo_handler):
    print("\n" + "="*50)
    print(" TASK 3: TESTING FRAMEWORK SUPERIORITY (FEDERATION)")
    print("="*50)

    # Insert a complex mixed schema record
    complex_record = {
        "operation": "insert",
        "data": {
            "uuid": "test_unifed_123",
            "username": "multi_db_user",        # Mapped to SQL table 'root'
            "age": 30,                          # Mapped to SQL
            "unstructured_favorites": ["A", "B"]# Mapped to MongoDB Unstructured Data
        }
    }
    
    # 1. Ingest via Framework (splinters into databases)
    crud_engine.handle_request(complex_record)

    # 2. Querying Via Direct MongoDB
    print("\n[A] Querying MongoDB directly:")
    db_result = mongo_handler.db.unstructured_data.find_one({"uuid": "test_unifed_123"})
    if db_result:
        db_result.pop("_id", None) # Clean ObjectId for display
    print("Result:", db_result)
    # print("-> Notice how MongoDB ONLY has 'unstructured_favorites'. Where is the username and age? They are missing!")

    # 3. Querying Via Framework (Federated Re-construction)
    print("\n[B] Querying via Framework:")
    framework_result = crud_engine.handle_request({"operation": "read", "root_id": "test_unifed_123"})
    print("Result:", framework_result.get("data", framework_result))
    # print("-> Notice how the framework beautifully joined the SQL properties AND the MongoDB properties into one perfect document!")


if __name__ == "__main__":
    print("Initializing components...")
    sql_handler, mongo_handler, md_manager, crud_engine, txn_coordinator = setup_framework()
    
    # Generate test load
    data = generate_mock_data(NUM_RECORDS)
    
    try:
        run_performance_benchmarks(crud_engine, txn_coordinator, data)
        run_comparative_evaluation(crud_engine, mongo_handler)
        # test_framework_strengths(crud_engine, mongo_handler)
    finally:
        # Cleanup
        print("\nCleaning up test collections...")
        mongo_handler.db.unstructured_data.delete_many({"username": {"$regex": "^user_"}})
        mongo_handler.db.unstructured_data.delete_one({"uuid": "test_unifed_123"})
        try:
            from sqlalchemy import text
            with sql_handler.engine.begin() as conn:
                conn.execute(text("DELETE FROM root WHERE uuid='txn_1'"))
                conn.execute(text("DELETE FROM root WHERE uuid='test_unifed_123'"))
        except Exception as e:
            print("Failed to clean up SQL:", e)


            