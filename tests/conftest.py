import pytest
import os
import sys
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.analyzer import Analyzer
from core.normalizer import Normalizer
from core.router import Router
from core.metadata_manager import MetadataManager
from core.crud_engine import CRUDEngine
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler


# --- Sample Data Fixtures ---

@pytest.fixture
def sample_flat_record():
    """Simple flat record without nesting."""
    return {
        "username": "john_doe",
        "email": "john@example.com",
        "age": 28,
        "timestamp": datetime.now().isoformat(),
        "country": "USA"
    }


@pytest.fixture
def sample_nested_record():
    """Complex record with lists of objects (triggers normalization)."""
    return {
        "user_id": "user_123",
        "username": "jane_smith",
        "profile": {
            "bio": "Software Engineer",
            "location": "San Francisco"
        },
        "orders": [
            {
                "order_id": "order_001",
                "amount": 99.99,
                "items": [
                    {"product": "Laptop", "qty": 1},
                    {"product": "Mouse", "qty": 2}
                ]
            },
            {
                "order_id": "order_002",
                "amount": 49.99,
                "items": [
                    {"product": "Keyboard", "qty": 1}
                ]
            }
        ],
        "comments": [
            {"text": "Great product!", "rating": 5},
            {"text": "Good value", "rating": 4}
        ]
    }


@pytest.fixture
def sample_large_record():
    """Record designed to trigger decomposition (>1KB with large fields)."""
    large_logs = [{"log_entry": f"Entry {i}", "data": "x" * 100} for i in range(50)]
    return {
        "user_id": "user_456",
        "username": "bob_wilson",
        "device_logs": large_logs,  # This should trigger 10% decomposition
        "basic_info": "Some basic data"
    }


@pytest.fixture
def batch_of_records(sample_flat_record, sample_nested_record):
    """Batch of mixed records."""
    return [sample_flat_record, sample_nested_record]


# --- Component Fixtures ---

@pytest.fixture
def analyzer():
    """Initialized Analyzer instance."""
    return Analyzer()


@pytest.fixture
def normalizer():
    """Initialized Normalizer instance."""
    return Normalizer()


@pytest.fixture
def metadata_manager(tmp_path):
    """Metadata manager with temporary file."""
    test_file = tmp_path / "test_schema.json"
    return MetadataManager(str(test_file))


@pytest.fixture
def sql_handler():
    """SQL Handler (assumes .env is configured)."""
    try:
        handler = SQLHandler()
        yield handler
        # Cleanup
        handler.reset_db()
    except Exception as e:
        pytest.skip(f"SQL Handler not available: {e}")


@pytest.fixture
def mongo_handler():
    """Mongo Handler (assumes .env is configured)."""
    try:
        handler = MongoHandler()
        yield handler
        # Cleanup - drop all collections
        if handler.db:
            handler.db.client.drop_database(handler.db.name)
    except Exception as e:
        pytest.skip(f"Mongo Handler not available: {e}")


@pytest.fixture
def router(sql_handler, mongo_handler, analyzer):
    """Router with all handlers."""
    return Router(sql_handler, mongo_handler, analyzer)


@pytest.fixture
def crud_engine(sql_handler, mongo_handler, metadata_manager):
    """CRUD Engine for query testing."""
    return CRUDEngine(sql_handler, mongo_handler, metadata_manager)