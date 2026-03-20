import pytest
from core.crud_engine import CRUDEngine


class TestCRUDEngine:
    """Tests for the CRUD Engine component."""
    
    def test_crud_initialization(self, crud_engine):
        """Test CRUD engine initializes correctly."""
        assert crud_engine.sql is not None
        assert crud_engine.mongo is not None
        assert crud_engine.meta is not None
    
    def test_handle_request_invalid_operation(self, crud_engine):
        """Test handling invalid operation."""
        request = {"operation": "invalid"}
        result = crud_engine.handle_request(request)
        print("[test_handle_request_invalid_operation] result:", result)
        assert result["status"] == "error"
        assert "not supported" in result["message"]
    
    def test_read_missing_root_id(self, crud_engine):
        """Test read without root_id."""
        request = {"operation": "read"}
        result = crud_engine.handle_request(request)
        print("[test_read_missing_root_id] result:", result)
        assert result["status"] == "error"
        # Should have an error (either missing root_id/filter or metadata not found)
        assert result["message"]
    
    def test_delete_missing_root_id(self, crud_engine):
        """Test delete without root_id."""
        request = {"operation": "delete"}
        result = crud_engine.handle_request(request)
        print("[test_delete_missing_root_id] result:", result)
        assert result["status"] == "error"
        assert "root_id" in result["message"]
    
    def test_read_nonexistent_record(self, crud_engine):
        """Test reading nonexistent record."""
        request = {"operation": "read", "root_id": "nonexistent_uuid"}
        result = crud_engine.handle_request(request)
        print("[test_read_nonexistent_record] result:", result)
        # Should return 404 or error
        assert result["status"] in ["404", "error"]


class TestCRUDEngineIntegration:
    """Integration tests for CRUD Engine."""
    
    def test_read_after_insert(self, crud_engine, router, metadata_manager, analyzer, sample_nested_record):
        """Test reading a record after inserting it."""
        # This is an integration test
        pytest.skip("Requires full integration setup")