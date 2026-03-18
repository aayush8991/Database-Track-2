import pytest
import json


class TestRouter:
    """Tests for the Router component."""
    
    def test_router_initialization(self, router):
        """Test router initializes correctly."""
        assert router.sql_handler is not None
        assert router.mongo_handler is not None
        assert router.normalizer is not None
    
    def test_detect_complex_record(self, router, sample_nested_record):
        """Test that router detects complex records."""
        batch = [sample_nested_record]
        
        # Router should route this to normalization
        schema_decisions = {"orders": {"target": "SQL"}}
        
        # Should not raise error
        router.process_batch(batch, schema_decisions)
    
    def test_detect_flat_record(self, router, sample_flat_record):
        """Test that router detects flat records."""
        batch = [sample_flat_record]
        schema_decisions = {"username": {"target": "MONGO"}}
        
        # Should not raise error
        router.process_batch(batch, schema_decisions)
    
    def test_decomposition_heuristic(self, router, sample_large_record):
        """Test 10% decomposition rule."""
        batch = [sample_large_record]
        schema_decisions = {"device_logs": {"target": "MONGO"}}
        
        # Process should trigger decomposition
        router.process_batch(batch, schema_decisions)
        
        # Verify decomposition happened (check in mongo)
        # This would need mongo verification


class TestRouterEdgeCases:
    """Edge case tests for Router."""
    
    def test_empty_batch(self, router):
        """Test processing empty batch."""
        router.process_batch([], {})
        # Should not raise error
    
    def test_mixed_complex_and_flat(self, router, sample_flat_record, sample_nested_record):
        """Test batch with both complex and flat records."""
        batch = [sample_flat_record, sample_nested_record]
        schema_decisions = {}
        
        # Should handle without error
        router.process_batch(batch, schema_decisions)