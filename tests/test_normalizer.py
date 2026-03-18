import pytest
from core.normalizer import Normalizer


class TestNormalizer:
    """Tests for the Normalizer component."""
    
    def test_to_snake_case(self, normalizer):
        """Test snake_case conversion."""
        assert normalizer._to_snake_case("userName") == "user_name"
        assert normalizer._to_snake_case("UserName") == "user_name"
        assert normalizer._to_snake_case("user_name") == "user_name"
        assert normalizer._to_snake_case("deviceType") == "device_type"
    
    def test_normalize_flat_record(self, normalizer, sample_flat_record):
        """Test normalizing a flat record."""
        result = normalizer.normalize_record(sample_flat_record)
        
        assert "username" in result
        assert result["username"] == sample_flat_record["username"]
    
    def test_shred_nested_record(self, normalizer, sample_nested_record):
        """Test shredding a nested record into multiple tables."""
        result = normalizer.shred_record(sample_nested_record)
        
        # Should create root, root_orders, root_comments tables
        assert "root" in result
        assert "root_orders" in result
        assert "root_comments" in result
        
        # Root should have one record
        assert len(result["root"]) == 1
        
        # Orders and comments should have multiple records
        assert len(result["root_orders"]) == 2
        assert len(result["root_comments"]) == 2
    
    def test_uuid_generation(self, normalizer, sample_nested_record):
        """Test that UUIDs are generated for linking."""
        result = normalizer.shred_record(sample_nested_record)
        
        # Root should have uuid
        root_id = result["root"][0]["uuid"]
        assert root_id is not None
        assert len(root_id) == 36  # UUID length
        
        # Child records should reference parent
        for order in result["root_orders"]:
            assert "root_id" in order
            assert order["root_id"] == root_id
    
    def test_nested_dict_flattening(self, normalizer):
        """Test that nested dicts are flattened."""
        record = {
            "user": "john",
            "profile": {
                "bio": "Engineer",
                "location": "NYC"
            }
        }
        
        result = normalizer.shred_record(record)
        root_record = result["root"][0]
        
        assert "profile_bio" in root_record
        assert root_record["profile_bio"] == "Engineer"
        assert "profile_location" in root_record
    
    def test_timestamp_addition(self, normalizer, sample_nested_record):
        """Test that sys_ingested_at is added."""
        result = normalizer.shred_record(sample_nested_record)
        
        root_record = result["root"][0]
        assert "sys_ingested_at" in root_record


class TestNormalizerEdgeCases:
    """Edge case tests for Normalizer."""
    
    def test_empty_lists(self, normalizer):
        """Test handling empty lists."""
        record = {"items": []}
        result = normalizer.shred_record(record)
        
        assert "root" in result
        assert len(result["root"]) == 1
    
    def test_primitive_lists(self, normalizer):
        """Test handling lists of primitives."""
        record = {"tags": ["tag1", "tag2", "tag3"]}
        result = normalizer.shred_record(record)
        
        # Primitive lists shouldn't create child tables
        assert "root_tags" not in result
        assert "tags" in result["root"][0]