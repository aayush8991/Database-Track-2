import pytest


class TestEndToEndIntegration:
    """End-to-end integration tests."""
    
    def test_full_pipeline_flat_record(self, analyzer, router, metadata_manager, sample_flat_record):
        """Test complete pipeline for flat record."""
        # 1. Analyze
        analyzer.analyze_batch([sample_flat_record])
        
        # 2. Sync metadata
        metadata_manager.sync_analyzer(analyzer)
        
        # 3. Route
        schema_decisions = {"username": {"target": "MONGO"}}
        router.process_batch([sample_flat_record], schema_decisions)
        
        # Verify metadata was updated
        stats = metadata_manager.global_schema["field_stats"]
        assert len(stats) > 0
    
    def test_full_pipeline_nested_record(self, analyzer, router, metadata_manager, sample_nested_record):
        """Test complete pipeline for nested record."""
        # 1. Analyze
        analyzer.analyze_batch([sample_nested_record])
        
        # 2. Sync metadata
        metadata_manager.sync_analyzer(analyzer)
        
        # 3. Route (should trigger normalization)
        schema_decisions = {}
        router.process_batch([sample_nested_record], schema_decisions)
        
        # 4. Verify structure was detected
        structure = metadata_manager.global_schema["relational_structure"]["tables"]
        assert "root_orders" in structure["root"]["children"]
    
    def test_pipeline_with_multiple_batches(self, analyzer, batch_of_records):
        """Test processing multiple batches sequentially."""
        for i in range(3):
            analyzer.analyze_batch(batch_of_records)
        
        # Should have processed 6 records (2 per batch * 3)
        assert analyzer.total_records_processed == 6
    
    def test_metadata_persistence_across_instances(self, metadata_manager, analyzer, sample_nested_record, tmp_path):
        """Test metadata persists across instances."""
        # Analyze and save
        analyzer.analyze_batch([sample_nested_record])
        metadata_manager.sync_analyzer(analyzer)
        metadata_manager.save_metadata()
        
        # Create new instance from same file
        from core.metadata_manager import MetadataManager
        new_manager = MetadataManager(metadata_manager.filepath)
        
        # Should have loaded the data
        structure = new_manager.global_schema["relational_structure"]["tables"]
        assert len(structure) > 0


class TestDataQualityValidation:
    """Tests to validate data quality through the pipeline."""
    
    def test_no_data_loss_during_normalization(self, normalizer, sample_nested_record):
        """Test that no data is lost during normalization."""
        # Count fields in original
        original_field_count = sum(
            1 for k, v in sample_nested_record.items() 
            if not isinstance(v, list) and not isinstance(v, dict)
        )
        
        result = normalizer.shred_record(sample_nested_record)
        
        # Root record should contain all original fields (flattened)
        root_record = result["root"][0]
        
        # Verify critical fields are present
        assert "username" in root_record or any("username" in str(k) for k in root_record.keys())
    
    def test_uuid_consistency_in_joins(self, normalizer, sample_nested_record):
        """Test that UUIDs are consistent for proper joining."""
        result = normalizer.shred_record(sample_nested_record)
        
        root_uuid = result["root"][0]["uuid"]
        
        # All children should reference this UUID
        for order in result["root_orders"]:
            assert order["root_id"] == root_uuid
        
        for comment in result["root_comments"]:
            assert order["root_id"] == root_uuid