import pytest
import json
import os


class TestMetadataManager:
    """Tests for the MetadataManager component."""
    
    def test_initialization(self, metadata_manager):
        """Test metadata manager initializes correctly."""
        assert metadata_manager.global_schema is not None
        assert "relational_structure" in metadata_manager.global_schema
        assert "tables" in metadata_manager.global_schema["relational_structure"]
    
    def test_save_metadata(self, metadata_manager):
        """Test saving metadata to file."""
        metadata_manager.global_schema["test_field"] = "test_value"
        metadata_manager.save_metadata()
        
        # Verify file exists
        assert os.path.exists(metadata_manager.filepath)
    
    def test_load_metadata(self, metadata_manager):
        """Test loading metadata from file."""
        # First save some data
        metadata_manager.global_schema["custom"] = "data"
        metadata_manager.save_metadata()
        
        # Create new instance and verify it loaded
        new_manager = type(metadata_manager)(metadata_manager.filepath)
        assert "custom" in new_manager.global_schema
    
    def test_sync_analyzer(self, metadata_manager, analyzer, sample_nested_record):
        """Test syncing analyzer data."""
        analyzer.analyze_batch([sample_nested_record])
        
        metadata_manager.sync_analyzer(analyzer)
        
        # Verify structure was synced
        tables = metadata_manager.global_schema["relational_structure"]["tables"]
        assert "root" in tables
    
    def test_sync_router(self, metadata_manager, router):
        """Test syncing router decisions."""
        decisions = {"field1": {"target": "SQL"}, "field2": {"target": "MONGO"}}
        router.previous_decisions = decisions
        
        metadata_manager.sync_router(router)
        
        assert metadata_manager.global_schema["field_routing"] == decisions
    
    def test_get_table_info(self, metadata_manager, analyzer, sample_nested_record):
        """Test retrieving table info."""
        analyzer.analyze_batch([sample_nested_record])
        metadata_manager.sync_analyzer(analyzer)
        
        info = metadata_manager.get_table_info("root")
        assert "columns" in info
        assert "children" in info
    
    def test_get_field_route(self, metadata_manager, router):
        """Test retrieving field routing info."""
        router.previous_decisions = {"username": {"target": "SQL"}}
        metadata_manager.sync_router(router)
        
        route = metadata_manager.get_field_route("username")
        assert route == "SQL"


class TestMetadataManagerEdgeCases:
    """Edge case tests for MetadataManager."""
    
    def test_save_with_nonexistent_directory(self, tmp_path):
        """Test saving to nonexistent directory."""
        from core.metadata_manager import MetadataManager
        
        filepath = tmp_path / "subdir" / "metadata.json"
        manager = MetadataManager(str(filepath))
        
        manager.save_metadata()
        assert os.path.exists(filepath)
    
    def test_load_nonexistent_file(self, tmp_path):
        """Test loading from nonexistent file."""
        from core.metadata_manager import MetadataManager
        
        filepath = tmp_path / "nonexistent.json"
        manager = MetadataManager(str(filepath))
        
        # Should not raise error, just initialize with defaults
        assert manager.global_schema is not None