import pytest
from core.analyzer import Analyzer


class TestAnalyzer:
    """Tests for the Analyzer component."""
    
    def test_analyzer_initialization(self):
        """Test analyzer initializes correctly."""
        analyzer = Analyzer()
        assert analyzer.field_stats == {}
        assert "root" in analyzer.structure_map
        assert analyzer.total_records_processed == 0
    
    def test_analyze_single_flat_record(self, analyzer, sample_flat_record):
        """Test analyzing a single flat record."""
        analyzer.analyze_batch([sample_flat_record])
        
        assert analyzer.total_records_processed == 1
        assert "username" in analyzer.field_stats
        assert "email" in analyzer.field_stats
        assert analyzer.field_stats["username"]["count"] == 1
    
    def test_analyze_nested_record_structure(self, analyzer, sample_nested_record):
        """Test that structure map detects nested objects."""
        analyzer.analyze_batch([sample_nested_record])
        
        structure = analyzer.get_structure_map()
        
        # Should detect root and children
        assert "root" in structure
        assert "root_orders" in structure["root"]["children"]
        assert "root_comments" in structure["root"]["children"]
    
    def test_analyze_batch_multiple_records(self, analyzer, batch_of_records):
        """Test analyzing multiple records in a batch."""
        analyzer.analyze_batch(batch_of_records)
        
        assert analyzer.total_records_processed == 2
        assert "username" in analyzer.field_stats
        assert analyzer.field_stats["username"]["count"] == 2
    
    def test_get_schema_stats(self, analyzer, sample_flat_record):
        """Test schema stats generation."""
        analyzer.analyze_batch([sample_flat_record])
        
        stats = analyzer.get_schema_stats()
        
        assert isinstance(stats, dict)
        assert "username" in stats
        assert "frequency_ratio" in stats["username"]
        assert stats["username"]["frequency_ratio"] == 1.0
        assert stats["username"]["type_stability"] == "stable"
    
    def test_field_type_stability(self, analyzer):
        """Test detection of type stability."""
        records = [
            {"field": "text"},
            {"field": 123},  # Different type
            {"field": "more text"}
        ]
        
        analyzer.analyze_batch(records)
        stats = analyzer.get_schema_stats()
        
        assert stats["field"]["type_stability"] == "unstable"
    
    def test_unique_ratio_calculation(self, analyzer):
        """Test unique value ratio calculation."""
        records = [
            {"status": "active"},
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"}
        ]
        
        analyzer.analyze_batch(records)
        stats = analyzer.get_schema_stats()
        
        # 2 unique values out of 4
        assert abs(stats["status"]["unique_ratio"] - 0.5) < 0.01
    
    def test_thread_safety_structure_map(self, analyzer):
        """Test structure map thread safety."""
        # This is a basic test - in production, use threading tests
        record1 = {"data": [{"nested": "value"}]}
        record2 = {"info": {"key": "val"}}
        
        analyzer.analyze_batch([record1, record2])
        structure = analyzer.get_structure_map()
        
        assert "root" in structure
        assert isinstance(structure["root"]["columns"], list)


class TestAnalyzerEdgeCases:
    """Edge case tests for Analyzer."""
    
    def test_empty_batch(self, analyzer):
        """Test analyzing empty batch."""
        analyzer.analyze_batch([])
        assert analyzer.total_records_processed == 0
    
    def test_record_with_null_values(self, analyzer):
        """Test handling null values."""
        record = {"field1": None, "field2": "value"}
        analyzer.analyze_batch([record])
        
        assert "field1" in analyzer.field_stats
        assert analyzer.field_stats["field1"]["count"] == 1
    
    def test_deeply_nested_structure(self, analyzer):
        """Test handling deeply nested structures."""
        record = {
            "level1": [
                {
                    "level2": [
                        {"level3": "value"}
                    ]
                }
            ]
        }
        analyzer.analyze_batch([record])
        
        structure = analyzer.get_structure_map()
        assert "root_level1" in structure["root"]["children"]