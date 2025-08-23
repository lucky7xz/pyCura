"""
Tests for core data processing logic - path-agnostic tests focusing on 
data transformations, ordering, and structure that should remain stable.
"""
import pytest
import polars as pl
from pathlib import Path
import tempfile
import json

# Import the processing modules we want to test
from src.processing_modules.edits.apply_padding import apply_padding
from src.processing_modules.edits.apply_case import apply_case
from src.processing_modules.edits.apply_char_replace import apply_char_replace
from src.processing_modules.inspections.length_map import length_map
from src.processing_modules.inspections.char_map import char_map
from src.processing_modules.inspections.occurrence_map import occurrence_map
from src.shared.utils import filter_by_whitelist, merge_dicts, sort_whitelist


class TestDataTransformations:
    """Test core data transformation functions that work on data structures."""
    
    def test_apply_padding_polars(self):
        """Test padding functionality on Polars LazyFrame."""
        # Create test data
        df = pl.LazyFrame({
            "test_col": ["1", "22", "333", "4444"]
        })
        
        # Apply padding
        result = apply_padding((df, "test_col"), "5", "0")
        collected = result.collect()
        
        # Check results
        expected = ["00001", "00022", "00333", "04444"]
        assert collected["test_col"].to_list() == expected
    
    def test_apply_padding_codebook(self):
        """Test padding on codebook dictionary structure."""
        test_data = {
            "1": "value1",
            "22": "value2", 
            "333": "value3"
        }
        
        result = apply_padding(test_data, "4", "0")
        
        expected_keys = ["0001", "0022", "0333"]
        assert list(result.keys()) == expected_keys
        assert list(result.values()) == ["value1", "value2", "value3"]

    def test_apply_case_polars(self):
        """Test case transformation on Polars data."""
        df = pl.LazyFrame({
            "test_col": ["Hello", "WORLD", "MiXeD"]
        })
        
        # Test uppercase
        result_upper = apply_case((df, "test_col"), "upper")
        assert result_upper.collect()["test_col"].to_list() == ["HELLO", "WORLD", "MIXED"]
        
        # Test lowercase  
        result_lower = apply_case((df, "test_col"), "lower")
        assert result_lower.collect()["test_col"].to_list() == ["hello", "world", "mixed"]


class TestInspectionFunctions:
    """Test data inspection functions that analyze data structure."""
    
    def test_length_map_polars(self):
        """Test length mapping on Polars data."""
        df = pl.LazyFrame({
            "col1": ["a", "bb", "ccc", "dd"],
            "col2": ["x", "yy", "zzz", "w"]
        })
        
        result = length_map((df, ["col1", "col2"]), False)
        
        # Check col1 length distribution
        assert result["col1"][1] == 1  # one string of length 1
        assert result["col1"][2] == 2  # two strings of length 2
        assert result["col1"][3] == 1  # one string of length 3
        
        # Check col2 length distribution - now 4 items total
        assert result["col2"][1] == 2  # "x" and "w" 
        assert result["col2"][2] == 1  # "yy"
        assert result["col2"][3] == 1  # "zzz"

    def test_length_map_codebook(self):
        """Test length mapping on codebook structure."""
        test_codebook = {
            "data": {
                "col1": {
                    "": "empty",
                    "000": "Germany",
                    "010": "Saarland"
                }
            }
        }
        
        result = length_map(test_codebook, False)
        
        # Should count key lengths, not values
        expected = {
            "col1": {
                0: 1,  # empty string
                3: 2   # two 3-character keys
            }
        }
        assert result == expected

    def test_char_map_polars(self):
        """Test character mapping - returns unique characters, not frequencies."""
        df = pl.LazyFrame({
            "test_col": ["abc", "aab", "xyz"]
        })
        
        result = char_map((df, ["test_col"]), False)
        
        # char_map returns unique characters as a sorted list, not frequencies
        unique_chars = result["test_col"]
        expected_chars = sorted(list(set("abcaabxyz")))  # ['a', 'b', 'c', 'x', 'y', 'z']
        assert unique_chars == expected_chars


class TestUtilityFunctions:
    """Test utility functions for data manipulation."""
    
    def test_sort_whitelist(self):
        """Test natural sorting of whitelist."""
        unsorted = ["A10", "A2", "A1", "B1", "A20"]
        result = sort_whitelist(unsorted)
        expected = ["A1", "A2", "A10", "A20", "B1"]
        assert result == expected

    def test_filter_by_whitelist_codebook(self):
        """Test filtering codebook by whitelist."""
        test_data = {
            "data": {
                "col1": {"key1": "val1"},
                "col2": {"key2": "val2"},
                "col3": {"key3": "val3"}
            },
            "metadata": {
                "col1": {"meta1": "data1"},
                "col2": {"meta2": "data2"}, 
                "col3": {"meta3": "data3"}
            }
        }
        
        whitelist = ["col1", "col3"]
        result = filter_by_whitelist(test_data, whitelist)
        
        assert set(result["data"].keys()) == {"col1", "col3"}
        assert set(result["metadata"].keys()) == {"col1", "col3"}
        assert "col2" not in result["data"]
        assert "col2" not in result["metadata"]

    def test_merge_dicts(self):
        """Test dictionary merging for inspections."""
        dict1 = {
            "col1": {"existing": "data"},
            "col2": {"more": "data"}
        }
        
        dict2 = {
            "col1": {"new": "inspection"},
            "col2": {"another": "inspection"}
        }
        
        result = merge_dicts(dict1, dict2, "PROCESSED")
        
        # Check structure
        assert "existing" in result["col1"]
        assert "PROCESSED" in result["col1"]
        assert result["col1"]["PROCESSED"]["new"] == "inspection"


class TestDataStructureConsistency:
    """Test that data structures remain consistent across transformations."""
    
    def test_polars_dataframe_schema_preservation(self):
        """Test that column schemas are preserved through transformations."""
        original_df = pl.LazyFrame({
            "col1": ["a", "b", "c"],
            "col2": ["x", "y", "z"],
            "file_name": ["file1.csv", "file1.csv", "file1.csv"]
        })
        
        # Apply transformation
        transformed = apply_padding((original_df, "col1"), "3", "0")
        
        # Schema should be preserved
        original_schema = original_df.collect_schema()
        transformed_schema = transformed.collect_schema()
        
        assert original_schema.names() == transformed_schema.names()
        assert len(original_schema) == len(transformed_schema)

    def test_codebook_structure_preservation(self):
        """Test that codebook structure is preserved."""
        original_codebook = {
            "data": {
                "col1": {"1": "val1", "2": "val2"}
            },
            "metadata": {
                "col1": {"info": "test"}
            }
        }
        
        # Apply transformation to data part only
        transformed_data = apply_padding(original_codebook["data"]["col1"], "3", "0")
        
        # Structure should be maintained
        assert isinstance(transformed_data, dict)
        assert len(transformed_data) == 2
        assert "001" in transformed_data
        assert "002" in transformed_data

    def test_inspection_output_structure(self):
        """Test that inspection outputs have consistent structure."""
        df = pl.LazyFrame({
            "col1": ["test", "data"],
            "col2": ["more", "test"]
        })
        
        # Run multiple inspections
        length_result = length_map((df, ["col1", "col2"]), False)
        char_result = char_map((df, ["col1", "col2"]), False)
        
        # Both should have same column keys
        assert set(length_result.keys()) == set(char_result.keys())
        assert set(length_result.keys()) == {"col1", "col2"}
        
        # length_map returns dict of dicts, char_map returns dict of lists
        for col in ["col1", "col2"]:
            assert isinstance(length_result[col], dict)
            assert isinstance(char_result[col], list)  # char_map returns lists


class TestEdgeCases:
    """Test edge cases that might break during rapid development."""
    
    def test_empty_data_handling(self):
        """Test handling of empty datasets."""
        # Create empty dataframe with proper string schema
        empty_df = pl.LazyFrame({"col1": []}, schema={"col1": pl.String})
        
        # Should not crash on empty data
        result = length_map((empty_df, ["col1"]), False)
        assert "col1" in result
        assert isinstance(result["col1"], dict)

    def test_single_row_data(self):
        """Test handling of single-row datasets."""
        single_df = pl.LazyFrame({"col1": ["test"]})
        
        result = length_map((single_df, ["col1"]), False)
        assert result["col1"][4] == 1  # "test" has length 4

    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        unicode_df = pl.LazyFrame({
            "col1": ["café", "naïve", "résumé"]
        })
        
        result = char_map((unicode_df, ["col1"]), False)
        
        # Should handle unicode properly
        assert "é" in result["col1"]
        assert "ï" in result["col1"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
