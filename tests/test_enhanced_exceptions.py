"""
Tests for enhanced exception functionality with LLM-friendly debugging info.
"""

import pytest
from src.shared.exceptions import (
    ProcessingError,
    ParsingError, 
    ConfigurationError, 
    EditError,
    InspectionError,
    ExportError
)


class TestEnhancedExceptions:
    """Test enhanced exception classes with file paths and data context."""
    
    def test_basic_exception_without_context(self):
        """Test exception with just a message."""
        with pytest.raises(ParsingError) as exc_info:
            raise ParsingError("Failed to parse CSV file")
        
        assert str(exc_info.value) == "Failed to parse CSV file"
        assert exc_info.value.file_path is None
        assert exc_info.value.data_context is None
    
    def test_exception_with_file_path(self):
        """Test exception with file path context."""
        with pytest.raises(ConfigurationError) as exc_info:
            raise ConfigurationError(
                "Invalid configuration key", 
                file_path="/home/user/config.toml"
            )
        
        expected = "Invalid configuration key | File: /home/user/config.toml"
        assert str(exc_info.value) == expected
        assert exc_info.value.file_path == "/home/user/config.toml"
        assert exc_info.value.data_context is None
    
    def test_exception_with_data_context(self):
        """Test exception with data context only."""
        with pytest.raises(EditError) as exc_info:
            raise EditError(
                "Edit function failed",
                data_context="Column: age, Edit: normalize_values"
            )
        
        expected = "Edit function failed | Context: Column: age, Edit: normalize_values"
        assert str(exc_info.value) == expected
        assert exc_info.value.file_path is None
        assert exc_info.value.data_context == "Column: age, Edit: normalize_values"
    
    def test_exception_with_full_context(self):
        """Test exception with both file path and data context."""
        with pytest.raises(InspectionError) as exc_info:
            raise InspectionError(
                "Inspection failed to process data",
                file_path="/data/input/dataset.csv",
                data_context="Inspection: length_map, Rows: 1000, Columns: 5"
            )
        
        expected = ("Inspection failed to process data | "
                   "File: /data/input/dataset.csv | "
                   "Context: Inspection: length_map, Rows: 1000, Columns: 5")
        assert str(exc_info.value) == expected
        assert exc_info.value.file_path == "/data/input/dataset.csv"
        assert exc_info.value.data_context == "Inspection: length_map, Rows: 1000, Columns: 5"
    
    def test_exception_inheritance(self):
        """Test that all exceptions inherit from ProcessingError."""
        exceptions = [
            ParsingError("test"),
            ConfigurationError("test"),
            EditError("test"),
            InspectionError("test"),
            ExportError("test")
        ]
        
        for exc in exceptions:
            assert isinstance(exc, ProcessingError)
    
    def test_debug_info_in_docstrings(self):
        """Test that all exceptions have debug info in their docstrings."""
        exceptions_with_debug = [
            ParsingError,
            ConfigurationError, 
            EditError,
            InspectionError,
            ExportError
        ]
        
        for exc_class in exceptions_with_debug:
            assert "Debug:" in exc_class.__doc__
            assert len(exc_class.__doc__.split("Debug:")[1].strip()) > 10  # Has meaningful debug info
