"""
Tests for processor classes and their simplified architecture.
"""

import pytest
import logging
from unittest.mock import Mock, patch
from pathlib import Path

from src.processors.base_processor import BaseProcessor
from src.processors.domain_data_processor import DomainDataProcessor
from src.processors.codebook_processor import CodebookProcessor


class TestBaseProcessor:
    """Test the BaseProcessor abstract base class."""
    
    def test_base_processor_cannot_be_instantiated(self):
        """BaseProcessor is abstract and cannot be instantiated directly."""
        config = {"white_list": ["test"]}
        logger = logging.getLogger("test")
        
        with pytest.raises(TypeError):
            BaseProcessor(config, logger)
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_base_processor_initialization(self, mock_get_path_manager):
        """Test BaseProcessor initialization with mocked path manager."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {"white_list": ["col1", "col2"]}
        logger = logging.getLogger("test")
        
        # Create a concrete subclass for testing
        class ConcreteProcessor(BaseProcessor):
            def run_pre_processing(self):
                pass
            def run_inspection_processing(self, second_run=False):
                pass
        
        processor = ConcreteProcessor(config, logger)
        
        assert processor.config == config
        assert processor.logger == logger
        assert processor.white_list == ["col1", "col2"]
        assert processor.path_manager == mock_path_manager
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_path_access_methods(self, mock_get_path_manager):
        """Test that path access methods work correctly."""
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input"
        mock_path_manager.get_buffer_path.return_value = "/test/buffer"
        mock_path_manager.get_output_path.return_value = "/test/output"
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {"white_list": ["test"]}
        logger = logging.getLogger("test")
        
        class ConcreteProcessor(BaseProcessor):
            def run_pre_processing(self):
                pass
            def run_inspection_processing(self, second_run=False):
                pass
        
        processor = ConcreteProcessor(config, logger)
        
        assert processor.get_input_path("domain") == Path("/test/input")
        assert processor.get_buffer_path("filtered") == Path("/test/buffer")
        assert processor.get_output_path("final") == Path("/test/output")
        
        mock_path_manager.get_input_path.assert_called_with("domain")
        mock_path_manager.get_buffer_path.assert_called_with("filtered")
        mock_path_manager.get_output_path.assert_called_with("final")


class TestDomainDataProcessor:
    """Test the DomainDataProcessor class."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_domain_processor_initialization(self, mock_get_path_manager):
        """Test DomainDataProcessor initialization with new signature."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1", "col2"],
            "dd_inspections": {"test": {"active": True}},
            "parsing_options": {"add_id": True},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "mirror_input"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        assert processor.config == config
        assert processor.logger == logger
        assert processor.white_list == ["col1", "col2"]
        assert processor.dd_inspections == {"test": {"active": True}}
        assert processor.parsing_options == {"add_id": True}
        assert processor.csv_export_delimiter == ","
        assert processor.to_select == []
        assert processor.parsed_table is None
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_domain_processor_config_defaults(self, mock_get_path_manager):
        """Test DomainDataProcessor handles missing config values with defaults."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {},
            "parsing_options": {}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        assert processor.csv_export_delimiter == ","  # default
        assert processor.output_formats_and_batching == {"csv": "mirror_input"}  # default


class TestCodebookProcessor:
    """Test the CodebookProcessor class."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_codebook_processor_initialization(self, mock_get_path_manager):
        """Test CodebookProcessor initialization with new signature."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["key1", "key2"],
            "cb_inspections": {"test": {"active": True}},
            "key_export_ban": ["banned_key"],
            "select_parser": "test_parser",
            "append_new_metadata": True
        }
        logger = logging.getLogger("test")
        
        processor = CodebookProcessor(config, logger)
        
        assert processor.config == config
        assert processor.logger == logger
        assert processor.white_list == ["key1", "key2"]
        assert processor.cb_inspections == {"test": {"active": True}}
        assert processor.key_export_ban == ["banned_key"]
        assert processor.select_parser == "test_parser"
        assert processor.append_new_metadata is True


class TestProcessorIntegration:
    """Test processor integration and compatibility."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_both_processors_use_same_base(self, mock_get_path_manager):
        """Test that both processors inherit from BaseProcessor correctly."""
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input"
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["test"],
            "dd_inspections": {},
            "parsing_options": {},
            "cb_inspections": {},
            "key_export_ban": [],
            "select_parser": "test",
            "append_new_metadata": False
        }
        logger = logging.getLogger("test")
        
        dd_processor = DomainDataProcessor(config, logger)
        cb_processor = CodebookProcessor(config, logger)
        
        # Both should have BaseProcessor methods
        assert hasattr(dd_processor, 'get_input_path')
        assert hasattr(cb_processor, 'get_input_path')
        
        # Both should use the same path manager
        assert dd_processor.path_manager == cb_processor.path_manager
        
        # Both should have the same white_list
        assert dd_processor.white_list == cb_processor.white_list
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_processor_path_methods_consistency(self, mock_get_path_manager):
        """Test that path methods work consistently across processors."""
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input"
        mock_path_manager.get_buffer_path.return_value = "/test/buffer"
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["test"],
            "dd_inspections": {},
            "parsing_options": {},
            "cb_inspections": {},
            "key_export_ban": [],
            "select_parser": "test",
            "append_new_metadata": False
        }
        logger = logging.getLogger("test")
        
        dd_processor = DomainDataProcessor(config, logger)
        cb_processor = CodebookProcessor(config, logger)
        
        # Both should return the same paths
        assert dd_processor.get_input_path("domain") == cb_processor.get_input_path("domain")
        assert dd_processor.get_buffer_path("test") == cb_processor.get_buffer_path("test")
