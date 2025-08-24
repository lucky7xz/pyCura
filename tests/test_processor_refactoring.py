"""
Tests for processor refactoring - covering issues that broke during the simplification.
"""

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import polars as pl

from src.processors.domain_data_processor import DomainDataProcessor, EditError, InspectionError
from src.processors.codebook_processor import CodebookProcessor
from src.shared.project_manager import ProjectManager


class TestProcessorInitialization:
    """Test processor initialization with new simplified signature."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_domain_processor_new_signature(self, mock_get_path_manager):
        """Test DomainDataProcessor can be initialized with (config, logger) signature."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1", "col2"],
            "dd_inspections": {"test_inspection": {"active": True}},
            "parsing_options": {"add_id": True},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        assert processor.config == config
        assert processor.logger == logger
        assert processor.dd_inspections == config["dd_inspections"]
        assert processor.parsing_options == config["parsing_options"]
        assert processor.csv_export_delimiter == ","
        assert processor.output_formats_and_batching == {"csv": "monolith"}
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_codebook_processor_new_signature(self, mock_get_path_manager):
        """Test CodebookProcessor can be initialized with (config, logger) signature."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1", "col2"],
            "cb_inspections": {"test_inspection": {"active": True}},
            "parsing_options": {"add_id": True},
            "append_new_metadata": False,
            "key_export_ban": [],
            "select_parser": "test_parser"
        }
        logger = logging.getLogger("test")
        
        processor = CodebookProcessor(config, logger)
        
        assert processor.config == config
        assert processor.logger == logger
        assert processor.cb_inspections == config["cb_inspections"]
        assert processor.append_new_metadata == False
        assert processor.key_export_ban == []
        assert processor.select_parser == "test_parser"


class TestEditFunctionImports:
    """Test edit function imports and execution."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_edit_function_import_success(self, mock_get_path_manager):
        """Test successful import and execution of edit functions."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1", "col2"],
            "dd_inspections": {},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        # Create test data
        test_data = pl.LazyFrame({
            "col1": ["a", "b", "c"],
            "file_name": ["file1.csv", "file2.csv", "file3.csv"]
        })
        processor.parsed_table = test_data
        
        # Mock the edit function
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_edit_func = Mock(return_value=test_data.with_columns(pl.lit("2023").alias("year")))
            mock_module.append_column = mock_edit_func
            mock_import.return_value = mock_module
            
            # Test edit execution
            processor.run_edit("year", "append_column", ["file_name", r".*(\d{4}).*"])
            
            # Verify import was called with correct path
            mock_import.assert_called_with("src.processing_modules.edits.append_column")
            mock_edit_func.assert_called_once()
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_edit_function_import_failure(self, mock_get_path_manager):
        """Test handling of edit function import failures."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        processor.parsed_table = pl.LazyFrame({"col1": ["a", "b"]})
        
        # Mock import failure
        with patch('importlib.import_module', side_effect=ImportError("Module not found")):
            with pytest.raises(EditError, match="Failed to import edit function"):
                processor.run_edit("new_col", "nonexistent_edit", ["param1"])
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_edit_preserves_file_name_column(self, mock_get_path_manager):
        """Test that file_name column is preserved during filtering."""
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = Path("/test/input")
        mock_path_manager.get_buffer_path.return_value = Path("/test/buffer")
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        # Create test data with file_name column
        test_data = pl.LazyFrame({
            "col1": ["a", "b", "c"],
            "col2": ["x", "y", "z"],
            "file_name": ["file1.csv", "file2.csv", "file3.csv"]
        })
        
        # Mock the parsing method and export method
        with patch.object(processor, '_parse_domain_data', return_value=test_data), \
             patch.object(processor, '_export_to_buffer'):
            processor.run_pre_processing()
            
            # Verify file_name was preserved even though not in whitelist
            schema_names = processor.parsed_table.collect_schema().names()
            assert "file_name" in schema_names
            assert "col1" in schema_names
            assert "col2" not in schema_names  # Should be filtered out


class TestInspectionFunctionImports:
    """Test inspection function imports."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_inspection_function_import_success(self, mock_get_path_manager):
        """Test successful import of inspection functions."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {"length_map": {"active": True}},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        processor.parsed_table = pl.LazyFrame({"col1": ["a", "bb", "ccc"]})
        
        # Mock the inspection function
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_inspection_func = Mock(return_value={"col1": {"1": 1, "2": 1, "3": 1}})
            mock_module.length_map = mock_inspection_func
            mock_import.return_value = mock_module
            
            # Test inspection execution
            processor.run_inspection_processing(second_run=False)
            
            # Verify import was called with correct path
            mock_import.assert_called_with("src.processing_modules.inspections.length_map")
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_inspection_function_import_failure(self, mock_get_path_manager):
        """Test handling of inspection function import failures."""
        mock_path_manager = Mock()
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {"nonexistent_inspection": {"active": True}},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        processor.parsed_table = pl.LazyFrame({"col1": ["a", "b"]})
        
        # Mock import failure - need to patch importlib at the module level
        with patch('importlib.import_module', side_effect=ImportError("Module not found")):
            # The function catches the error and logs it, continues processing
            processor.run_inspection_processing(second_run=False)
            # Verify the inspection was skipped (no exception raised)


class TestPathResolution:
    """Test path resolution in processors."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_processor_uses_base_class_path_methods(self, mock_get_path_manager):
        """Test that processors use base class path methods correctly."""
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input"
        mock_path_manager.get_buffer_path.return_value = "/test/buffer"
        mock_path_manager.get_output_path.return_value = "/test/output"
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        
        # Test path method calls
        input_path = processor.get_input_path("domain")
        buffer_path = processor.get_buffer_path("filtered_dd_mirror")
        output_path = processor.get_output_path("domain")
        
        assert input_path == Path("/test/input")
        assert buffer_path == Path("/test/buffer")
        assert output_path == Path("/test/output")
        
        # Verify the path manager was called correctly
        mock_path_manager.get_input_path.assert_called_with("domain")
        mock_path_manager.get_buffer_path.assert_called_with("filtered_dd_mirror")
        mock_path_manager.get_output_path.assert_called_with("domain")


class TestProjectManagerIntegration:
    """Test ProjectManager integration with new processor signatures."""
    
    @patch('src.shared.project_manager.initialize_path_manager')
    @patch('src.shared.project_manager.Path.exists')
    @patch('src.processors.base_processor.get_path_manager')
    def test_project_manager_processor_creation(self, mock_get_path_manager, mock_exists, mock_init_path_manager):
        """Test that ProjectManager creates processors with correct signatures."""
        # Setup mocks
        mock_exists.return_value = True
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input"
        mock_path_manager.get_output_path.return_value = "/test/output"
        mock_path_manager.get_buffer_path.return_value = "/test/buffer"
        mock_init_path_manager.return_value = mock_path_manager
        mock_get_path_manager.return_value = mock_path_manager
        
        # Mock config loading
        config_data = {
            'project_name': 'test_project',
            'domain_foldername': 'test_domain',
            'white_list': ['col1', 'col2'],
            'parsing_options': {'add_id': True},
            'dd_inspections': {'test_inspection': {'active': True}},
            'cb_inspections': {'test_cb_inspection': {'active': True}},
            'key_export_ban': [],
            'select_parser': 'test_parser',
            'append_new_metadata': False,
            'edits': ['test_edit'],
            'csv_export_delimiter': ',',
            'output_formats_and_batching': {'csv': True}
        }
        
        with patch('builtins.open', create=True) as mock_open, \
             patch('src.shared.project_manager.tomllib.loads', return_value=config_data):
            # Mock the file read to return a valid TOML string
            mock_open.return_value.__enter__.return_value.read.return_value = """
[project]
name = "test_project"
"""

            pm = ProjectManager('test_config.toml')
            
            # Test processor creation
            dd_processor = pm.get_domain_data_processor()
            cb_processor = pm.get_codebook_processor()
            
            # Verify processors were created with correct config and logger
            assert dd_processor.config == pm.config
            assert dd_processor.logger == pm.logger
            assert cb_processor.config == pm.config
            assert cb_processor.logger == pm.logger
            
            # Verify domain-specific config was set correctly
            assert dd_processor.dd_inspections == config_data['dd_inspections']
            assert dd_processor.parsing_options == config_data['parsing_options']
            assert cb_processor.cb_inspections == config_data['cb_inspections']


class TestExportFunctionality:
    """Test export functionality with new path resolution."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_export_uses_correct_paths(self, mock_get_path_manager):
        """Test that export functions use correct path resolution."""
        mock_path_manager = Mock()
        mock_path_manager.get_buffer_path.return_value = Path("/tmp/test/buffer")
        mock_path_manager.get_output_path.return_value = Path("/tmp/test/output")
        mock_get_path_manager.return_value = mock_path_manager
        
        config = {
            "white_list": ["col1"],
            "dd_inspections": {},
            "parsing_options": {},
            "csv_export_delimiter": ",",
            "output_formats_and_batching": {"csv": "monolith"}
        }
        logger = logging.getLogger("test")
        
        processor = DomainDataProcessor(config, logger)
        processor.parsed_table = pl.LazyFrame({"col1": ["a", "b", "c"]})
        
        # Mock the ingestion tracker file
        tracker_data = {"file1.csv": {"checksum": "abc123"}}
        
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', create=True) as mock_open, \
             patch('json.load', return_value=tracker_data), \
             patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch.object(processor, '_export_csv') as mock_export_csv:
            
            processor.run_export()
            
            # Verify correct paths were used
            mock_path_manager.get_buffer_path.assert_called_with("filtered_dd_mirror")
            mock_path_manager.get_output_path.assert_called_with("domain")
            
            # Verify export method was called
            mock_export_csv.assert_called_once()
