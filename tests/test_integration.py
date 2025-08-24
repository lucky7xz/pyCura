"""
Integration tests for the simplified processor architecture.
"""

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import polars as pl

from src.shared.project_manager import ProjectManager


class TestProcessorIntegrationWithProjectManager:
    """Test processor integration with ProjectManager."""
    
    @patch('src.shared.project_manager.initialize_path_manager')
    @patch('src.shared.project_manager.Path.exists')
    def test_project_manager_creates_processors_with_new_signature(self, mock_exists, mock_init_path_manager):
        """Test that ProjectManager creates processors with the new simplified signature."""
        # Mock file existence checks
        mock_exists.return_value = True
        
        # Mock path manager initialization
        mock_path_manager = Mock()
        mock_path_manager.get_input_path.return_value = "/test/input/path"
        mock_path_manager.get_output_path.return_value = "/test/output/path"
        mock_path_manager.get_buffer_path.return_value = "/test/buffer/path"
        mock_init_path_manager.return_value = mock_path_manager
        
        # Create a minimal config file content
        config_content = """
project_name = "test_project"
domain_foldername = "test_domain"
white_list = ["col1", "col2"]
append_new_metadata = false
select_parser = "test_parser"
key_export_ban = []
edits = ["test_edit"]
csv_export_delimiter = ","

[parsing_options]
add_id = true

[dd_inspections]
test_inspection = { active = true }

[cb_inspections]
test_cb_inspection = { active = true }

[output_formats_and_batching]
csv = true
"""
        
        # Mock file reading
        with patch('builtins.open', mock_open_multiple_files({
            'config_files/test_config.toml': config_content
        })):
            with patch('src.shared.project_manager.tomllib.load') as mock_toml_load:
                mock_toml_load.return_value = {
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
                
                # Initialize ProjectManager
                pm = ProjectManager('test_config.toml')
                    
                # Test that processors can be created with new signature
                with patch('src.processors.base_processor.get_path_manager') as mock_get_path_manager:
                    mock_get_path_manager.return_value = mock_path_manager
                    cb_processor = pm.get_codebook_processor()
                    dd_processor = pm.get_domain_data_processor()
                
                # Verify processors were created successfully
                assert cb_processor is not None
                assert dd_processor is not None
                
                # Verify they have the expected config
                assert cb_processor.config == pm.config
                assert dd_processor.config == pm.config
                assert cb_processor.logger == pm.logger
                assert dd_processor.logger == pm.logger


def mock_open_multiple_files(files_dict):
    """Helper to mock multiple file opens."""
    def mock_open_func(*args, **kwargs):
        filename = args[0]
        if isinstance(filename, Path):
            filename = str(filename)
        
        for file_path, content in files_dict.items():
            if file_path in filename:
                mock_file = MagicMock()
                mock_file.read.return_value = content
                mock_file.__enter__.return_value = mock_file
                return mock_file
        
        # Default behavior for unmatched files
        mock_file = MagicMock()
        mock_file.read.return_value = ""
        mock_file.__enter__.return_value = mock_file
        return mock_file
    
    return mock_open_func


class TestSimplifiedArchitecture:
    """Test the overall simplified architecture."""
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_no_complex_injection_dictionaries(self, mock_get_path_manager):
        """Test that processors no longer use complex injection dictionaries."""
        from src.processors.domain_data_processor import DomainDataProcessor
        from src.processors.codebook_processor import CodebookProcessor
        
        # Mock path manager
        mock_path_manager = Mock()
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
        
        # Should be able to create processors with simple config + logger
        dd_proc = DomainDataProcessor(config, logger)
        cb_proc = CodebookProcessor(config, logger)
        
        # Verify no injection attributes exist
        assert not hasattr(dd_proc, 'dd_injection')
        assert not hasattr(cb_proc, 'cb_injection')
        assert not hasattr(dd_proc, 'module_paths')
        assert not hasattr(cb_proc, 'module_paths')
        
        # Verify they have simple config access
        assert dd_proc.config == config
        assert cb_proc.config == config
    
    @patch('src.processors.base_processor.get_path_manager')
    def test_dry_path_access(self, mock_get_path_manager):
        """Test that both processors use the same DRY path access methods."""
        from src.processors.domain_data_processor import DomainDataProcessor
        from src.processors.codebook_processor import CodebookProcessor
        
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
        
        dd_proc = DomainDataProcessor(config, logger)
        cb_proc = CodebookProcessor(config, logger)
        
        # Both should use the same path manager
        assert dd_proc.path_manager == cb_proc.path_manager
        
        # Both should have the same path access methods
        dd_path = dd_proc.get_input_path("domain")
        cb_path = cb_proc.get_input_path("domain")
        assert dd_path == cb_path == Path("/test/input")
    
    def test_simple_module_imports(self):
        """Test that processors use simple module import paths."""
        from src.processors.domain_data_processor import DomainDataProcessor
        from src.processors.codebook_processor import CodebookProcessor
        
        # Check that the processors import correctly without complex dependencies
        assert DomainDataProcessor is not None
        assert CodebookProcessor is not None
        
        # Verify they inherit from BaseProcessor
        from src.processors.base_processor import BaseProcessor
        assert issubclass(DomainDataProcessor, BaseProcessor)
        assert issubclass(CodebookProcessor, BaseProcessor)
