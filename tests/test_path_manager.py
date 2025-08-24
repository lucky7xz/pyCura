"""
Tests for PathManager - comprehensive path management system.
"""

import pytest
from pathlib import Path
import tempfile
import shutil
import logging
from unittest.mock import patch, MagicMock

from src.shared.path_manager import (
    PathManager, PathConfig, initialize_path_manager, 
    get_path_manager, get_input_path, get_buffer_path,
    get_output_path, get_config_path, get_log_path
)


class TestPathConfig:
    """Test PathConfig dataclass."""
    
    def test_default_values(self):
        """Test default path configuration values."""
        config = PathConfig()
        
        assert config.base_input == "data_in"
        assert config.base_buffer == "data_buffer"
        assert config.base_output == "data_out"
        assert config.base_config == "config_files"
        assert config.base_logs == "logs"
        assert config.project_name is None
        assert config.domain_foldername is None
    
    def test_custom_values(self):
        """Test custom path configuration values."""
        config = PathConfig(
            base_input="s3://bucket/input",
            base_output="gs://bucket/output",
            project_name="test_project",
            domain_foldername="health"
        )
        
        assert config.base_input == "s3://bucket/input"
        assert config.base_output == "gs://bucket/output"
        assert config.project_name == "test_project"
        assert config.domain_foldername == "health"


class TestPathManager:
    """Test PathManager functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def basic_config(self):
        """Basic configuration for testing."""
        return {
            "project_name": "test_project",
            "domain_foldername": "test_domain"
        }
    
    @pytest.fixture
    def config_with_paths(self, temp_dir):
        """Configuration with custom paths."""
        return {
            "project_name": "test_project",
            "domain_foldername": "test_domain",
            "paths": {
                "base_input": str(Path(temp_dir) / "input"),
                "base_buffer": str(Path(temp_dir) / "buffer"),
                "base_output": str(Path(temp_dir) / "output"),
                "base_config": str(Path(temp_dir) / "config"),
                "base_logs": str(Path(temp_dir) / "logs")
            }
        }
    
    def test_initialization_basic(self, basic_config):
        """Test basic PathManager initialization."""
        logger = logging.getLogger("test")
        pm = PathManager(basic_config, logger)
        
        assert pm.config == basic_config
        assert pm.logger == logger
        assert pm.path_config.project_name == "test_project"
        assert pm.path_config.domain_foldername == "test_domain"
    
    def test_initialization_with_custom_paths(self, config_with_paths):
        """Test PathManager initialization with custom paths."""
        pm = PathManager(config_with_paths)
        
        assert pm.path_config.base_input == config_with_paths["paths"]["base_input"]
        assert pm.path_config.base_buffer == config_with_paths["paths"]["base_buffer"]
    
    def test_load_path_config_defaults(self, basic_config):
        """Test loading path config with defaults."""
        pm = PathManager(basic_config)
        
        assert pm.path_config.base_input == "data_in"
        assert pm.path_config.base_buffer == "data_buffer"
        assert pm.path_config.project_name == "test_project"
    
    def test_load_path_config_custom(self, config_with_paths):
        """Test loading path config with custom values."""
        pm = PathManager(config_with_paths)
        
        assert pm.path_config.base_input == config_with_paths["paths"]["base_input"]
        assert pm.path_config.base_output == config_with_paths["paths"]["base_output"]
    
    def test_resolve_input_paths_basic(self, basic_config):
        """Test resolving input paths without domain folder."""
        config = {"project_name": "test"}
        pm = PathManager(config)
        
        input_paths = pm._resolve_input_paths(None)
        assert input_paths == {"base": "data_in"}
    
    def test_resolve_input_paths_with_domain(self, basic_config):
        """Test resolving input paths with domain folder."""
        pm = PathManager(basic_config)
        
        input_paths = pm._resolve_input_paths("test_domain")
        expected = {
            "base": "data_in",
            "domain": str(Path("data_in") / "test_domain" / "domain"),
            "codebook": str(Path("data_in") / "test_domain" / "codebook")
        }
        assert input_paths == expected
    
    def test_resolve_buffer_paths(self, basic_config):
        """Test resolving buffer paths."""
        pm = PathManager(basic_config)
        
        buffer_paths = pm._resolve_buffer_paths("test_project")
        base_path = str(Path("data_buffer") / "test_project")
        
        expected = {
            "base": base_path,
            "cb_mirror": str(Path(base_path) / "original_cb_mirror.json"),
            "filtered_cb_mirror": str(Path(base_path) / "filtered_cb_mirror.json"),
            "filtered_dd_mirror": str(Path(base_path) / "buffer_dd")
        }
        assert buffer_paths == expected
    
    def test_resolve_output_paths(self, basic_config):
        """Test resolving output paths."""
        pm = PathManager(basic_config)
        
        output_paths = pm._resolve_output_paths("test_project")
        base_path = str(Path("data_out") / "test_project")
        
        expected = {
            "base": base_path,
            "inspection": str(Path(base_path) / "inspection"),
            "key_exports": str(Path(base_path) / "key_exports"),
            "domain_exports": str(Path(base_path) / "domain_exports"),
            "final_dd": str(Path(base_path) / "final_data_data"),
            "final_cb": str(Path(base_path) / "final_codebook.json")
        }
        assert output_paths == expected
    
    def test_join_path_local(self, basic_config):
        """Test joining local paths."""
        pm = PathManager(basic_config)
        
        result = pm._join_path("data", "subfolder", "file.txt")
        expected = str(Path("data") / "subfolder" / "file.txt")
        assert result == expected
    
    def test_join_path_uri(self, basic_config):
        """Test joining URI paths."""
        pm = PathManager(basic_config)
        
        result = pm._join_path("s3://bucket", "folder", "file.csv")
        assert result == "s3://bucket/folder/file.csv"
        
        result = pm._join_path("gs://bucket/", "/folder/", "file.parquet")
        assert result == "gs://bucket/folder/file.parquet"
    
    def test_is_uri(self, basic_config):
        """Test URI detection."""
        pm = PathManager(basic_config)
        
        assert pm._is_uri("s3://bucket/path")
        assert pm._is_uri("gs://bucket/path")
        assert pm._is_uri("https://example.com/data")
        assert pm._is_uri("abfss://container@account.dfs.core.windows.net/path")
        
        assert not pm._is_uri("local/path")
        assert not pm._is_uri("/absolute/path")
        assert not pm._is_uri("./relative/path")
        assert not pm._is_uri("file:///local/path")
    
    def test_get_input_path(self, basic_config):
        """Test getting input paths."""
        pm = PathManager(basic_config)
        
        assert pm.get_input_path() == "data_in"
        assert pm.get_input_path("base") == "data_in"
        assert pm.get_input_path("domain") == str(Path("data_in") / "test_domain" / "domain")
        assert pm.get_input_path("codebook") == str(Path("data_in") / "test_domain" / "codebook")
    
    def test_get_buffer_path(self, basic_config):
        """Test getting buffer paths."""
        pm = PathManager(basic_config)
        
        base_path = str(Path("data_buffer") / "test_project")
        assert pm.get_buffer_path() == base_path
        assert pm.get_buffer_path("base") == base_path
        assert pm.get_buffer_path("cb_mirror") == str(Path(base_path) / "original_cb_mirror.json")
    
    def test_get_output_path(self, basic_config):
        """Test getting output paths."""
        pm = PathManager(basic_config)
        
        base_path = str(Path("data_out") / "test_project")
        assert pm.get_output_path() == base_path
        assert pm.get_output_path("inspection") == str(Path(base_path) / "inspection")
        assert pm.get_output_path("final_cb") == str(Path(base_path) / "final_codebook.json")
    
    def test_resolve_data_sources_local(self, basic_config, temp_dir):
        """Test resolving local data sources."""
        pm = PathManager(basic_config)
        
        # Create test files
        test_dir = Path(temp_dir) / "test_data"
        test_dir.mkdir()
        (test_dir / "file1.csv").touch()
        (test_dir / "file2.csv").touch()
        
        # Test single source
        sources = [str(test_dir / "file1.csv")]
        resolved = pm.resolve_data_sources(sources)
        assert resolved == sources
        
        # Test glob pattern
        glob_pattern = str(test_dir / "*.csv")
        resolved = pm.resolve_data_sources([glob_pattern])
        assert len(resolved) == 2
        assert all(f.endswith(".csv") for f in resolved)
    
    def test_resolve_data_sources_uris(self, basic_config):
        """Test resolving URI data sources."""
        pm = PathManager(basic_config)
        
        uris = [
            "s3://bucket/data/*.csv",
            "gs://bucket/data.parquet",
            "https://example.com/data.json"
        ]
        
        resolved = pm.resolve_data_sources(uris)
        assert resolved == uris  # URIs passed through unchanged
    
    def test_validate_source_accessibility_local(self, basic_config, temp_dir):
        """Test validating local source accessibility."""
        pm = PathManager(basic_config)
        
        # Create existing file
        existing_file = Path(temp_dir) / "exists.csv"
        existing_file.touch()
        
        # Test with mix of existing and non-existing files
        sources = [
            str(existing_file),
            str(Path(temp_dir) / "missing.csv")
        ]
        
        result = pm.validate_source_accessibility(sources)
        
        assert len(result["accessible"]) == 1
        assert len(result["inaccessible"]) == 1
        assert result["total"] == 2
        assert result["success_rate"] == 0.5
    
    def test_validate_source_accessibility_uris(self, basic_config):
        """Test validating URI source accessibility."""
        pm = PathManager(basic_config)
        
        uris = [
            "s3://bucket/data.csv",
            "gs://bucket/data.parquet"
        ]
        
        result = pm.validate_source_accessibility(uris)
        
        # URIs assumed accessible
        assert len(result["accessible"]) == 2
        assert len(result["inaccessible"]) == 0
        assert result["success_rate"] == 1.0


class TestGlobalPathManager:
    """Test global path manager functionality."""
    
    def test_initialize_and_get_path_manager(self):
        """Test initializing and getting global path manager."""
        config = {"project_name": "global_test"}
        logger = logging.getLogger("global_test")
        
        # Initialize
        pm = initialize_path_manager(config, logger)
        assert pm is not None
        
        # Get same instance
        pm2 = get_path_manager()
        assert pm is pm2
    
    def test_get_path_manager_not_initialized(self):
        """Test getting path manager when not initialized."""
        # Reset global state
        import src.shared.path_manager
        src.shared.path_manager._global_path_manager = None
        
        with pytest.raises(RuntimeError, match="Path manager not initialized"):
            get_path_manager()
    
    def test_convenience_functions(self):
        """Test convenience functions."""
        config = {
            "project_name": "convenience_test",
            "domain_foldername": "test_domain"
        }
        
        initialize_path_manager(config)
        
        # Test convenience functions
        assert get_input_path() == "data_in"
        assert get_input_path("domain") == str(Path("data_in") / "test_domain" / "domain")
        
        base_buffer = str(Path("data_buffer") / "convenience_test")
        assert get_buffer_path() == base_buffer
        
        base_output = str(Path("data_out") / "convenience_test")
        assert get_output_path() == base_output
        
        assert get_config_path() == "config_files"
        
        base_logs = str(Path("logs") / "convenience_test")
        assert get_log_path() == base_logs


class TestPathManagerIntegration:
    """Integration tests for PathManager."""
    
    def test_cloud_storage_paths(self):
        """Test cloud storage path handling."""
        config = {
            "project_name": "cloud_test",
            "domain_foldername": "data",
            "paths": {
                "base_input": "s3://input-bucket",
                "base_output": "gs://output-bucket",
                "base_buffer": "data_buffer"  # Mix of cloud and local
            }
        }
        
        pm = PathManager(config)
        
        # Input paths should use S3
        assert pm.get_input_path("domain") == "s3://input-bucket/data/domain"
        
        # Output paths should use GCS
        assert pm.get_output_path("inspection") == "gs://output-bucket/cloud_test/inspection"
        
        # Buffer paths should be local
        assert pm.get_buffer_path().startswith("data_buffer")
    
    def test_mixed_uri_resolution(self):
        """Test resolving mixed local and URI sources."""
        config = {"project_name": "mixed_test"}
        pm = PathManager(config)
        
        sources = [
            "./local/*.csv",
            "s3://bucket/*.parquet",
            "https://api.example.com/data.json"
        ]
        
        resolved = pm.resolve_data_sources(sources)
        
        # Should have at least the URIs (local glob may expand to 0 files)
        assert "s3://bucket/*.parquet" in resolved
        assert "https://api.example.com/data.json" in resolved
    
    @patch('pathlib.Path.mkdir')
    def test_directory_creation(self, mock_mkdir):
        """Test that directories are created for local paths."""
        config = {
            "project_name": "dir_test",
            "domain_foldername": "test_domain"
        }
        
        pm = PathManager(config)
        
        # Should have called mkdir for various directories
        assert mock_mkdir.called
        
        # Check that parents=True and exist_ok=True were used
        call_args = mock_mkdir.call_args_list
        for call in call_args:
            assert call[1]['parents'] is True
            assert call[1]['exist_ok'] is True
