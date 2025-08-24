"""
Path Management System for pyCura

Provides external path configuration with full Polars URI support and clean component interfaces.
Supports local files, cloud storage (S3, GCS, Azure), HTTP URLs, and glob patterns.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import logging
import os
from dataclasses import dataclass
from urllib.parse import urlparse
import glob


@dataclass
class PathConfig:
    """Configuration for path management system."""
    # Base directories (can be URIs)
    base_input: str = "data_in"
    base_buffer: str = "data_buffer" 
    base_output: str = "data_out"
    base_config: str = "config_files"
    base_logs: str = "logs"
    
    # Project-specific overrides
    project_name: Optional[str] = None
    domain_foldername: Optional[str] = None


class PathManager:
    """
    Centralized path management supporting all Polars-compatible URIs.
    
    Resolves paths at startup and provides clean interfaces for components.
    Supports: local files, S3, GCS, Azure, HTTP, glob patterns.
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize path manager with configuration.
        
        Args:
            config: Configuration dictionary containing path settings
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = config
        
        # Extract path configuration from config or use defaults
        self.path_config = self._load_path_config(config)
        
        # Resolve all paths at startup
        self.resolved_paths = self._resolve_all_paths()
        
        # Create necessary directories for local paths
        self._ensure_directories()
        
        self.logger.info(f"PathManager initialized for project: {self.path_config.project_name}")
    
    def _load_path_config(self, config: Dict[str, Any]) -> PathConfig:
        """Load path configuration from config dict with fallback to defaults."""
        path_settings = config.get("paths", {})
        
        return PathConfig(
            base_input=path_settings.get("base_input", "data_in"),
            base_buffer=path_settings.get("base_buffer", "data_buffer"),
            base_output=path_settings.get("base_output", "data_out"),
            base_config=path_settings.get("base_config", "config_files"),
            base_logs=path_settings.get("base_logs", "logs"),
            project_name=config.get("project_name"),
            domain_foldername=config.get("domain_foldername")
        )
    
    def _resolve_all_paths(self) -> Dict[str, Dict[str, str]]:
        """Resolve all path categories at startup."""
        project_name = self.path_config.project_name or "default_project"
        domain_folder = self.path_config.domain_foldername
        
        paths = {
            "input": self._resolve_input_paths(domain_folder),
            "buffer": self._resolve_buffer_paths(project_name),
            "output": self._resolve_output_paths(project_name),
            "config": self._resolve_config_paths(),
            "logs": self._resolve_log_paths(project_name)
        }
        
        return paths
    
    def _resolve_input_paths(self, domain_folder: Optional[str]) -> Dict[str, str]:
        """Resolve input paths with domain folder structure."""
        base = self.path_config.base_input
        
        if not domain_folder:
            return {"base": base}
        
        return {
            "base": base,
            "domain": self._join_path(base, domain_folder, "domain"),
            "codebook": self._join_path(base, domain_folder, "codebook")
        }
    
    def _resolve_buffer_paths(self, project_name: str) -> Dict[str, str]:
        """Resolve buffer/temporary paths for project."""
        base = self._join_path(self.path_config.base_buffer, project_name)
        
        return {
            "base": base,
            "cb_mirror": self._join_path(base, "original_cb_mirror.json"),
            "filtered_cb_mirror": self._join_path(base, "filtered_cb_mirror.json"),
            "filtered_dd_mirror": self._join_path(base, "buffer_dd")
        }
    
    def _resolve_output_paths(self, project_name: str) -> Dict[str, str]:
        """Resolve output paths for project."""
        base = self._join_path(self.path_config.base_output, project_name)
        
        return {
            "base": base,
            "inspection": self._join_path(base, "inspection"),
            "key_exports": self._join_path(base, "key_exports"),
            "domain_exports": self._join_path(base, "domain_exports"),
            "final_dd": self._join_path(base, "final_data_data"),
            "final_cb": self._join_path(base, "final_codebook.json")
        }
    
    def _resolve_config_paths(self) -> Dict[str, str]:
        """Resolve configuration file paths."""
        base = self.path_config.base_config
        
        return {
            "base": base
        }
    
    def _resolve_log_paths(self, project_name: str) -> Dict[str, str]:
        """Resolve logging paths for project."""
        base = self._join_path(self.path_config.base_logs, project_name)
        
        return {
            "base": base,
            "main": self._join_path(base, "pycura.log"),
            "errors": self._join_path(base, "errors.log")
        }
    
    def _join_path(self, *parts: str) -> str:
        """
        Join path parts, handling both local paths and URIs.
        
        For URIs (s3://, gcs://, etc.), use forward slashes.
        For local paths, use OS-appropriate separators.
        """
        if not parts:
            return ""
        
        base = parts[0]
        remaining = parts[1:]
        
        # Check if base is a URI
        if self._is_uri(base):
            # For URIs, always use forward slashes
            result = base.rstrip('/')
            for part in remaining:
                result += '/' + str(part).strip('/')
            return result
        else:
            # For local paths, use pathlib
            result = Path(base)
            for part in remaining:
                result = result / part
            return str(result)
    
    def _is_uri(self, path: str) -> bool:
        """Check if path is a URI (has scheme like s3://, gcs://, http://)."""
        parsed = urlparse(str(path))
        return bool(parsed.scheme and parsed.scheme not in ['', 'file'])
    
    def _ensure_directories(self) -> None:
        """Create necessary directories for local paths."""
        directories_to_create = []
        
        for category, paths in self.resolved_paths.items():
            for path_name, path_value in paths.items():
                if not self._is_uri(path_value):
                    path_obj = Path(path_value)
                    
                    # Check if this looks like a file (has extension) or is explicitly a file path
                    if (path_obj.suffix in ['.json', '.log', '.csv', '.parquet', '.txt'] or 
                        path_name in ['cb_mirror', 'filtered_cb_mirror', 'final_cb', 'main', 'errors']):
                        # For files, create parent directory only
                        directories_to_create.append(path_obj.parent)
                    else:
                        # For directories, create the directory itself
                        directories_to_create.append(path_obj)
        
        # Create directories (remove duplicates)
        unique_directories = set(directories_to_create)
        for directory in unique_directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Ensured directory exists: {directory}")
            except Exception as e:
                self.logger.warning(f"Could not create directory {directory}: {e}")
    
    # ==================== PUBLIC INTERFACE ====================
    
    def get_input_path(self, path_type: str = "base") -> str:
        """Get input path by type (base, domain, codebook)."""
        return self.resolved_paths["input"].get(path_type, self.resolved_paths["input"]["base"])
    
    def get_buffer_path(self, path_type: str = "base") -> str:
        """Get buffer path by type (base, cb_mirror, filtered_cb_mirror, filtered_dd_mirror)."""
        return self.resolved_paths["buffer"].get(path_type, self.resolved_paths["buffer"]["base"])
    
    def get_output_path(self, path_type: str = "base") -> str:
        """Get output path by type (base, inspection, key_exports, domain_exports, final_dd, final_cb)."""
        return self.resolved_paths["output"].get(path_type, self.resolved_paths["output"]["base"])
    
    def get_config_path(self, path_type: str = "base") -> str:
        """Get config path by type."""
        return self.resolved_paths["config"].get(path_type, self.resolved_paths["config"]["base"])
    
    def get_log_path(self, path_type: str = "base") -> str:
        """Get log path by type (base, main, errors)."""
        return self.resolved_paths["logs"].get(path_type, self.resolved_paths["logs"]["base"])
    
    def get_all_paths(self) -> Dict[str, Dict[str, str]]:
        """Get all resolved paths for debugging/inspection."""
        return self.resolved_paths.copy()
    
    def resolve_data_sources(self, sources: Union[str, List[str]]) -> List[str]:
        """
        Resolve data source patterns to actual paths/URIs.
        
        Supports:
        - Local files and glob patterns
        - S3 URIs: s3://bucket/path/*.csv
        - GCS URIs: gs://bucket/path/*.parquet  
        - Azure URIs: abfss://container@account.dfs.core.windows.net/path/*
        - HTTP URLs: https://example.com/data.csv
        
        Args:
            sources: Single source or list of source patterns
            
        Returns:
            List of resolved paths/URIs
        """
        if isinstance(sources, str):
            sources = [sources]
        
        resolved = []
        
        for source in sources:
            source_str = str(source)
            
            if self._is_uri(source_str):
                # For URIs, pass through (Polars handles expansion)
                resolved.append(source_str)
            else:
                # For local paths, expand globs
                if '*' in source_str or '?' in source_str:
                    # Glob pattern
                    matches = glob.glob(source_str, recursive=True)
                    resolved.extend(matches)
                else:
                    # Regular path
                    resolved.append(source_str)
        
        self.logger.debug(f"Resolved {len(sources)} source patterns to {len(resolved)} paths")
        return resolved
    
    def validate_source_accessibility(self, sources: List[str]) -> Dict[str, Any]:
        """
        Validate accessibility of data sources.
        
        Args:
            sources: List of source paths/URIs
            
        Returns:
            Dict with validation results
        """
        accessible = []
        inaccessible = []
        
        for source in sources:
            if self._is_uri(source):
                # For URIs, assume accessible (Polars will handle auth)
                accessible.append(source)
            else:
                # For local paths, check existence
                if Path(source).exists():
                    accessible.append(source)
                else:
                    inaccessible.append(source)
        
        return {
            "accessible": accessible,
            "inaccessible": inaccessible,
            "total": len(sources),
            "accessible_count": len(accessible),
            "success_rate": len(accessible) / len(sources) if sources else 0.0
        }


# ==================== GLOBAL PATH MANAGER INSTANCE ====================

_global_path_manager: Optional[PathManager] = None


def initialize_path_manager(config: Dict[str, Any], logger: Optional[logging.Logger] = None) -> PathManager:
    """Initialize the global path manager instance."""
    global _global_path_manager
    _global_path_manager = PathManager(config, logger)
    return _global_path_manager


def get_path_manager() -> PathManager:
    """Get the global path manager instance."""
    if _global_path_manager is None:
        raise RuntimeError("Path manager not initialized. Call initialize_path_manager() first.")
    return _global_path_manager


# ==================== CONVENIENCE FUNCTIONS ====================

def get_input_path(path_type: str = "base") -> str:
    """Convenience function to get input path."""
    return get_path_manager().get_input_path(path_type)


def get_buffer_path(path_type: str = "base") -> str:
    """Convenience function to get buffer path."""
    return get_path_manager().get_buffer_path(path_type)


def get_output_path(path_type: str = "base") -> str:
    """Convenience function to get output path."""
    return get_path_manager().get_output_path(path_type)


def get_config_path(path_type: str = "base") -> str:
    """Convenience function to get config path."""
    return get_path_manager().get_config_path(path_type)


def get_log_path(path_type: str = "base") -> str:
    """Convenience function to get log path."""
    return get_path_manager().get_log_path(path_type)
