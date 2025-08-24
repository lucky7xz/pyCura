"""
Base processor class with shared path logic and simple configuration.
"""

from pathlib import Path
import logging
from typing import Dict, Any
from abc import ABC, abstractmethod

from src.shared.path_manager import get_path_manager
from src.shared.exceptions import (
    DomainProcessingError,
    ParsingError,
    InspectionError,
    EditError,
    ExportError,
    ConfigurationError
)


class BaseProcessor(ABC):
    """Base class for all processors with common path and config logic."""
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """Initialize with simple config and logger."""
        self.config = config
        self.logger = logger
        self.path_manager = get_path_manager()
        
        # Common config extraction
        self.white_list = config["white_list"]
        
        # Make exceptions available to subclasses
        self.DomainProcessingError = DomainProcessingError
        self.ParsingError = ParsingError
        self.InspectionError = InspectionError
        self.EditError = EditError
        self.ExportError = ExportError
        self.ConfigurationError = ConfigurationError
    
    # Path access methods - DRY principle
    def get_input_path(self, path_type: str = "base") -> Path:
        """Get input path as Path object."""
        return Path(self.path_manager.get_input_path(path_type))
    
    def get_buffer_path(self, path_type: str = "base") -> Path:
        """Get buffer path as Path object."""
        return Path(self.path_manager.get_buffer_path(path_type))
    
    def get_output_path(self, path_type: str = "base") -> Path:
        """Get output path as Path object."""
        return Path(self.path_manager.get_output_path(path_type))
    
    def get_config_path(self, path_type: str = "base") -> Path:
        """Get config path as Path object."""
        return Path(self.path_manager.get_config_path(path_type))
    
    def get_log_path(self, path_type: str = "base") -> Path:
        """Get log path as Path object."""
        return Path(self.path_manager.get_log_path(path_type))
    
    @abstractmethod
    def run_pre_processing(self):
        """Run pre-processing - must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def run_inspection_processing(self, second_run: bool = False):
        """Run inspection processing - must be implemented by subclasses."""
        pass
