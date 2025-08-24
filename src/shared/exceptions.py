"""
Shared exception classes for pyCura processors with LLM-friendly debugging info.
"""


class ProcessingError(Exception):
    """Base exception for all processing errors."""
    
    def __init__(self, message, file_path=None, data_context=None):
        super().__init__(message)
        self.file_path = file_path
        self.data_context = data_context
    
    def __str__(self):
        msg = super().__str__()
        if self.file_path:
            msg += f" | File: {self.file_path}"
        if self.data_context:
            msg += f" | Context: {self.data_context}"
        return msg


class DomainProcessingError(ProcessingError):
    """Base exception for domain data processing errors.
    
    Debug: Check domain data files in data_in/, verify CSV format and column names match whitelist.
    """
    pass


class CodebookProcessingError(ProcessingError):
    """Base exception for codebook processing errors.
    
    Debug: Check codebook files in data_in/codebook/, ensure only one codebook file exists.
    """
    pass


class ParsingError(ProcessingError):
    """Exception raised when there's an error parsing data.
    
    Debug: Verify file format matches parser type, check for encoding issues or malformed data.
    """
    pass


class InspectionError(ProcessingError):
    """Exception raised when there's an error during data inspection.
    
    Debug: Check inspection function exists in src/processing_modules/inspections/, verify data is parsed.
    """
    pass


class EditError(ProcessingError):
    """Exception raised when there's an error editing data.
    
    Debug: Check edit function exists in src/processing_modules/edits/, verify parameters and data types.
    """
    pass


class ExportError(ProcessingError):
    """Exception raised when there's an error exporting data.
    
    Debug: Check data_out/ directory permissions, verify parsed data exists and output format is valid.
    """
    pass


class ConfigurationError(ProcessingError):
    """Exception raised when there's an error with configuration.
    
    Debug: Check config file syntax, verify required keys exist, validate path configurations.
    """
    pass
