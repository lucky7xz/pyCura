"""
Shared exception classes for pyCura processors.
"""


class ProcessingError(Exception):
    """Base exception for all processing errors."""
    pass


class DomainProcessingError(ProcessingError):
    """Base exception for domain data processing errors."""
    pass


class CodebookProcessingError(ProcessingError):
    """Base exception for codebook processing errors."""
    pass


class ParsingError(ProcessingError):
    """Exception raised when there's an error parsing data."""
    pass


class InspectionError(ProcessingError):
    """Exception raised when there's an error during data inspection."""
    pass


class EditError(ProcessingError):
    """Exception raised when there's an error editing data."""
    pass


class ExportError(ProcessingError):
    """Exception raised when there's an error exporting data."""
    pass


class PathError(ProcessingError):
    """Exception raised when there's an error with path operations."""
    pass


class ConfigurationError(ProcessingError):
    """Exception raised when there's an error with configuration."""
    pass
