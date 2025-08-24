# Testing Guide for pyCura

This guide covers testing commands for validating your pyCura installation and developing custom extensions.

## Framework Testing

These tests validate the core pyCura framework functionality. Run these to ensure your installation is working correctly.

### Run All Framework Tests
```bash
python -m pytest tests/ -v
```

### Run Specific Test Categories
```bash
# Core processor functionality
python -m pytest tests/test_processors.py -v

# Path management and URI support
python -m pytest tests/test_path_manager.py -v

# Data processing utilities
python -m pytest tests/test_data_processing.py -v

# Integration tests
python -m pytest tests/test_integration.py -v
```

### Run Single Test File
```bash
python -m pytest tests/test_processor_refactoring.py -v
```

### Run Specific Test Method
```bash
python -m pytest tests/test_processors.py::TestDomainDataProcessor::test_domain_processor_initialization -v
```

## Module Extension Testing

When developing custom parsers, inspections, or edits, create tests in your project directory and run them separately from framework tests.

### Test Your Custom Modules
```bash
# Test custom parsers
python -m pytest your_project/tests/test_custom_parsers.py -v

# Test custom inspections
python -m pytest your_project/tests/test_custom_inspections.py -v

# Test custom edits
python -m pytest your_project/tests/test_custom_edits.py -v
```

### Combined Testing
```bash
# Test both framework and your extensions
python -m pytest tests/ your_project/tests/ -v
```

## Coverage Reporting

### Generate Coverage Report
```bash
# Run tests with coverage
python -m pytest tests/ --cov=src --cov-report=html

# View coverage in terminal
python -m pytest tests/ --cov=src --cov-report=term-missing
```

### Coverage for Specific Modules
```bash
# Coverage for processors only
python -m pytest tests/test_processors.py --cov=src/processors --cov-report=term-missing

# Coverage for path management
python -m pytest tests/test_path_manager.py --cov=src/shared/path_manager --cov-report=html
```

## Quick Validation Commands

### Verify Installation
```bash
# Quick framework validation (68 tests)
python -m pytest tests/ -v

# Check for any failures
echo $?  # Should return 0 if all tests pass
```

### Development Workflow
```bash
# Test specific functionality you're working on
python -m pytest tests/test_processor_refactoring.py::TestEditFunctionImports -v

# Run with coverage for the module you're modifying
python -m pytest tests/test_processors.py --cov=src/processors --cov-report=term-missing
```

## Test Output Interpretation

### Success Indicators
- `68 passed` - All framework tests passing
- `0 warnings` - No deprecation or compatibility issues
- Exit code `0` - Clean test run

### Common Test Patterns
```bash
# Framework validation before deployment
python -m pytest tests/ -v

# Module development cycle
python -m pytest your_tests/ --cov=your_module --cov-report=term-missing -v

# Full validation with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

The framework tests ensure pyCura's core functionality works correctly, while module extension tests validate your custom components integrate properly with the framework.
