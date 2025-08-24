# Test Coverage Tracking

## Current Test Status (Updated: 2025-08-20)

### ✅ TESTED (22% overall coverage)
- **Data Transformations**
  - `apply_padding()` - 86% coverage
  - `apply_case()` - 55% coverage
- **Inspections** 
  - `length_map()` - 62% coverage
  - `char_map()` - 48% coverage
- **Utilities**
  - `sort_whitelist()`, `filter_by_whitelist()`, `merge_dicts()` - 27% coverage

### ❌ NOT TESTED (0% coverage)
- **CLI Interface** (`cura.py`) - 202 lines untested
- **Core Processors**
  - `CodebookProcessor` - not covered
  - `DomainDataProcessor` - not covered
- **Project Management**
  - `ProjectManager` - not covered
- **Parsers**
  - All domain/codebook parsers - not covered
- **Additional Processing Modules**
  - `apply_char_replace()` - 9% coverage
  - `occurrence_map()` - 8% coverage

## Testing Strategy by Priority

### High Priority (Core Stability)
- [ ] Integration test for full pipeline run
- [ ] ProjectManager config loading
- [ ] Basic CLI command execution

### Medium Priority (Feature Completeness)  
- [ ] Individual processor methods
- [ ] Parser functionality
- [ ] Export operations

### Low Priority (Edge Cases)
- [ ] Error handling scenarios
- [ ] Complex configuration edge cases
- [ ] Performance testing

## Commands for Tracking

```bash
# Run tests with coverage
./venv/bin/python -m pytest tests/ --cov=src --cov-report=term-missing

# Generate HTML coverage report
./venv/bin/python -m pytest tests/ --cov=src --cov-report=html

# Run specific test categories
./venv/bin/python -m pytest tests/test_data_processing.py -v
```

## Notes
- Focus on testing stable components first
- Add tests when fixing bugs (organic growth)
- Path-agnostic tests preferred during restructuring phase
