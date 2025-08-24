# C4 Architecture - pyCura Beta 2408

## Context (Level 1)

```
┌─────────────────────────────────────────────────────────────────┐
│                         pyCura System                           │
│                                                                 │
│  Data Curation Framework for Research & Analytics               │
│  • Processes domain data with codebook validation               │
│  • Supports local files and cloud storage (S3, GCS, Azure)      │
│  • Configurable inspections, edits, and export formats          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         ↑                    ↑                    ↑
    [Researchers]        [Data Teams]        [Cloud Storage]
```

**External Dependencies:**
- **Polars**: High-performance data processing with native cloud support
- **Cloud Storage**: S3, Google Cloud Storage, Azure Blob Storage
- **Configuration**: TOML-based project configuration files

## Container (Level 2)

```
┌─────────────────────────────────────────────────────────────────┐
│                         pyCura System                           │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   CLI Interface │  │  Core Framework │  │  Path Manager   │  │
│  │                 │  │                 │  │                 │  │
│  │ • Interactive   │  │ • Processors    │  │ • URI Support   │  │
│  │ • Batch Mode    │  │ • Parsers       │  │ • Cloud Storage │  │
│  │ • Config Load   │  │ • Inspections   │  │ • Local Files   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│           │                     │                     │         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Project Manager │  │ Processing Mods │  │  Data Sources   │  │
│  │                 │  │                 │  │                 │  │
│  │ • Config Parse  │  │ • Edits         │  │ • Domain Data   │  │
│  │ • Orchestration │  │ • Inspections   │  │ • Codebooks     │  │
│  │ • Logging       │  │ • Validations   │  │ • Exports       │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Component (Level 3)

### Core Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    Core Framework Container                     │
│                                                                 │
│  ┌─────────────────┐           ┌─────────────────┐              │
│  │  BaseProcessor  │◄──────────┤ DomainProcessor │              │
│  │                 │           │                 │              │
│  │ • Path Access   │           │ • Pre-process   │              │
│  │ • Config Mgmt   │           │ • Inspections   │              │
│  │ • Logging       │           │ • Edits         │              │
│  └─────────────────┘           │ • Export        │              │
│           ▲                    └─────────────────┘              │
│           │                                                     │
│  ┌─────────────────┐           ┌─────────────────┐              │
│  │ CodebookProc    │           │  PathManager    │              │
│  │                 │           │                 │              │
│  │ • Parse CB      │           │ • URI Detection │              │
│  │ • Validate      │           │ • Path Join     │              │
│  │ • Mirror        │           │ • Cloud Support │              │
│  │ • Export        │           │ • Dir Creation  │              │
│  └─────────────────┘           └─────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

### Processing Modules

```
┌─────────────────────────────────────────────────────────────────┐
│                 Processing Modules Container                    │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │     Edits       │  │  Inspections    │  │   Parsers       │  │
│  │                 │  │                 │  │                 │  │
│  │ • apply_case    │  │ • length_map    │  │ • CSV Parser    │  │
│  │ • apply_padding │  │ • char_map      │  │ • SPSS Parser   │  │
│  │ • token_replace │  │ • occurrence    │  │ • Zero Parser   │  │
│  │ • char_replace  │  │ • custom_funcs  │  │ • Custom Parse  │  │
│  │ • append_column │  │                 │  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│           │                     │                     │         │
│           └─────────────────────┼─────────────────────┘         │
│                                 │                               │
│                    ┌─────────────────┐                          │
│                    │ Dynamic Import  │                          │
│                    │                 │                          │
│                    │ • Static Paths  │                          │
│                    │ • Error Handle  │                          │
│                    │ • Function Load │                          │
│                    └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Code (Level 4) - Key Components

### Architecture Principles

**After Beta 2408 Refactoring:**

1. **Simple Constructors**: `Processor(config, logger)` - no complex injections
2. **Centralized Paths**: Single PathManager with URI support
3. **Static Imports**: Reliable module loading with error handling
4. **Pure Functions**: Path resolution without side effects
5. **Clean Interfaces**: DRY principle in base classes

### Path Management Flow

```python
# Configuration → PathManager → Processors
config = load_toml("demo1.toml")
path_manager = PathManager(config)  # URI-aware
initialize_path_manager(path_manager)

# Processors use clean interfaces
processor = DomainDataProcessor(config, logger)
input_path = processor.get_input_path("domain")  # Could be s3://bucket/
```

### Processing Pipeline

```python
class DomainDataProcessor(BaseProcessor):
    def __init__(self, config, logger):  
        super().__init__(config, logger)
        self.dd_inspections = config["dd_inspections"]
    
    def run_pre_processing(self):
        # Parse → Filter → Export to buffer
        
    def run_inspection_processing(self):
        # Import functions statically → Execute → Export results
        
    def run_edit_processing(self):
        # Import edits statically → Apply → Update data
```

## Architecture Improvements (Beta 2408)

- ❌ **Complex Injection Dictionaries**: Removed nested parameter passing
- ❌ **Dynamic Path Construction**: Replaced with centralized resolution
- ❌ **Scattered Path Logic**: Consolidated in PathManager
- ❌ **Interactive CLI Prompts**: Simplified to declarative config

- ✅ **URI-Native Paths**: `s3://`, `gs://`, `abfss://` support
- ✅ **Static Module Imports**: Reliable function loading
- ✅ **Clean Base Classes**: DRY path access methods
- ✅ **Simple Data Structures**: Config dictionaries over complex objects

### Cloud-Native Design
- **Polars Integration**: Direct URI support in `pl.scan_csv()`
- **Authentication Agnostic**: Uses standard cloud credentials
- **Mixed Environments**: Local + cloud paths in same project
- **Glob Patterns**: `s3://bucket/data/*.csv` expansion

## Technology Stack

- **Core**: Python 3.12, Polars (data processing)
- **Configuration**: TOML (human-readable config)
- **Cloud**: Native S3/GCS/Azure support via Polars
- **Testing**: pytest with 68 tests, 40% coverage
- **Architecture**: Clean Architecture principles, dependency injection elimination
