# C4 Model Architecture for pyCura

## Level 1: System Context Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Analyst                             │
│                     [Person]                                    │
│                                                                 │
│              Uses pyCura to curate and                          │
│              transform research data                            │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  │ Configures and runs
                  │ data processing
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        pyCura                                   │
│                   [Software System]                             │
│                                                                 │
│     Data curation framework for processing                      │
│     codebooks and domain data with configurable                 │
│     transformations and inspections                             │
└─────────────┬───────────────────────────┬───────────────────────┘
              │                           │
              │ Reads from                │ Writes to
              ▼                           ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│    Raw Data Sources     │    │    Output Storage       │
│    [External System]    │    │    [External System]    │
│                         │    │                         │
│ • CSV files             │    │ • Processed CSV         │
│ • Excel files           │    │ • Parquet files         │
│ • Parquet files         │    │ • JSON reports          │
│ • SQLite databases      │    │ • Log files             │
│ • Codebook files        │    │                         │
└─────────────────────────┘    └─────────────────────────┘
```

## Level 2: Container Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        pyCura System                            │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
│  │   CLI Interface │    │  Core Engine    │    │   Config    │  │
│  │   [Container]   │    │   [Container]   │    │ [Container] │  │
│  │                 │    │                 │    │             │  │
│  │ Command-line    │────│ Data processing │────│ TOML/JSON   │  │
│  │ interface for   │    │ orchestration   │    │ config      │  │
│  │ user commands   │    │ and workflow    │    │ files       │  │
│  └─────────────────┘    │ management      │    └─────────────┘  │
│                         └─────────┬───────┘                     │
│                                   │                             │
│  ┌─────────────────┐    ┌─────────▼───────┐    ┌─────────────┐  │
│  │   Data Parsers  │    │   Processors    │    │  Processing │  │
│  │   [Container]   │    │   [Container]   │    │  Modules    │  │
│  │                 │    │                 │    │ [Container] │  │
│  │ File format     │────│ Codebook &      │────│ Edits &     │  │
│  │ specific        │    │ Domain data     │    │ Inspections │  │
│  │ parsers         │    │ processors      │    │ functions   │  │
│  └─────────────────┘    └─────────────────┘    └─────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Level 3: Component Diagram - Core Engine

```
┌─────────────────────────────────────────────────────────────────┐
│                      Core Engine Container                      │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
│  │ ProjectManager  │    │CodebookProcessor│    │DomainData   │  │
│  │  [Component]    │    │  [Component]    │    │Processor    │  │
│  │                 │    │                 │    │[Component]  │  │
│  │ • Config load   │────│ • Parse codebook│    │• Parse data │  │
│  │ • Path setup    │    │ • Run edits     │    │• Run edits  │  │
│  │ • Logging       │    │ • Run inspect   │    │• Run inspect│  │
│  │ • Orchestration │    │ • Export        │    │• Export     │  │
│  └─────────────────┘    └─────────────────┘    └─────────────┘  │
│           │                       │                     │       │
│           │ Creates & configures  │                     │       │
│           └───────────────────────┼─────────────────────┘       │
│                                   │                             │
│  ┌─────────────────┐    ┌─────────▼───────┐    ┌─────────────┐  │
│  │ParsingManager   │    │ BaseProcessor   │    │ Utils       │  │
│  │ [Component]     │    │  [Component]    │    │[Component]  │  │
│  │                 │    │                 │    │             │  │
│  │ • Discover      │────│ • Abstract      │────│ • Helpers   │  │
│  │   parsers       │    │   processing    │    │ • Sorting   │  │
│  │ • Validate      │    │   logic         │    │ • Filtering │  │
│  │ • Route files   │    │ • Error handle  │    │             │  │
│  └─────────────────┘    └─────────────────┘    └─────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Level 4: Code Level - Key Classes

```python
# ProjectManager (God Object - Needs Refactoring)
class ProjectManager:
    - config: dict
    - logger: Logger
    - paths: dict
    - injections: dict
    + __init__(config_filename)
    + _load_config()
    + _setup_paths()
    + reset_project()

# CodebookProcessor 
class CodebookProcessor:
    - parsing_manager: ParsingManager
    - parsed_codebook: dict
    + run_codebook_pre_processing()
    + run_inspection_processing()
    + run_edit()

# DomainDataProcessor
class DomainDataProcessor:
    - parsing_manager: ParsingManager  
    - parsed_table: LazyFrame
    + run_domain_pre_processing()
    + run_inspection_processing()
    + run_edit()

# Pure Functions (Good - Keep These)
apply_padding(data, length, token) -> LazyFrame
apply_case(data, case_type) -> LazyFrame
length_map(data, columns) -> dict
char_map(data, columns) -> dict
```

## Complexity Analysis

**🔴 High Complexity:**
- ProjectManager: 366 lines, 15+ responsibilities
- CLI Interface: 386 lines, nested match statements
- Injection dictionaries: Complex coupling

**🟡 Medium Complexity:**
- Processors: Mixed concerns but focused
- Parsing managers: Dynamic discovery logic

**🟢 Low Complexity:**
- Pure functions: Single responsibility
- Data parsers: Clear interfaces
