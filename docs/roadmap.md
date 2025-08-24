# 🗺️ Roadmap

We believe that **data literacy** is essential in the digital era, and are committed to providing an accessible way for everyone to learn about data. Our goal is to provide **three levels of interface**, so that users can engage in the way that best matches their skills and needs:

1. **🎯 GUI Interface** *(Planned)* — Designed for beginners—no programming required. Users can run pre-built pipelines, verify results, and interact with their data in an approachable, guided environment.

2. **⚙️ Configuration-Driven** *(Current)* — JSON, TOML, CLI & Regular Expressions for intermediate users comfortable with configuration files. This level enables writing parameterized pipelines.

3. **🐍 Python & Polars** *(Available)* — For advanced users and developers. The modular codebase allows for easy customization—users can write their own modules and use them in pyCura pipelines.

Our commitment is to **ensure everyone can use pyCura**—and that everyone who does, will gain experience that is **applicable universally**, not just limited to this product.

## 🎯 Core Development

### Architecture & Performance
- [x] **Configuration System** — JSON/TOML config files
- [x] **Enhanced Logging** — Project-specific + global debug logs  
- [x] **DataFrame Integration** — Polars output logging with formatting
- [x] **Exception Handling** — Enhanced error context and debugging
- [x] **Dependency Analysis** — MIT license compatibility verified
- [ ] **CLI Refactoring** — Simplify cura.py value routing
- [ ] **Memory Optimization** — Research polars >v1.26 RAM issues

### Data Format Support
- [x] **CSV** — Input/output support
- [ ] **Parquet** — Input/output support  
- [ ] **SQLite** — Input/output support
- [ ] **JSON** — Input/output support
- [ ] **Codebook Exports** — Multi-format support

### User Experience  
- [ ] **GUI Interface** — Beginner-friendly visual interface
- [ ] **Query Support** — Data querying capabilities
- [ ] **Status Routine** — Pipeline progress monitoring

*Current focus: Core stability and format expansion*

## 👥 Target Audience

**Primary Users:**
- 📊 Statisticians processing encoded data
- 🏛️ Government and public health officials handling sensitive data  
- 🎓 Academic researchers working with open datasets
- 🏢 Municipal IT and open data teams

**Secondary Users:**
- 📋 Survey methodologists handling encoded response data
- 👨‍🏫 Educators teaching data processing, CLI, Python, and ETL concepts
- 🔧 Anyone needing reproducible tabular data curation
