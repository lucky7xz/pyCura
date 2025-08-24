# Roadmap

We believe that **data literacy** is essential in the digital era, and are committed to prividing an accessible way for everyone to learn about data. Our goal is to provide **three levels of interface**, so that users can engage in the way that best matches their skills and needs:

1. **[TODO]: TBA :** Designed for beginners—no programming required. Users can run pre-build pipelines, verify results, and interact with their data in an approachable, guided environment.

2. **JSON, TOML, CLI & Regular Expressions:** For intermediate users who are comfortable with configuration files. This level enables writing parameterized pipelines.

3. **Python, Polars, SQL and Custom Modules:** For advanced users and developers. The modular codebase allows for easy customization—users can write their own modules and use them in pyCura pipelines.

Our commitment is to **ensure everyone can use pyCura**—and that everyone who does, will gain experience that is **applicable universally**, not just limited to this product.

## MUST

- [x] Allow for programmatic json config files
- [x] Allow for programmatic toml config files
- [ ] Rework cura.py
  - [ ] simplify -value routing
- [ ] Research out-of-ram Issue with polars >v1.26
- [ ] Easy interface (GUI) ideas
- [ ] Query support

- [ ] Enable support for
    - [ ] domain data input formats  
        - [x] csv
        - [ ] parquet
        - [ ] sqlite
        - [ ] json
    - [ ] domain data output formats
        - [x] csv
        - [ ] parquet
        - [ ] sqlite
        - [ ] json
    - [ ] Same for codebook exports

- [ ] Update README

## NICE TO HAVE

- [x] Project based logging
- [ ] 'status' routine

## Target Audience Examples
- Statisticians processing encoded data
- Government and public health officials processing sensitive data
- Academic researchers working with open datasets
- Municipal IT and open data teams
- Survey methodologists handling encoded response data
- Educators teaching data processing, CLI usage, Python, JSON configuration, and ETL concepts through practical examples
- Anyone needing to curate tabular data in reproducible ways, with or without a codebook
