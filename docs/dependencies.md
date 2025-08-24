# Dependencies Analysis

This document provides a comprehensive analysis of all dependencies used in pyCura, including their licenses and manual verification status.

## Summary

**Total Dependencies**: 32 packages  
**License Compatibility**: ✅ All licenses are MIT-compatible


## Dependency Table

| Package | Version | License | Manual Check | License Source | Notes | Link |
|---------|---------|---------|--------------|----------------|-------|------|
| annotated-types | 0.7.0 | MIT License | ❌ | [License](https://github.com/annotated-types/annotated-types/blob/main/LICENSE) | Pydantic dependency | [annotated-types](https://github.com/annotated-types/annotated-types) |
| cachetools | 5.5.2 | MIT License | ❌ | [License](https://github.com/tkem/cachetools/blob/main/LICENSE) | Caching utilities | [cachetools](https://github.com/tkem/cachetools) |
| certifi | 2025.4.26 | MPL-2.0 | ❌ | [License](https://github.com/certifi/python-certifi/blob/master/LICENSE) | Mozilla CA certificates | [certifi](https://github.com/certifi/python-certifi) |
| charset-normalizer | 3.4.2 | MIT License | ❌ | [License](https://github.com/Ousret/charset_normalizer/blob/main/LICENSE) | Character encoding detection | [charset-normalizer](https://github.com/Ousret/charset_normalizer) |
| click | 8.2.0 | BSD-3-Clause | ❌ | [License](https://github.com/pallets/click/blob/main/LICENSE) | CLI framework | [click](https://github.com/pallets/click) |
| coverage | 7.10.5 | Apache Software License | ❌ | [License](https://github.com/nedbat/coveragepy/blob/main/LICENSE) | Test coverage measurement | [coverage](https://github.com/nedbat/coveragepy) |
| fsspec | 2025.3.2 | BSD License | ❌ | [License](https://github.com/fsspec/filesystem_spec/blob/main/LICENSE) | Filesystem abstraction | [fsspec](https://github.com/fsspec/filesystem_spec) |
| greenlet | 3.2.2 | MIT AND Python-2.0 | ❌ | [License](https://github.com/python-greenlet/greenlet/blob/master/LICENSE) | Lightweight threading | [greenlet](https://github.com/python-greenlet/greenlet) |
| idna | 3.10 | BSD License | ❌ | [License](https://github.com/kjd/idna/blob/master/LICENSE.md) | Internationalized domain names | [idna](https://github.com/kjd/idna) |
| iniconfig | 2.1.0 | MIT License | ❌ | [License](https://github.com/pytest-dev/iniconfig/blob/main/LICENSE) | INI file parsing | [iniconfig](https://github.com/pytest-dev/iniconfig) |
| markdown-it-py | 3.0.0 | MIT License | ❌ | [License](https://github.com/executablebooks/markdown-it-py/blob/main/LICENSE) | Markdown parser | [markdown-it-py](https://github.com/executablebooks/markdown-it-py) |
| mdurl | 0.1.2 | MIT License | ❌ | [License](https://github.com/executablebooks/mdurl/blob/main/LICENSE) | URL utilities for markdown | [mdurl](https://github.com/executablebooks/mdurl) |
| mmh3 | 5.1.0 | MIT License | ❌ | [License](https://github.com/hajimes/mmh3/blob/main/LICENSE) | MurmurHash3 implementation | [mmh3](https://github.com/hajimes/mmh3) |
| packaging | 25.0 | Apache Software License \| BSD License | ❌ | [License](https://github.com/pypa/packaging/blob/main/LICENSE) | Package version handling | [packaging](https://github.com/pypa/packaging) |
| pip | 25.2 | MIT License | ❌ | [License](https://github.com/pypa/pip/blob/main/LICENSE.txt) | Package installer | [pip](https://github.com/pypa/pip) |
| pluggy | 1.6.0 | MIT License | ❌ | [License](https://github.com/pytest-dev/pluggy/blob/main/LICENSE) | Plugin system | [pluggy](https://github.com/pytest-dev/pluggy) |
| polars | 1.26.0 | MIT License | ❌ | [License](https://github.com/pola-rs/polars/blob/main/LICENSE) | **Core dependency** - DataFrame library | [polars](https://github.com/pola-rs/polars) |
| pyarrow | 19.0.1 | Apache Software License | ❌ | [License](https://github.com/apache/arrow/blob/main/LICENSE.txt) | Apache Arrow Python bindings | [pyarrow](https://github.com/apache/arrow) |
| pydantic | 2.11.4 | MIT License | ❌ | [License](https://github.com/pydantic/pydantic/blob/main/LICENSE) | Data validation library | [pydantic](https://github.com/pydantic/pydantic) |
| pydantic_core | 2.33.2 | MIT License | ❌ | [License](https://github.com/pydantic/pydantic-core/blob/main/LICENSE) | Data validation library | [pydantic_core](https://github.com/pydantic/pydantic-core) |
| Pygments | 2.19.1 | BSD-2-Clause \| BSD License | ❌ | [License](https://github.com/pygments/pygments/blob/master/LICENSE) | Syntax highlighting | [Pygments](https://github.com/pygments/pygments) |
| pyiceberg | 0.9.1 | Apache-2.0 \| Apache Software License | ❌ | [License](https://github.com/apache/iceberg-python/blob/main/LICENSE) | Apache Iceberg Python library | [pyiceberg](https://github.com/apache/iceberg-python) |
| pyparsing | 3.2.3 | MIT License | ❌ | [License](https://github.com/pyparsing/pyparsing/blob/master/LICENSE) | Parsing library | [pyparsing](https://github.com/pyparsing/pyparsing) |
| pytest | 8.4.1 | MIT License | ❌ | [License](https://github.com/pytest-dev/pytest/blob/main/LICENSE) | Testing framework | [pytest](https://github.com/pytest-dev/pytest) |
| pytest-cov | 6.2.1 | MIT License | ❌ | [License](https://github.com/pytest-dev/pytest-cov/blob/main/LICENSE) | Testing framework | [pytest-cov](https://github.com/pytest-dev/pytest-cov) |
| python-dateutil | 2.9.0.post0 | Dual License \| BSD License \| Apache Software License | ❌ | [License](https://github.com/dateutil/dateutil/blob/master/LICENSE) | Date/time utilities | [python-dateutil](https://github.com/dateutil/dateutil) |
| requests | 2.32.3 | Apache-2.0 \| Apache Software License | ❌ | [License](https://github.com/psf/requests/blob/main/LICENSE) | HTTP library | [requests](https://github.com/psf/requests) |
| rich | 13.9.4 | MIT License | ❌ | [License](https://github.com/Textualize/rich/blob/master/LICENSE) | Rich text formatting | [rich](https://github.com/Textualize/rich) |
| six | 1.17.0 | MIT License | ❌ | [License](https://github.com/benjaminp/six/blob/master/LICENSE) | Python 2/3 compatibility | [six](https://github.com/benjaminp/six) |
| sortedcontainers | 2.4.0 | Apache 2.0 \| Apache Software License | ❌ | [License](https://github.com/grantjenks/python-sortedcontainers/blob/master/LICENSE) | Sorted collections | [sortedcontainers](https://github.com/grantjenks/python-sortedcontainers) |
| SQLAlchemy | 2.0.43 | MIT License | ❌ | [License](https://github.com/sqlalchemy/sqlalchemy/blob/main/LICENSE) | SQL toolkit | [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) |
| strictyaml | 1.7.3 | MIT License | ❌ | [License](https://github.com/crdoconnor/strictyaml/blob/master/LICENSE.txt) | Strict YAML parser | [strictyaml](https://github.com/crdoconnor/strictyaml) |
| tenacity | 9.1.2 | Apache 2.0 \| Apache Software License | ❌ | [License](https://github.com/jd/tenacity/blob/main/LICENSE) | Retry library | [tenacity](https://github.com/jd/tenacity) |
| typing-inspection | 0.4.0 | MIT License | ❌ | [License](https://github.com/ilevkivskyi/typing_inspect/blob/master/LICENSE) | Type inspection utilities | [typing-inspection](https://github.com/ilevkivskyi/typing_inspect) |
| typing_extensions | 4.13.2 | Python Software Foundation License | ❌ | [License](https://github.com/python/typing_extensions/blob/main/LICENSE) | Type system extensions | [typing_extensions](https://github.com/python/typing_extensions) |
| urllib3 | 2.4.0 | MIT License | ❌ | [License](https://github.com/urllib3/urllib3/blob/main/LICENSE.txt) | HTTP client library | [urllib3](https://github.com/urllib3/urllib3) |

## MIT License Compatibility

**✅ All dependencies are compatible with MIT licensing**

- **MIT/BSD/Apache 2.0**: Permissive licenses allowing inclusion in MIT projects
- **MPL-2.0**: Weak copyleft, compatible for usage (no modification of certifi required)
- **PSF**: Python Software Foundation license, MIT-compatible

**Result**: pyCura can be safely published under MIT license with proper attribution.
