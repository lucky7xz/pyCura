# URI Support in pyCura's Path System

The pyCura path management system provides **native URI support** that leverages Polars' built-in cloud storage capabilities. This enables seamless integration with cloud storage providers while maintaining backward compatibility with local file systems.

## URI Detection & Path Handling

The system automatically detects and handles URIs using URL parsing:

```python
def _is_uri(self, path: str) -> bool:
    """Check if path is a URI (has scheme like s3://, gs://, http://)."""
    parsed = urlparse(str(path))
    return bool(parsed.scheme and parsed.scheme not in ['', 'file'])
```

Path joining is handled differently for URIs vs local paths:

```python
def _join_path(self, *parts: str) -> str:
    """Join path parts, handling both local paths and URIs."""
    if self._is_uri(base):
        # For URIs, always use forward slashes
        result = base.rstrip('/')
        for part in remaining:
            result += '/' + str(part).strip('/')
        return result
    else:
        # For local paths, use pathlib
        return str(Path(base) / part1 / part2)
```

## Supported URI Schemes

The path manager recognizes and handles these URI types:

- **Amazon S3**: `s3://bucket/path/data.csv`
- **Google Cloud Storage**: `gs://bucket/path/data.parquet`
- **Azure Blob Storage**: `abfss://container@account.dfs.core.windows.net/path/data.csv`
- **HTTP/HTTPS**: `https://example.com/data.csv`
- **Local files**: `/path/to/file.csv` (fallback)

## Configuration Examples

### Basic Local Configuration
```toml
# demo1.toml - Traditional local paths
project_name = "demo1"
domain_foldername = "demo1_dataset"
# Uses default local directories: data_in/, data_buffer/, data_out/
```

### Cloud Storage Configuration
```toml
# cloud_project.toml - URI-based paths
project_name = "cloud_analytics"
domain_foldername = "sales_data"

[paths]
base_input = "s3://data-lake/raw"
base_buffer = "s3://data-lake/buffer" 
base_output = "gs://analytics-output/processed"
```

### Mixed Environment Configuration
```toml
# hybrid_project.toml - Mix of cloud and local
project_name = "hybrid_pipeline"

[paths]
base_input = "s3://external-data/feeds"     # Cloud input
base_buffer = "data_buffer"                 # Local processing
base_output = "gs://company-warehouse/curated"  # Cloud output
```

## Data Source Resolution

The `resolve_data_sources()` method handles both local and cloud patterns:

```python
def resolve_data_sources(self, sources: Union[str, List[str]]) -> List[str]:
    """Resolve data source patterns to actual paths/URIs."""
    for source in sources:
        if self._is_uri(source_str):
            # For URIs, pass through (Polars handles expansion)
            resolved.append(source_str)
        else:
            # For local paths, expand globs
            if '*' in source_str:
                matches = glob.glob(source_str, recursive=True)
                resolved.extend(matches)
```

**Examples:**
- `s3://bucket/data/*.csv` → Passed to Polars for cloud-native glob expansion
- `./local/*.parquet` → Expanded using Python's `glob.glob()`
- `https://api.example.com/data.json` → Passed through as-is

## Polars Integration

The resolved URIs work directly with Polars I/O functions:

```python
# In processors - works with any URI type
input_path = self.get_input_path("domain")  # Could be s3://bucket/domain/

# Polars handles the URI automatically
df = pl.scan_csv(f"{input_path}/*.csv")     # Works with S3, GCS, Azure
df = pl.read_parquet(f"{input_path}/data.parquet")  # Cloud-native reading
```

## Authentication Methods

Authentication is handled through Polars' native cloud support:

### AWS S3
- **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- **IAM roles**: Automatic when running on EC2/ECS/Lambda
- **AWS profiles**: Using `~/.aws/credentials`
- **Storage options**: Passed directly to Polars functions

```python
# Example with storage options
storage_options = {
    "aws_access_key_id": "your_key",
    "aws_secret_access_key": "your_secret",
    "aws_region": "us-east-1"
}
df = pl.scan_parquet("s3://bucket/data.parquet", storage_options=storage_options)
```

### Google Cloud Storage
- **Service account keys**: `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- **Default credentials**: When running on GCP compute instances
- **Storage options**: Service account JSON path

```python
storage_options = {"SERVICE_ACCOUNT": "/path/to/service-account.json"}
df = pl.read_parquet("gs://bucket/data.parquet", storage_options=storage_options)
```

### Azure Blob Storage
- **Connection strings**: `AZURE_STORAGE_CONNECTION_STRING`
- **Account keys**: `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY`
- **Managed identity**: When running on Azure compute
- **Azure credential providers**: Using `DefaultAzureCredential`

## Smart Directory Management

The path manager only creates directories for local paths:

```python
def _ensure_directories(self) -> None:
    """Create directories only for local paths, skip URIs."""
    for path_value in all_paths:
        if not self._is_uri(path_value):  # Only create local directories
            Path(path_value).mkdir(parents=True, exist_ok=True)
        # URIs are skipped - cloud storage handles this automatically
```

## Path Validation

The system validates accessibility differently for local vs cloud paths:

```python
def validate_source_accessibility(self, sources: List[str]) -> Dict[str, Any]:
    """Validate data source accessibility."""
    for source in sources:
        if self._is_uri(source):
            # For URIs, assume accessible (Polars handles auth validation)
            accessible.append(source)
        else:
            # For local paths, check file existence
            if Path(source).exists():
                accessible.append(source)
```

## Resolved Path Structure Examples

### Local Project (demo1)
```
📁 data_in/demo1_dataset/
├── domain/          # → get_input_path("domain")
└── codebook/        # → get_input_path("codebook")

📁 data_buffer/project_demo1/
├── original_cb_mirror.json     # → get_buffer_path("cb_mirror")
├── filtered_cb_mirror.json     # → get_buffer_path("filtered_cb_mirror")
└── buffer_dd/                  # → get_buffer_path("filtered_dd_mirror")

📁 data_out/project_demo1/
├── inspection/      # → get_output_path("inspection")
├── key_exports/     # → get_output_path("key_exports")
└── final_data_data/ # → get_output_path("final_dd")
```

### Cloud Project
```
📁 s3://data-lake/raw/sales_data/
├── domain/          # → get_input_path("domain")
└── codebook/        # → get_input_path("codebook")

📁 s3://data-lake/buffer/cloud_analytics/
├── original_cb_mirror.json     # → get_buffer_path("cb_mirror")
└── buffer_dd/                  # → get_buffer_path("filtered_dd_mirror")

📁 gs://analytics-output/processed/cloud_analytics/
├── inspection/      # → get_output_path("inspection")
└── final_data_data/ # → get_output_path("final_dd")
```

## Key Benefits

1. **Transparent Integration**: Processors use the same API regardless of storage type
2. **Polars Native**: Leverages Polars' optimized cloud I/O with predicate pushdown
3. **Mixed Environments**: Can combine local and cloud paths in the same project
4. **Authentication Agnostic**: Uses standard cloud authentication methods
5. **Glob Support**: Works with patterns like `s3://bucket/data/*.csv`
6. **Backward Compatible**: Existing local configurations work unchanged

## Migration Path

Existing projects continue to work without changes. To add cloud storage:

1. **Add `[paths]` section** to your TOML config
2. **Specify URI-based paths** for the storage types you want to use
3. **Set up authentication** using your cloud provider's standard methods
4. **Test with small datasets** before migrating large workloads

The URI support is completely transparent to processors - they simply call `get_input_path()` and receive a path that works with Polars, whether local or cloud-based.
