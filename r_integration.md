# R Integration Guide

After pre-processing your data with pyCura, you can import it directly into R. As of now, you can **choose between multiple data formats and output-batching strategies** via the configuration file. Each supported format has slighly different properties and performance. The output-batching strategy defines how rows are grouped together in the output files. You can, of course, also output the data as a monolithic file.  

For example:

```json
"output_formats_and_batching": { 
  "parquet": "monolith", 
  "feather": "mirror_input", 
  "csv": "MONTH" ,
  "xlsx": "100000"

}
```
... will output the entire pre-processed dataset **4 times**. The first time as a **monolith** (all rows in one file), the second time as a **mirror of the input** (which could be multiple csv files with 50k rows each, or just one file with 250k rows), the third time with a **column-based** batch strategy, and the fourth time with a **maximum of 100k** rows per file. The default is:

```json
"output_formats_and_batching": { "csv": "mirror_input" }
```
## Comparison of Formats
<table>
  <thead>
    <tr>
      <th>Format</th>
      <th>Speed (Read/Write)</th>
      <th>File Size</th>
      <th>R Package</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Feather</td>
      <td>🔥🔥🔥</td>
      <td>Moderate</td>
      <td>arrow</td>
      <td>Best for R < --- > Python workflows.</td>
    </tr>
    <tr>
      <td>Parquet</td>
      <td>🔥🔥🔥</td>
      <td>Compact</td>
      <td>arrow</td>
      <td>Best for analytics use cases (with huge datasets).</td>
    </tr>
    <tr>
      <td>CSV</td>
      <td>🔥</td>
      <td>Large</td>
      <td>data.table, utils</td>
      <td>Universal, but slower and less space-efficient.</td>
    </tr>
  </tbody>
</table>

## Format-Specific Instructions

The output paths of pyCura projects are **hardcoded** and **determined by the project name** (defined in the config file).
As such, you can define the base path in R as follows:

```r
# As an Absolute Path (recommended)
# --> meaning the path points from the root of your file system to the data_out directory

base_path <- "absolute_path_to_pyCura/data_out/project_name/domain_export"

# eg. for Windows
base_path <- "C:/Users/username/Desktop/pyCura/data_out/project_name/domain_export"

# eg. for Linux/Mac
base_path <- "/home/username/Desktop/pyCura/data_out/project_name/domain_export"

```

The output path of the domain data is relative to your R working directory.

```r
base_path <- "absolute_path_to_pyCura/data_out/project_name/domain_export"

```

### 1. Arrow/Parquet 

#### Read Parquet file in R

```r
library(arrow)
df <- read_parquet(file.path(base_path, "parquet", "data.parquet"))
print(df)
```

### 2. Feather/Arrow IPC

```r
library(arrow)
df <- read_feather(file.path(base_path, "feather", "data.feather"))
print(df)
```

### 3. CSV

```r
df <- read.csv(file.path(base_path, "csv", "data.csv"))
print(df)
```
