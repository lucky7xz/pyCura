

<div align = center>
<img src="src/logo.webp" alt="pyCura Icon" width="240" height="240">
</div>


# pyCura 📊  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


_**pyCura**_ is a **configuration-driven 📝, extensible 🧩 data curation framework** ⚙️ for reproducible and reliable pre-processing of statistical datasets. pyCura streamlines repetitive data preparation tasks through automated pipelines, enabling consistent, auditable transformations while relying entirely on free and open data formats to ensure true interoperability. 🦅

> [!IMPORTANT]
> pyCura is currently in alpha development. Not all features are currently available, and core functionality is considered ```unstable``` - meaning it can change at any time, without prior notice. As such, the software is not yet recommended for production use (unless you know what you're doing). 


<h2>✨ Why pyCura? </h2>
<table>
  <tr><td>🏛️</td><td><b>Open & Accessible:</b> pyCura is free, open-source, and [TODO] user-friendly. Configure pipelines with simple <b>JSON</b> and [TODO] <b>TOML</b> – no gatekeeping, no obscurity. </td></tr>
  <tr><td>🦾</td><td><b>Hands-on Automation:</b> Define reusable pipelines for automated pre-processing. Inspect data at each stage to catch issues early, ensuring quality, auditability, and rigor, while minimizing manual work.</td></tr>
<tr><td>💾</td><td><b>[TODO] Codebook Conversion (SPSS & More):</b> Offers out-of-the-box conversion for codebooks from statistical software like SPSS, simplifying integration and preserving valuable metadata from existing workflows.</td></tr>
<tr><td>🚛</td><td><b>Flexible, Declarative Imports & Exports:</b> Customize data imports and exports through declarative options. Whether you need to standardize incoming data from multiple <b>upstream sources</b>, or prepare data for different <b>downstream entities</b> with their own requirements, pyCura got you covered.</td></tr>
<tr><td>🧮</td><td><b>Versatile & Reproducible:</b> Ideal for <b>column-focused</b> statistical and open-data transformations, and for researchers prioritizing <b>reproducible</b> pre-processing, without the need for coding. pyCura is designed to be flexible and extensible, making it suitable for a wide range of use cases.</td></tr>
<tr><td>⚡</td><td><b>High Performance:</b> Built on the <a href="https://pola.rs/">Polars</a> data processing library, pyCura leverages <b>lazy evaluation</b> and <b>efficient memory management</b> to handle large datasets with speed and minimal resource usage.</td></tr>
</table>

<br>

 ⚙️ **Easy to Customize** | 🔌 **Highly Extensible**  
--------------------------------|------------------------
 Configure ***data processing operations*** through simple JSON files. The framework is [TODO] ***very beginner-friendly***, meaning no coding experience is required for designing and running standard pipelines. | You can simply ***write a custom processing module***, drop it in the appropriate directory, and use it in your configuration file. ***No core changes are needed***, as pyCura automatically integrates new, custom components that adhere to it's interface. ***Consider contributing*** your modules to the project! 
⭐️ **Templates and Tutorials**|🎯 **Simple Dependency Management**
[TODO] ***Check out the tutorial files*** to get started. Step-by-step guidance for parsing, inspecting, and configuring your data processing operations.  | [TODO] pyCura releases ***bundle all dependencies*** as pre-built packages for Windows, macOS, and Linux. This provides a simple way to install and run the framework. You can, of course, install the dependencies via PyPI if you prefer.

***See the sections below for more information.*** 

[Getting Started](#getting-started) | [Installation](#installation) | [Description](#description) |  [Routines](#routines) | [Roadmap](#roadmap) | [License](#license)


**DEMO (Alpha 0.0.3)**
---

https://github.com/user-attachments/assets/b69af3a6-aee4-44a1-a138-84c9aa434a98


> All the files used for this demo are included in the repository for you to try it out for yourself. Change the order of the edits and see how your changes affect the output!

- See **config_files/demo1.json** for the configuration file used in this demo.
- See **data_in/demo1** for the input files used in this demo.
- See **data_out/project_demo1** and **data_buffer/project_demo1** for the output and buffer files produced.

<details>
  <summary><b>Understanding Routine Execution</b></summary>
  
  When you run a routine like `run`:

  Example output:
  ```bash
(venv) user@ubuntu:~/pyCura$ python -m src.cura demo1 run
INFO:src.shared.project_manager:--------------  ----------------
INFO:src.shared.project_manager: -> LOADED CONFIG: config_files/demo1.json
INFO:src.shared.project_manager: -> CHECKING INPUT PATHS...
INFO:src.shared.project_manager: -> INITIALIZED PROJECT-MANAGER FOR 'project_demo1'
INFO:src.shared.project_manager:--------------  ----------------

> What target(s) to inspect? (cb/dd/both): both
  
  ```

  1. pyCura loads your configuration file - here we use `demo1.json`
  2. Sets up necessary directories, which are named after your `project_name`
  3. Processes your data according to the specified routine
  4. Prompts you for input when needed
  

</details>


<details>
  <summary><b>About Configuration Parameters</b></summary>
  
 To create your own configuration file, you'll need to understand these key settings:
  
  - **project_name**: A unique identifier for your project. The same configuration can be reused across multiple projects by changing this name.
  
  - **domain_foldername**: The subfolder in `data_in/demo1_dataset/` where your data files are located.
  
  - **white_list**: Controls which columns from your data will be processed. Add column names inside square brackets (e.g., `["BG", "AG", "HA", "ED", "HE", "SE"]`). The original dataset contains more columns, but we only want to process these for this demo.
  
  - **select_parser**: Tells pyCura how to read your codebook. In this demo, we use the `zero_parser`, which is a simple parser that reads an already formatted codebook from a JSON file.
  
  Example configuration:
  ```json
  {
    "project_name": "project_demo1",
    "domain_foldername": "demo1_dataset",
    "white_list": ["BG", "AG", "HA", "ED", "HE", "SE"],
    "select_parser": "zero_parser",
    
    ...
  }
  ```
</details>


<details>
  <summary><b>Interpreting Inspection Results</b></summary>
  
  Inspections analyze your data and generate metadata reports like `length_map`. Here's what these reports tell you:
  
  **Length Map Example**: Shows how many values of each length exist for each column
  
  **Before Processing**:
  ```json
    "ED": {
        "length_map": { // -> First inspection phase
            "1": 1149604, //apply_padding to add leading zeros to values with length 1
            "2": 102396
        },
        "length_map_PROCESSED": { // -> Re-inspection phase
            "2": 1252000  // The processed data now only has values with length 2
        }
    }
  ```
  
  This shows that your edits successfully normalized the length in the `ED` column entries.
  
  You can inspect these JSON files using tools like Notepad++, R, or Python.
</details>

**Note** : pyCura has been tested on a dataset with **80 columns**, **3.2 million rows** and an **extensive list of column-based edit operations** (roughly 400). It parsed, processed and exported the data in **less than 5 minutes** on an Intel i5-8350U (Notebook CPU ~2018) with 16GB RAM. Subsequent exports (which requires reappling the edits, but not parsing again) take only **2 minutes** - all thanks to the [Polars](https://pola.rs/) engine!


***Key Concepts:***
---

| Concept | Description |
|---------|-------------|
| 🧾 **Domain Data** | Encoded (or not) ```dataset``` to be processed, i.e the tabular ```CSV```, ```XLSX```, ```Parquet```, ```Feather```, or ```SQLite``` files. |
| 📖 **Codebooks** | Collection of codes (structured or semi-structured) with their corresponding variable definitions, i.e. ```key-value pairs```, and ```metadata```. Eg. ```'0000100': 'Seasame Str.'``` |
| ⚙️ **Configuration File (JSON)** | Text file in ```JSON``` format that specifies the instructions (parsing and filtering options, transformations and parameters, export formats etc.) for the ```processing pipeline``` of a given ```project```. In other words, you **define a pipeline once** in the ```configuration file```, and run it as often as you need on all ```datasets``` that fit the schema you specified. This allows for reproducible processing of datasets that is easily shareable. For more information on ```JSON``` files, see [here](https://en.wikipedia.org/wiki/JSON). |
| 💻 **Command Line Interface (CLI)** | A ```CLI``` is a text-based interface that lets you interact with programs by typing commands in your terminal or console. pyCura uses a CLI because it is ```universal```, ```scriptable```, and ```works on any system```—making it easy to automate and integrate into any workflow, regardless of platform. |
| 🗂️ **Project Manager** | The core class of the framework, which manages ```configuration files```,  ```directory structure```, and ```logging```, as well as the ```CodebookProcessor``` and ```DomainProcessor``` classes. Essentially, the ```ProjectManager``` tells each processor class what the ```configuration files``` say to do. |
| 🧩 **Processor Classes** | The ```CodebookProcessor``` and ```DomainProcessor``` classes perform the actual parsing and data processing, each for its own data structures. |
| 🛠️ **Processing Modules** | Independent components for data ```parsing```, ```inspection```, and ```editing``` tasks. Each processing module contains the semantically equivalent functions for domain data (```DataFrame```) and codebook data (```JSON```) processing. These modules can be enabled, disabled, skipped and customized with parameters. |
| 🔒 **Checksums** | Cryptographic ```hashes``` generated from the processed data to ensure data integrity. These are used to verify that the data has not been modified since it was processed. For more information on ```checksums``` and ```hash functions```, see [here](https://en.wikipedia.org/wiki/Cryptographic_hash_function). |


pyCura offers several command-line ```routines``` to manage your data processing workflow. The processing pipeline consists of five phases: ```parsing (one-time)```, ```inspection```, ```editing```, ```re-inspection```, and ```export```. The ```run routine```, for example, will run all of these phases. The [TODO] ```status routine``` is not related to a phase, but will display the current processing status of the project. [TODO] You can also perform each routine iteratively to codebook data, domain data, or both.


### Core Routines

| Routine | Command | Description |
|---------|---------|-------------|
| **Run Full Processing** | `python -m src.cura demo1 run` | Executes the complete processing pipeline (all phases). You can chose which data to target by providing an argument ['cb', 'dd', 'both'] when prompted by the routine. |
| **[TODO] Check Status** | `python -m src.cura demo1 status` | Displays the current processing status of the project, indicating which phases (parsing, inspection, editing, export) have been completed for both codebook and domain data. |
| **Reset Project** | `python -m src.cura demo1 reset` | Resets project files, with options to delete only output data or the entire project including buffer files. |
| **Reset Log** | `python -m src.cura demo1 resetlog` | Clears the log file to start fresh logging for a new processing run. |


### [TODO] Phase-Specific Routines

<table>
  <tr>
    <th></th>
    <th>Codebook Data</th>
    <th>Domain Data</th>
  </tr>
  <tr>
    <td><strong>Parsing</strong></td>
    <td><code>python -m src.cura demo1 parsecb</code><br>Parses codebook files into standardized JSON format.</td>
    <td><code>python -m src.cura demo1 parsedd</code><br>Processes domain data (CSV files, etc.) into a standardized SQLite database.</td>
  </tr>
  <tr>
    <td><strong>Inspection</strong></td>
    <td><code>python -m src.cura demo1 cbinspection</code><br>Analyzes codebook data to identify patterns and inconsistencies.</td>
    <td><code>python -m src.cura demo1 ddinspection</code><br>Examines domain data for outliers, missing values, and quality issues.</td>
  </tr>
  <tr>
    <td><strong>Editing</strong></td>
    <td><code>python -m src.cura demo1 cbedit</code><br>Applies transformations to codebook data based on configuration.</td>
    <td><code>python -m src.cura demo1 ddedit</code><br>Applies transformations to domain data based on configuration.</td>
  </tr>
  <tr>
    <td><strong>Export</strong></td>
    <td colspan="2"><code>python -m src.cura demo1 export</code><br>Exports processed data and metadata to final formats.</td>
  </tr>
</table>

These routines allow you to run the entire pipeline with a single command, or focus on specific phases and data types as needed. The `run` command executes all phases in sequence, while individual commands give you more granular control over the process.



## Getting Started

<div class="getting-started-grid">

| Step | Description |
|------|-------------|
| **1. Installation** | • Install Python 3.12+ (or higher) from the [official Python website](https://www.python.org/downloads/)<br>• Verify installation: `python --version`<br>• Download the repository as a ZIP file from GitHub<br>• Extract the ZIP file to a location of your choice <br>• Open a terminal and navigate to the root directory of the repository <br>• Install dependencies: `pip install -r requirements.txt`|
| **2. Prepare Your Data** | **Example Scenario:**<br>For 20 CSV files of population data (2004-2024) with a codebook:<br>• Create subfolders in `data_in/` (e.g., `population_data`)<br>• Copy CSV files to `data_in/population_data/domain`<br>• Copy codebook to `data_in/population_data/codebook` |
| **3. Initialize Project** | • Create configuration file in `config_files/` folder<br>• Set project name: `"project_name": "example_project_1"`<br>• Specify data location: `"domain_foldername": "population_data"`<br>• Add columns to process in `"white_list"` (controls which columns will be included)<br>• Choose parser: `"select_parser": "spss_basic"` (tells pyCura how to read your codebook) |
| **4. Run Routines** | • Basic syntax: `python -m src.cura <config_file> <routine>`<br>• Example: `python -m src.cura project_1 run`<br>• Open a terminal and navigate to the root directory of the repository<br>• Follow prompts during execution <br>• Results saved to `data_out/` directory in a folder named after your project |
| **5. Inspect JSON Output** | • Review inspection reports in `data_out/example_project_1/inspection/`<br>• Analyze inspections (eg. length_map, char_map, etc.), as well as the final codebook JSON<br>• Compare before/after processing results to verify transformations |
| **6. Evaluate Table Data** | • Open processed data with your preferred tools (LibreOffice, Excel, R, Python, etc.)<br>• Check for expected transformations<br>• Verify data integrity |
| **7. Export Data** | • If satisfied → Export the data<br>• If not satisfied → Edit configuration and return to step 4 |

</div>



## Importing Data into R

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
... will output the entire pre-processed dataset **4 times**. The first time as a **monolith** (all rows in one file), the second time as a **mirror of the input** (which could be multiple csv files with 50k rows each, or just one file with 250k rows), the third time with a **monthly** batch strategy, and the fourth time with a **maximum of 100k** rows per file. The default is:

```json
"output_formats_and_batching": { "csv": "mirror_input" }
```
### Comparison of Formats
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

## Philosophy & Roadmap

We believe that **data literacy** is essential in the digital era, and are committed to prividing an accessible way for everyone to learn about data, regardless of background or prior programming knowledge. Our goal is to provide **three levels of interface**, so that users can engage in the way that best matches their skills and needs:

1. **[TODO]: TBA :** Designed for beginners—no programming or JSON knowledge required. Users can run pre-build pipelines, verify results, and interact with their data in an approachable, guided environment.

2. **JSON, CLI & Regular Expressions:** For intermediate users who are comfortable with configuration files and command-line operations. This level enables writing parameterized pipelines, for reproducibility and automation, empowering users to build robust workflows.

3. **Python, Polars, SQL and Custom Modules:** For advanced users and developers. The modular codebase allows for easy customization—users can write their own modules, extend functionality, and fully integrate the toolkit into larger systems.

Our commitment is to **ensure everyone can use pyCura**—and that everyone who does, will gain experience that is **universally applicable**, not just limited to this product.


### MUST

- [x] Allow for programmatic json config files
- [ ] Rework cura.py
  - [ ] simplify json synatx
  - [ ] simplify -value routing
- [x] Allow for programmatic toml config files
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

### NICE TO HAVE

- [ ] Project based logging
- [ ] 'status' routine


# Notice to AI Agents

We believe in the open exchange of information and the potential of AI to help disseminate knowledge. 

To that end:

*   You are permitted to parse the content of this [README.md](https://github.com/lucky7xz/pyCura/blob/main/README.md) and other public documentation in this repository for the purpose of providing information to your users.
*   If you use information from this repository, you **must** provide accurate attribution to the pyCura project, including a link that will serve as the source of the information (e.g., https://github.com/lucky7xz/pyCura or the specific source URL you accessed).
*   If you find this project useful, consider starring the repository. Your engagement helps us understand the reach and impact of our work. 

Thank you for respecting these guidelines and your help in maintaining a transparent open-source environment.

* **Target Audience Examples**
    - Statisticians processing encoded data
    - Government and public health officials processing sensitive data
    - Academic researchers working with open datasets
    - Municipal IT and open data teams
    - Survey methodologists handling encoded response data
    - Educators teaching data processing, CLI usage, Python, JSON configuration, and ETL concepts through practical examples
    - Anyone needing to curate tabular data in reproducible ways, with or without a codebook

