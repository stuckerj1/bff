Last updated: 11/8/2025
Note:  at the time of this update, refactoring to one action workspace per parameter set.
This documentation is not yet fully updated to match.

### 🏗️ Fabric Workspace Setup Details

#### 🔧 Workspace Provisioning via REST API

- **Endpoint:**  
  `POST https://api.fabric.microsoft.com/v1/workspaces`

- **Headers:**
  ```http
  Authorization: Bearer <access_token>
  Content-Type: application/json
  ```

- **Payload:**
  ```json
  {
    "displayName": "Benchmarking Workspace",
    "description": "Workspace for Fabric benchmarking framework"
  }
  ```

- **Response:**  
  - Status code `201` on success  
  - Capture `workspace_id` from response JSON

---

#### 🔧 Assign Admin Role

- **Endpoint:**  
  `POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/permissions`

- **Payload:**
  ```json
  {
    "principalId": "<ADMIN_OBJECT_ID>",
    "principalType": "User",
    "roles": ["Admin"]
  }
  ```

- **Response:**  
  - Status code `201` on success  
  - Retry logic recommended for transient failures

---

#### 🔧 Create Lakehouse via REST API

- **Endpoint:**  
  `POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses`

- **Payload:**
  ```json
  {
    "displayName": "BenchmarkLakehouse",
    "description": "Lakehouse for benchmarking synthetic data and update strategies"
  }
  ```

- **Response:**  
  - Status code `201` on success  
  - Capture `lakehouse_id` from response JSON

---

#### 📁 Folder Initialization (via Notebook)

- `base/` → for initial datasets  
- `updates/` → for increment update slices  
- `cdc/` → for CDC merge slices (CDC deferred)

---

### 🐍 Python and Jupiter File Scaffolds

#### `provision_workspace.py`
Creates a Fabric workspace and assigns admin role.

#### `provision_lakehouses_warehouse.py`
Creates DataSourceLakehouse, BenchmarkLakehouse and BenchmarkWarehouse inside the workspace.

#### `provision_notebooks.py`
Creates 5 notebooks from the respective files.

#### `generate_data.ipynb` = `1.GenerateData`
Synthetic data generation.

#### `ingest_data.ipynb` = `2.IngestData`
Initial data load

#### `apply_updates.ipynb` = `3.ApplyUpdates`
Load updates and capture metrics for each update strategy

#### `run_queries.ipynb` = `4.RunQueries`
Capture query benchmarking timings

#### `visualize_metrics.ipynb` = `5.VisualizeMetrics`
Display metrics from capture

---

## 📦 Misc. Templates

### Project Manifest

```yaml
benchmark_project:
  name: "FabricBenchmarking"
  platform: "Microsoft Fabric"
  workspace: "BenchmarkingWorkspace"
  capacity: "Premium"
  dimensions:
    row_count: [10K, 1M]
    format: ["Parquet", "Delta"]
    location: ["Files", "Tables", "Shortcut"]
    update_strategy: ["Full Refresh", "Full Compare", "Increment"]
    query_type: ["Filter", "Join", "Aggregate"]
  modules:
    - synthetic_data_generator
    - ingestion_module
    - update_strategy_module
    - query_benchmarking_module
    - scorecard_generator
    - metric_capture
```
(CDC deferred)

### Automation Sweep

```yaml
automation:
  orchestrator: "benchmark_pipeline"
  sweep_parameters:
    - row_count
    - format
    - update_strategy
  output_targets:
    - PowerBI
    - Markdown
    - Excel
```

---

## 🧱 Work Breakdown Structure

### Phase 1: Project Initialization
- 1.1 Create Microsoft Fabric workspace (Premium capacity)
- 1.2 Assign workspace admin role
- 1.3 Create Lakehouse
  - 1.3.1 Define folder structure for test cases (`base/`, `updates/`, `cdc/`) (CDC deferred)
  - 1.3.2 Enable Delta support
- 1.4 Set up Fabric Capacity Metrics App
  - 1.4.1 Confirm workspace telemetry is active
  - 1.4.2 Validate access to refresh logs and utilization metrics

### Phase 2: Environment Setup
- 2.1 Create Notebooks
  - 2.1.1 Synthetic data generation
  - 2.1.2 Ingestion workflows
  - 2.1.3 Update strategy logic
  - 2.1.4 Query benchmarking
- 2.2 Create Semantic Model
  - 2.2.1 Connect to Delta tables
  - 2.2.2 Define measures and relationships
- 2.3 Create Power BI Reports
  - 2.3.1 Scorecard visualization
  - 2.3.2 Refresh latency tracking
  - 2.3.3 Query performance dashboards
  - 2.3.4 Capacity cost for storage
  - 2.3.5 Capacity cost for processing
- 2.4 Create Shortcuts
  - 2.4.1 Link to external lakehouse/table
  - 2.4.2 Validate metadata sync
- 2.5 Generate synthetic datasets
  - 2.5.1 Parameterize for Small (10K rows) & Large (1M rows)
  - 2.5.2 Initial data
  - 2.5.3 Change %
  - 2.5.4 New %
  - 2.5.5 Delete %

### Phase 3: Test Case Execution
- 3.1 Ingest datasets
  - 3.1.1 Parquet: Full Refresh, Full Compare, Increment (CDC deferred)
  - 3.1.2 Delta: Full Refresh, Full Compare, Increment (CDC deferred)
  - 3.1.3 Shortcut to Delta: All strategies
- 3.3 Apply update strategies
  - 3.3.1 Full Refresh (`overwrite`)
  - 3.3.2 Full Compare (detect and `append` changes)
  - 3.3.3 Increment (`append` changes`)
- 3.4 Run query benchmarks
  - 3.4.1 PySpark: Filter, Join, Aggregate
  - 3.4.2 Power BI: Refresh and report latency

### Phase 4: Metric Capture & Analysis
- 4.1 Capture ingestion metrics
  - 4.1.1 Ingestion time
  - 4.1.2 Storage footprint
- 4.2 Capture update metrics
  - 4.2.1 Execution time
  - 4.2.2 Data correctness
- 4.3 Capture query metrics
  - 4.3.1 Execution time
  - 4.3.2 Resource usage
- 4.4 Capture capacity & cost metrics
  - 4.4.1 Workspace utilization
  - 4.4.2 Refresh frequency impact
  - 4.4.3 Estimated cost per test case

### Phase 5: Reporting & Automation
- 5.1 Generate scorecard
  - 5.1.1 Tabular comparison of all test cases
  - 5.1.2 Highlight best/worst performers
- 5.2 Automate test sweeps
  - 5.2.1 Parameterized notebook execution
  - 5.2.2 Optional pipeline orchestration
- 5.3 Export results
  - 5.3.1 Power BI dashboards
  - 5.3.2 Markdown or Excel summary

### Phase 6: Metrics
- 6.1 Capacity metrics via Fabric Metrics App
- 6.2 Notebook execution time tracking
- 6.3 Shortcut metadata sync latency
- 6.4 Storage footprint and compute time

### 🧩 WBS-to-Python File Mapping

| WBS Phase | Task Description | Python File |
|-----------|------------------|-------------|
| 1.1–1.2 | Create Fabric workspace and assign admin role | `provision_workspace.py` |
| 1.3 | Create Lakehouse | `provision_lakehouse.py` |
| 2.5 | Generate synthetic datasets | `generate_data.py` |
| 3.1 | Ingest datasets | `ingest_data.py` |
| 3.3 | Apply update strategies | `ingest_data.py` |
| 3.4 | Run query benchmarks | `benchmark_queries.py` |
| 5.1 | Generate scorecard | `scorecard_generator.py` |
| 5.2 | Automate test sweeps | GitHub Actions workflows |
| 6.1–6.4 | Capture metrics | `benchmark_queries.py`, `scorecard_generator.py` |

### 📚 Artifact-to-WBS Mapping

#### 🧪 Notebooks

| Notebook Purpose | WBS Phase | Description |
|------------------|-----------|-------------|
| `data_generation.ipynb` | 2.1.1, 2.5 | Generates synthetic datasets and update slices |
| `ingestion.ipynb` | 2.1.2, 3.1 | Ingests data using full refresh, full compare, increment (CDC deferred) |
| `update_logic.ipynb` | 2.1.3, 3.3 | Applies update strategies and validates correctness |
| `query_benchmarking.ipynb` | 2.1.4, 3.4 | Runs filter, join, and aggregate queries |
| `metric_capture.ipynb` | 4.1–4.4 | Captures ingestion, update, query, and capacity metrics |

---

#### 📊 Semantic Model

| Component | WBS Phase | Description |
|-----------|-----------|-------------|
| Delta Table Connections | 2.2.1 | Connects semantic model to benchmarked Delta tables |
| Measures & Relationships | 2.2.2 | Defines metrics for ingestion, update, and query performance |

---

#### 📈 Power BI Reports

| Report Name | WBS Phase | Description |
|-------------|-----------|-------------|
| Scorecard | 2.3.1, 5.1 | Tabular comparison of test cases |
| Refresh Latency | 2.3.2, 4.1.1 | Tracks dataset and report refresh times |
| Query Performance | 2.3.3, 4.3 | Visualizes query execution metrics |
| Capacity Cost – Storage | 2.3.4, 4.4.1 | Estimates storage impact per test case |
| Capacity Cost – Processing | 2.3.5, 4.4.2 | Tracks compute time and refresh frequency impact |

## 🤪 Quirky Lessons Learned

### 🕰️ SQL Endpoints and Warehouse tables need time zones for time stamps

Ambiguous time zones are more than an end-user pain.  Data type `timestamp_ntz` 💥crashes both the warehouse table loads and delta table SQL endpoint reads.  We fix this in the synthetic data creation.  But it can break all too easily if time zone info is lost.
- `spark.read.parquet(base_file)` **(works)**: loads timestamp columns as `timestamp` (compatible with Warehouse).
- `spark.read.format("parquet").load(base_file)` **(does not work)**: loads timestamp columns as `timestamp_ntz` (not compatible).

Alternative helper function (if needed):
```python
from pyspark.sql.functions import col
def fix_timestamp_ntz(df):
    for field in df.schema.fields:
        if field.dataType.typeName() == 'timestamp_ntz':
            df = df.withColumn(field.name, col(field.name).cast("timestamp"))
    return df
```
### 🕳️ Disproving the NULL hypothesis

Some dataframe actions cause the df to think a column schema is `not nullable` when it has been, and always will be `nullable`.  The df schema nullability has to match the warehouse's nullability _exactly_ for every column, or the load 💥fails. We created a helper function to remind the df that it can take null values whenever it wants to.
