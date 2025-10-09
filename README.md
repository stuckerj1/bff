# Benchmark Fabric Framework (BFF)

This repository contains a modular, repeatable benchmarking framework for Microsoft Fabric. It supports synthetic data generation, ingestion strategies, update logic, query benchmarking, and scorecard reporting.

## 🚀 Purpose
To evaluate ingestion, update, and query performance across different formats, strategies, and access modes in Microsoft Fabric.

## 🧩 Repo Structure
See [`GitHub Repo Structure`](#github-repo-structure-for-benchmarking-framework) section for full layout.

---

## 🛠️ Setup Instructions / Checklist

- [ ] Clone GitHub repo:  
  `git clone https://github.com/stuckerj1/bff.git`

- [ ] Fabric workspace created and assigned to Premium capacity  
  See 🏗️ Fabric Workspace Setup Details below for links

- [ ] Register Azure AD app:  
  Name: `FabricBenchmarkingProvisioner`

- [ ] Enable service principal access in Microsoft Fabric tenant settings:  
  - “Allow service principals to use Fabric APIs”  
  - “Allow service principals to create workspaces”

- [ ] Create client secret and capture:  
  - `TENANT_ID`  
  - `CLIENT_ID`  
  - `CLIENT_SECRET`  
  - `ADMIN_OBJECT_ID`  
  - `CAPACITY_ID`

- [ ] Add all five secrets to GitHub → Settings → Secrets → Actions

- [ ] Run GitHub Actions workflow: `Provision Fabric Benchmarking Workspace`

- [ ] Confirm Workspace created successfully:  
  - Workspace ID logged  
  - Admin role assigned with status code `201`  
  - Retry loop logs error metadata if assignment fails

- [ ] Run GitHub Actions workflow: `Provision Fabric Lakehouses`

- [ ] Confirm Lakehouses created successfully:  
  - Names: `BenchmarkLakehouse` and `DataSourceLakehouse`
  - Description: "Lakehouse for benchmarking synthetic data and update strategies"  
  - Created using `POST /v1/workspaces/{workspaceId}/lakehouses`  
  - Confirm status code `201` and capture `lakehouse_ids`

- [ ] Folder structure initialized:  
  - `base/` → for initial datasets  
  - `updates/` → for batch update slices  
  - `cdc/` → for CDC merge slices  
  - Folder creation handled via notebook logic or post-creation script

- [ ] Notebooks created for:  
  - [ ] Data generation  
  - [ ] Ingestion  
  - [ ] Updates  
  - [ ] Query benchmarking  
  - [ ] Metric capture

- [ ] Semantic model connected to Delta tables

- [ ] Power BI reports created for scorecard, refresh latency, and query performance

- [ ] Shortcuts created and validated

- [ ] Fabric Capacity Metrics App installed

---


# 📘 BFF Documentation: Context, Structure and Process

## 🔧 Parameterized Dimensions

| Dimension | Values | 
| --- | --- | 
| `row_count` | 10K, 1M | 
| `format` | Parquet, Delta | 
| `location` | Files, Tables, Shortcut | 
| `access_mode` | Native, Shortcut | 
| `query_type` | Filter, Join, Aggregate | 
| `update_strategy` | Full Refresh, Batch, CDC | 

---

## 🏗️ External Source Simulation Strategy

### External Data Ingestion

To simulate ingesting data from an external system into Microsoft Fabric, the framework currently uses a **separate Fabric lakehouse** as the external data source. Synthetic datasets are generated and stored in this lakehouse as files (Parquet or Delta). This enables realistic benchmarking and makes it easy to automate and parameterize ingestion workflows.

**Why use a separate lakehouse?**
- Mirrors common real-world scenarios where data lands in a data lake before ingestion into Fabric.
- Enables shortcut creation and metadata sync latency measurement.
- Simplifies automation, setup, and reproducibility.
- Provides flexibility to later swap in other external sources (SQL DB, Blob Storage) with minimal workflow changes.

**Future extensibility:**  
The workflow is designed so you can easily replace the external lakehouse with an external SQL database or Blob Storage source, to benchmark those ingestion patterns.

---

### Ingestion Targets

After simulating external data, the framework supports ingestion into three target types:

| Target Type           | Description                                   | Reporting Method         |
|-----------------------|-----------------------------------------------|-------------------------|
| Lakehouse (Parquet)   | Parquet files ingested into Fabric Lakehouse  | Python charts/notebooks  |
| Lakehouse (Delta)     | Delta tables ingested into Fabric Lakehouse   | Power BI reports        |
| Warehouse             | Ingestion into Fabric Warehouse (SQL tables)  | Power BI reports        |

- **Lakehouse (Parquet):** Used for Python-based metric charting and quick analysis.
- **Lakehouse (Delta) & Warehouse:** Used for Power BI reporting, scorecards, and advanced query performance metrics.

### Reporting Overview

- **Python charts** (matplotlib, seaborn, plotly) are used to visualize metrics for Parquet data in the lakehouse.
- **Power BI reports** are used for benchmarking and scorecard generation for Delta tables and warehouse data.

---

### Workflow Flexibility

The ingestion module is structured for easy swapping of external sources. To add new external sources:
- Implement new data generators or connectors (SQL, Blob, etc.) in the synthetic data and ingestion notebooks/scripts.
- Update the workflow to select the desired external source when running ingestion benchmarks.

---

## 🧩 Modular Components

### 1. Synthetic Data Generation
- **Purpose:** Create parameterized synthetic datasets (base and incremental slices) in DataSourceLakehouse.
- **Parameters:** `row_count` (e.g., 10K, 1M), schema, distribution, change %, insert %, delete %.
- **Output:** Parquet/Delta base data, batch update slices, CDC slices.

### 2. Ingestion & Lakehouse/Warehouse Provisioning
- **Targets:** 
  - Parquet in BenchmarkLakehouse
  - Delta in BenchmarkLakehouse with shortcut in BenchmarkWarehouse
  - Copy in BenchmarkWarehouse
- **Note:** Includes step to provision BenchmarkWarehouse.

### 3. Update Strategy & Incremental Load Performance Testing
- **Strategies:** Full Refresh, Batch (append/deduplication), CDC (merge logic).
- **Purpose:** Benchmark incremental ingestion for all targets.
- **Metrics:** Ingestion time, update latency, resource usage, reliability, correctness metrics.

### 4. Query Performance Testing
- **Modes:**
  - Notebook/Python (Parquet)
  - Power BI (Delta via shortcut)
  - Power BI (Warehouse copy)
- **Metrics:** Query time, refresh latency, resource usage.

### 5. Scorecard & Capacity/Cost Metrics Capture
- **Purpose:** Compile results, track workspace utilization, storage/compute footprint, refresh rates, and estimated cost per test case.
- **Tools:** Fabric Metrics App, notebook logging, Power BI dashboards, comparative tables.


## 🏗️ Deployment Architecture

**Workspace Setup**
- One dedicated Microsoft Fabric workspace
- Assigned to Premium capacity for consistent performance tracking

**Artifacts to Create**
- Lakehouse: Stores Parquet and Delta files/tables
- Notebooks: For synthetic data generation, ingestion, updates, and PySpark queries
- Semantic Model: Built from Delta tables for Power BI reporting
- Power BI Reports: Visualize scorecard, refresh latency, and query performance
- Shortcuts: Created to simulate cross-workspace access latency

## 📊 Sample Scorecard Layout

| Test Case | Format | Location | Rows | Update Strategy | Ingest Time | Storage Size | Query Type | Query Time | Notes | 
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | 
| TC01 | Parquet | Files | 10K | Full Refresh | 3.2s | 12 MB | N/A | N/A | No tabular access | 
| TC02 | Parquet | Files | 1M | Batch | 5.6s | 120 MB | N/A | N/A | Append + dedup | 
| TC03 | Parquet | Files | 1M | Full Refresh | 5.1s | 118 MB | N/A | N/A | Overwrite mode | 
| TC04 | Parquet | Files | 10K | Batch | 3.4s | 12 MB | N/A | N/A | Append + dedup | 
| TC05 | Parquet | Files | 10K | CDC | 4.2s | 13 MB | N/A | N/A | Merge logic applied | 
| TC06 | Parquet | Files | 1M | CDC | 6.3s | 122 MB | N/A | N/A | Merge logic applied | 
| TC07 | Delta | Tables | 10K | Full Refresh | 3.5s | 14 MB | Filter | 1.2s | Overwrite mode | 
| TC08 | Delta | Tables | 1M | Full Refresh | 6.2s | 130 MB | Aggregate | 2.8s | Overwrite mode | 
| TC09 | Delta | Tables | 10K | Batch | 4.1s | 14 MB | Join | 1.6s | Append + dedup | 
| TC10 | Delta | Tables | 1M | Batch | 6.8s | 132 MB | Filter | 2.2s | Append + dedup | 
| TC11 | Delta | Tables | 10K | CDC | 4.8s | 14 MB | Filter | 1.2s | Merge logic applied | 
| TC12 | Delta | Tables | 1M | CDC | 7.1s | 135 MB | Aggregate | 3.1s | Merge logic applied | 
| TC13 | Shortcut to Delta | Tables | 10K | Full Refresh | 2.9s | 0 MB | Filter | 1.1s | Metadata sync delay: 0.3s | 
| TC14 | Shortcut to Delta | Tables | 1M | Full Refresh | 5.4s | 0 MB | Join | 2.9s | Metadata sync delay: 0.5s | 
| TC15 | Shortcut to Delta | Tables | 10K | Batch | 3.2s | 0 MB | Aggregate | 1.4s | Append + dedup | 
| TC16 | Shortcut to Delta | Tables | 1M | Batch | 6.1s | 0 MB | Join | 3.2s | Append + dedup | 
| TC17 | Shortcut to Delta | Tables | 10K | CDC | 3.9s | 0 MB | Filter | 1.3s | Merge logic applied | 
| TC18 | Shortcut to Delta | Tables | 1M | CDC | 6.7s | 0 MB | Aggregate | 3.4s | Merge logic applied | 


---

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
- `updates/` → for batch update slices  
- `cdc/` → for CDC merge slices

---

### 🐍 Python File Scaffolds

#### `provision_workspace.py`
Creates a Fabric workspace and assigns admin role.

#### `provision_lakehouse.py`
Creates a Lakehouse inside the workspace.

#### `generate_data.py`
Generates synthetic datasets and update slices.

#### `ingest_data.py`
Handles ingestion logic for full refresh, batch, and CDC.

#### `benchmark_queries.py`
Executes filter, join, and aggregate queries and captures metrics.

#### `scorecard_generator.py`
Compiles results into a comparative scorecard.

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
    update_strategy: ["Full Refresh", "Batch", "CDC"]
    query_type: ["Filter", "Join", "Aggregate"]
  modules:
    - synthetic_data_generator
    - ingestion_module
    - update_strategy_module
    - query_benchmarking_module
    - scorecard_generator
    - metric_capture
```

### 📊 Scorecard Template (incomplete, need to flesh out)

| Test Case | Format | Location | Rows | Update Strategy | Ingest Time | Storage Size | Query Type | Query Time | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TC01 | Parquet | Files | 10K | Full Refresh |  |  |  |  |  |
| TC02 | Delta | Tables | 1M | CDC |  |  | Aggregate |  |  |
| TC03 | Shortcut | Tables | 10K | Batch |  |  | Join |  |  |


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
  - 1.3.1 Define folder structure for test cases (`base/`, `updates/`, `cdc/`)
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
  - 3.1.1 Parquet: Full Refresh, Batch, CDC
  - 3.1.2 Delta: Full Refresh, Batch, CDC
  - 3.1.3 Shortcut to Delta: All strategies
- 3.3 Apply update strategies
  - 3.3.1 Full Refresh (`overwrite`)
  - 3.3.2 Batch (`append` + deduplication)
  - 3.3.3 CDC (`MERGE INTO` or `applyChanges()`)
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
| `ingestion.ipynb` | 2.1.2, 3.1 | Ingests data using full refresh, batch, and CDC |
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
