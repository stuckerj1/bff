# BFF — Benchmarking Fabric Framework (refactor plan)

This repo is being refactored to support running many parameterized benchmark workspaces so Fabric capacity/metrics can be captured per test configuration.

TL;DR — approach
- Use a parameter-sets YAML (config/parameter_sets.yml) to declare the datasets and test combinations.
- Each parameter set corresponds to one "actions" workspace where capacity metrics are most meaningful.
- A central "Controller" workspace will run the controller_orchestrator notebook/script to launch the per-workspace runs in sequence.
- Results (metrics table snapshot) are saved into a metrics lakehouse so the Controller workspace can visualize them centrally.

What I added (templates)
- config/parameter_sets.yml — declare datasets and parameter sets (row_count, source, format, update_strategy, etc.).
- notebooks/controller_orchestrator.ipynb — synapse-friendly orchestrator to loop parameter sets and run the canonical notebooks.
- Starter guidance and snippets to wire provisioning, orchestration, and visualization.

Prerequisites
- Common credentials available for provisioning and workspace operations and Azure SQL connection:  FabricBenchmarkingProvisioner

How to use (high level)
1. Edit config/parameter_sets.yml to add datasets and the parameter_sets (workspaces) you want to run.
2. Provision the BFF Controller workspace (manual or automated).
   - Generate synthetic data as specified in `datasets` (the Controller can seed both DataSourceLakehouse and external Azure SQL).
   - Create a `MetricsLakehouse` (a lakehouse that will host the central metrics table) used by the Controller for aggregation/visualization.
3. For each action workspace (workspace name = parameter_set.name), provision:
   - The notebooks: `1.IngestData`, `2.ApplyUpdates`, `3.RunQueries`
   - Data source: `DataSourceLakehouse` or `DataSource_Azure_SQL` as required by the parameter set
   - Data destination: `BenchmarkLakehouse` (delta tables) or `BenchmarkWarehouse` (warehouse tables) depending on `format`
4. In the BFF Controller workspace, run `notebooks/controller_orchestrator.py` (update `NOTEBOOK_PATHS` to refer to the notebook paths in the action workspaces). The orchestrator:
   - Reads `config/parameter_sets.yml`
   - Merges dataset-level settings (row counts, fractions, seeds) with each parameter_set
   - Calls each action workspace's notebooks (via `mssparkutils.notebook.run`) passing a params dict
   - Archives a snapshot of `BenchmarkLakehouse.metrics` per run into ADLS for long-term analysis
5. The Controller workspace hosts a visualization notebook that reads the central metrics table (or archived CSVs) to produce cross-workspace comparisons and capacity charts.

How metrics are written (recommended models)
- Recommended (Direct-write, simpler):
  - Each action workspace is given write access to a shared `MetricsLakehouse.metrics` table (grant the single credential write access).
  - Action notebooks append their run metrics directly to `MetricsLakehouse.metrics` (e.g., `spark.createDataFrame(...).write.mode('append').saveAsTable('MetricsLakehouse.metrics')`).
  - Controller reads this central table for visualization and/or archives periodic snapshots to ADLS.
  - Note: ensure the credential used by actions has permission to write to the metrics table.

Important operational notes
- Single credential assumption:
  - RBAC will not be an issue because you're using the same credentials everywhere. Put service principal or user in your secrets and ensure it has the required Fabric/ADLS/SQL roles (write/read for metrics and data).
- Capacity metrics latency:
  - Fabric capacity metrics and the Capacity Metrics app may lag (commonly hours, sometimes visible the next day). Plan analysis windows and account for refresh delays when correlating capacity usage to immediate run timings.
- Cost & concurrency:
  - Large runs (1M datasets, multiple workspaces concurrently) can be expensive. Start with a dry-run (10k) and stagger or serialize runs for larger tests to avoid quota or cost surprises.
- Naming & sanitization rules:
  - The parameter_set `name` is only used for the human workspace name.
    - Example: `"BFF 10k LH to WH Full Refresh"` works for a Fabric Workspace name.  All programmatic references use the GUIDs saved in the artifact files.
  - Use sanitized names for resource groups, storage folder names, and other Azure resource identifiers.
- Dataset naming mapping:
  - `datasets[].name` is the canonical dataset identifier used for:
    - Lakehouse folder naming: `/Files/{dataset_name}base/` and `/Files/{dataset_name}updates/`
    - Azure SQL table prefixes: e.g., `dbo` → tables `base_{dataset_name}`, `updates_{dataset_name}`

Parameter validation (apply in matrix-builder / CI)
- Validate for each `parameter_set`:
  - `dataset_name` exists in `datasets`
  - `source` is one of: `lakehouse`, `sql`
  - `format` is one of: `delta`, `warehouse`
  - `update_strategy` is one of: `Full Refresh`, `Full Compare`, `Incremental`
  - Fractional values (from dataset): `0 < change_fraction < 1`, `0 <= new_fraction < 1`, `0 <= delete_fraction < 1`
  - No duplicate `parameter_set.name` values
- The matrix-builder script (tools/build_matrix_from_params.py) should produce `sanitized_name` and the JSON `matrix.include` for GitHub Actions.

Example params dict passed from orchestrator to notebooks
```
{
  "workspace_name": "BFF 10k LH to WH Full Refresh",
  "sanitized_name": "bff-10k-lh-to-wh-full-refresh",
  "dataset_name": "10k",
  "row_count": "10000",
  "source": "lakehouse",
  "format": "warehouse",
  "update_strategy": "Full Refresh",
  "change_fraction": 0.01,
  "new_fraction": 0.005,
  "delete_fraction": 0.001,
  "seed": 42
}
```

Example code to retrieve parameters for workflow
```python
cfg = yaml.safe_load(open('/config/parameter_sets.yaml', 'r', encoding='utf-8'))
datasets = {d['name']: d for d in cfg.get('datasets', [])}
dataset_cfg = datasets.get(params.get('dataset_name'), {})
row_count = int(params.get('row_count', dataset_cfg.get('row_count', 10000)))
change_fraction = float(params.get('change_fraction', dataset_cfg.get('change_fraction', 0.01)))
# ... read other params similarly
```

Example first cell of notebook to capture parameters from the workflow
```python
%%configure -f
{
  "conf": {
    "spark.notebook.parameters": "{\"DATASETS_PARAM\": [{\"name\": \"1k\", \"row_count\": 1000, \"change_fraction\": 0.01, \"new_fraction\": 0.005, \"delete_fraction\": 0.001, \"seed\": 42, \"description\": \"Interactive small dataset (1k rows)\"}, {\"name\": \"100k\", \"row_count\": 100000, \"change_fraction\": 0.01, \"new_fraction\": 0.005, \"delete_fraction\": 0.001, \"seed\": 42, \"description\": \"Interactive medium dataset (100k rows)\"}], \"PUSH_TO_AZURE_SQL\": true, \"AZURE_SQL_SERVER\": \"benchmarking-bff\", \"AZURE_SQL_DB\": \"benchmarking\", \"AZURE_SQL_SCHEMA\": \"dbo\", \"distribution\": \"uniform\", \"seed\": 42}"
  },
  "defaultLakehouse": {
    "name": "DataSourceLakehouse"
  }
}
```

Rollout recommendation
- Start with a single parameter_set (10k full refresh). Confirm:
  - GenerateData creates base & updates in the Controller's DataSourceLakehouse and the SQL schema if `source: sql`.
  - IngestData and ApplyUpdates run successfully and write metrics to `MetricsLakehouse.metrics`.
  - The Controller can read/aggregate metrics and (optionally) archive to ADLS.
- Once validated, expand to additional parameter_sets and scale up to 1M runs with controlled concurrency.

Checklist before running large-scale jobs
- [ ] config/parameter_sets.yaml validated and present in repo
- [ ] Credentials (service principal / secrets) in place and documented
- [ ] Action workspaces provisioned with the required notebooks and dataset shortcuts
- [ ] MetricsLakehouse.metrics accessible and writable by the credential
- [ ] Dry-run validated (10k)
- [ ] Cost and quota limits reviewed

Next steps (suggested)
- If you want, I will:
  - produce the small matrix-builder / validator script (tools/build_matrix_from_params.py) that emits the GH Actions matrix and validates the config, or
  - produce the small GenerateData notebook header cell (to accept dataset params and write base/updates to lakehouse and SQL), or
  - produce a sample provision_workspaces GitHub Actions workflow that consumes the matrix and runs a placeholder provisioning step.
