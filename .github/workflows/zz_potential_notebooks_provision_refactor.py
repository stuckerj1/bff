import yaml, json, sys
cfg = yaml.safe_load(open("config/test_parameter_sets.yml","r",encoding="utf-8")) or {}
per_workspace = [p['name'] for p in cfg.get('parameter_sets', [])]
controller = ["BFF-Controller"]
notebooks = [
  {"displayName":"0.GenerateData","description":"Global generate","file":"notebooks/generate_data.ipynb","workspaces":controller},
  {"displayName":"1.IngestData","description":"Test ingest","file":"notebooks/ingest_data.ipynb","workspaces":per_workspace},
  {"displayName":"2.ApplyUpdates","description":"Test updates","file":"notebooks/apply_updates.ipynb","workspaces":per_workspace},
  {"displayName":"3.Queries","description":"Test queries","file":"notebooks/queries.ipynb","workspaces":per_workspace},
  {"displayName":"4.RunBenchmarks","description":"Global visualize","file":"notebooks/run_benchmarks.ipynb","workspaces":controller}
  {"displayName":"5.VisualizeMetrics","description":"Global visualize","file":"notebooks/visualize_metrics.ipynb","workspaces":controller}
]
open(".state/notebooks_to_create.json","w",encoding="utf-8").write(json.dumps(notebooks,indent=2))
print("Prepared notebooks_to_create with", len(per_workspace), "test workspaces", file=sys.stderr)
