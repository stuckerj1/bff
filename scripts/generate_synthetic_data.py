#!/usr/bin/env python3
"""
Run the generate_data notebook using parameters from a YAML parameter file,
then write .state/datasets.json listing the dataset names produced.

Minimal change: pass parameters to papermill both as a single JSON string under
"spark.notebook.parameters" (what the notebook's %%configure cell reads) AND
also provide the top-level mapping so notebooks that declare explicit papermill
parameters can still receive them. This keeps things simple and avoids rewriting
the notebook or introducing extra complexity.
"""
import argparse
import json
from pathlib import Path
import sys
import yaml
import papermill as pm
from papermill.exceptions import PapermillExecutionError

STATE_DIR = Path(".state")
STATE_DIR.mkdir(exist_ok=True)

def load_params(yaml_path: str) -> dict:
    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg or {}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--params-file", required=True, help="YAML file with notebook parameters (e.g. config/test_parameter_sets.yml)")
    parser.add_argument("--notebook", required=True, help="Path to notebook to execute")
    parser.add_argument("--output", default=str(STATE_DIR / "generate_output.ipynb"), help="Output executed notebook path")
    args = parser.parse_args()

    params = load_params(args.params_file)

    # Build papermill parameters simply and plainly:
    # - include the top-level keys so notebooks expecting explicit papermill params receive them
    # - also include spark.notebook.parameters as the JSON string the notebook's %%configure cell expects
    papermill_params = {}
    if isinstance(params, dict):
        papermill_params.update(params)                     # top-level keys (if any)
    try:
        papermill_params["spark.notebook.parameters"] = json.dumps(params)
    except Exception:
        papermill_params["spark.notebook.parameters"] = "{}"

    print(f"Running notebook {args.notebook} -> {args.output} with params keys: {list(papermill_params.keys())}")
    try:
        pm.execute_notebook(
            args.notebook,
            args.output,
            parameters=papermill_params,
            progress_bar=False
        )
    except PapermillExecutionError as e:
        print("Notebook execution failed. Papermill raised an execution error.", file=sys.stderr)
        print(str(e), file=sys.stderr)
        raise

    # Produce .state/datasets.json containing dataset names (DATASETS_PARAM is expected)
    datasets = []
    if isinstance(params, dict) and "DATASETS_PARAM" in params:
        datasets = [d.get("name") for d in params["DATASETS_PARAM"] if isinstance(d, dict) and d.get("name")]
    else:
        maybe = params.get("datasets") or params.get("DATASETS") or params.get("parameter_sets") or params.get("parameterSets")
        if maybe and isinstance(maybe, list):
            datasets = [d.get("name") for d in maybe if isinstance(d, dict) and d.get("name")]

    out = STATE_DIR / "datasets.json"
    out.write_text(json.dumps({"datasets": datasets}, indent=2), encoding="utf-8")
    print(f"Wrote datasets list -> {out}")

if __name__ == "__main__":
    main()
