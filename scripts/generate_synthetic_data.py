#!/usr/bin/env python3
"""
Run the generate_data notebook using parameters from a YAML parameter file,
then write .state/datasets.json listing the dataset names produced.

This version ensures parameters are passed to the notebook in the same
shape the notebook expects: a single string stored under the key
"spark.notebook.parameters" (JSON-encoded). Papermill will inject that
string into the notebook's parameters cell so the notebook can read it
the same way as when run interactively.
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
    return cfg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--params-file", required=True, help="YAML file with notebook parameters (e.g. config/test_parameter_sets.yml)")
    parser.add_argument("--notebook", required=True, help="Path to notebook to execute")
    parser.add_argument("--output", default=str(STATE_DIR / "generate_output.ipynb"), help="Output executed notebook path")
    args = parser.parse_args()

    params = load_params(args.params_file)
    # Papermill will accept any mapping for parameters, but your notebook expects a single
    # spark.notebook.parameters JSON string (see its %%configure -f cell). Encode the full
    # params mapping as a JSON string and pass it under that key so the notebook reads it
    # unchanged via spark.conf.get("spark.notebook.parameters") or similar.
    papermill_params = {"spark.notebook.parameters": json.dumps(params)}

    print(f"Running notebook {args.notebook} -> {args.output} with injected key: spark.notebook.parameters")
    try:
        pm.execute_notebook(
            args.notebook,
            args.output,
            parameters=papermill_params,
            progress_bar=False
        )
    except PapermillExecutionError as e:
        # Surface a helpful message and re-raise so CI shows the error
        print("Notebook execution failed. Papermill raised an execution error.", file=sys.stderr)
        # print the short error for logs
        print(str(e), file=sys.stderr)
        raise

    # Produce .state/datasets.json containing dataset names (DATASETS_PARAM is expected)
    datasets = []
    if isinstance(params, dict) and "DATASETS_PARAM" in params:
        datasets = [d.get("name") for d in params["DATASETS_PARAM"]]
    else:
        maybe = params.get("datasets") or params.get("DATASETS") or params.get("parameter_sets") or params.get("parameterSets")
        if maybe and isinstance(maybe, list):
            datasets = [d.get("name") for d in maybe if isinstance(d, dict) and d.get("name")]
    out = STATE_DIR / "datasets.json"
    out.write_text(json.dumps({"datasets": datasets}, indent=2), encoding="utf-8")
    print(f"Wrote datasets list -> {out}")

if __name__ == "__main__":
    main()
