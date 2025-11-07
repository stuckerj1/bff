#!/usr/bin/env python3
"""
Run the generate_data notebook using parameters from a YAML parameter file,
then write .state/datasets.json listing the dataset names produced.

Assumptions:
 - The notebook has a parameters cell compatible with papermill (the notebook already
   includes a %%configure -f parameters cell tagged 'parameters').
 - config/test_parameter_sets.yml contains the dataset configuration (DATASETS_PARAM, etc).
"""
import argparse
import json
from pathlib import Path
import sys
import yaml
import papermill as pm

STATE_DIR = Path(".state")
STATE_DIR.mkdir(exist_ok=True)

def load_params(yaml_path):
    with open(yaml_path, "r") as f:
        cfg = yaml.safe_load(f)
    # The test_parameter_sets.yml may define DATASETS_PARAM or a top-level mapping
    # expected by the notebook. We'll pass the whole file as the parameters dict
    return cfg

def run_notebook(notebook_path, output_path, params):
    # papermill will inject parameters into the notebook's parameters cell
    print(f"Running notebook {notebook_path} -> {output_path} with params keys: {list(params.keys())}")
    pm.execute_notebook(
        notebook_path,
        output_path,
        parameters=params,
        progress_bar=False
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--params-file", required=True, help="YAML file with notebook parameters (e.g. config/test_parameter_sets.yml)")
    parser.add_argument("--notebook", required=True, help="Path to notebook to execute")
    parser.add_argument("--output", default=str(STATE_DIR / "generate_output.ipynb"), help="Output executed notebook path")
    args = parser.parse_args()

    params = load_params(args.params_file)

    run_notebook(args.notebook, args.output, params)
    
    # Produce .state/datasets.json containing dataset names (DATASETS_PARAM is expected)
    datasets = []
    datasets = [d.get("name") for d in params["DATASETS_PARAM"]]

    out = STATE_DIR / "datasets.json"
    out.write_text(json.dumps({"datasets": datasets}, indent=2))
    print(f"Wrote datasets list -> {out}")

if __name__ == "__main__":
    main()
