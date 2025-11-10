#!/usr/bin/env python3
"""
Provision and upload notebooks to Fabric workspaces.

Behavior (minimal):
- Read config/test_parameter_sets.yml from the repo and synthesize notebooks-to-create manifest.
- Populate the notebooks' %%configure parameter cells dynamically from the parameter file:
  - generate_data.ipynb -> DATASETS_PARAM populated from datasets list
  - ingest_data/apply_updates/queries -> single-parameter-set shape for each workspace (includes Azure SQL params)
  - run_benchmarks/visualize_metrics -> runs list containing all parameter_sets
- Authenticate using client credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET).
- For each notebook->workspace:
  - find workspace id by displayName (via GET /workspaces)
  - replace the notebook parameter cell only when an explicit parameters cell is present
  - base64-encode the .ipynb and POST to /workspaces/{workspace_id}/items
  - if the create returns 202, poll /workspaces/{workspace_id}/items until the notebook appears (best-effort)
- Write .state/notebooks_created.json with one entry per notebook->workspace mapping.

This is intentionally the "happy path" provisioner and will not attempt fallbacks when the expected parameter cell or parameter set is missing.
"""
from __future__ import annotations
import base64
import json
import os
import sys
import time
from pathlib import Path
import requests
import yaml

# constants / easy-to-change parameter file name
PARAMS_SOURCE = "config/test_parameter_sets.yml"  # change to parameter_sets.yml later if desired

STATE_DIR = Path(".state")
STATE_DIR.mkdir(exist_ok=True)
OUT_FILE = STATE_DIR / "notebooks_created.json"
MANIFEST_FILE = STATE_DIR / "notebooks_to_create.json"

OAUTH_TIMEOUT = 30
API_BASE = "https://api.fabric.microsoft.com/v1"
UPLOAD_TIMEOUT = 60
POLL_SLEEP = 5
POLL_ATTEMPTS = 20

# read auth env (require these in one place, same style)
tenant = os.environ.get("TENANT_ID")
client = os.environ.get("CLIENT_ID")
secret = os.environ.get("CLIENT_SECRET")
az_server = os.environ.get("AZURE_SQL_SERVER")
az_db = os.environ.get("AZURE_SQL_DB")
# AZURE_SQL_SCHEMA is optional
az_schema = os.environ.get("AZURE_SQL_SCHEMA")

if not (tenant and client and secret and az_server and az_db):
    print("TENANT_ID, CLIENT_ID, CLIENT_SECRET, AZURE_SQL_SERVER and AZURE_SQL_DB environment variables are required.", file=sys.stderr)
    sys.exit(4)

# Synthesize notebooks manifest from config/test_parameter_sets.yml (always)
cfg_path = Path(PARAMS_SOURCE)
cfg = yaml.safe_load(open(cfg_path, "r", encoding="utf-8")) or {}

per_workspace = [p["name"] for p in cfg.get("parameter_sets", [])]
controller = ["BFF-Controller"]

notebooks = [
  {"displayName":"0.GenerateData","description":"Global generate","file":"notebooks/generate_data.ipynb","workspaces": controller},
  {"displayName":"1.IngestData","description":"Test ingest","file":"notebooks/ingest_data.ipynb","workspaces": per_workspace},
  {"displayName":"2.ApplyUpdates","description":"Test updates","file":"notebooks/apply_updates.ipynb","workspaces": per_workspace},
  {"displayName":"3.Queries","description":"Test queries","file":"notebooks/queries.ipynb","workspaces": per_workspace},
  {"displayName":"4.RunBenchmarks","description":"Global run/visualize","file":"notebooks/run_benchmarks.ipynb","workspaces": controller},
  {"displayName":"5.VisualizeMetrics","description":"Global visualize","file":"notebooks/visualize_metrics.ipynb","workspaces": controller}
]

# persist the generated manifest for debugging / workflow artifact upload
MANIFEST_FILE.write_text(json.dumps(notebooks, indent=2), encoding="utf-8")
print(f"Wrote synthesized notebooks manifest -> {MANIFEST_FILE}")


# obtain AAD token via client credentials
token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
data = {
    "grant_type": "client_credentials",
    "client_id": client,
    "client_secret": secret,
    "scope": "https://api.fabric.microsoft.com/.default"
}
r = requests.post(token_url, data=data, timeout=OAUTH_TIMEOUT)
if r.status_code != 200:
    print(f"Failed to obtain AAD token: {r.status_code} {r.text}", file=sys.stderr)
    sys.exit(5)
token = r.json().get("access_token")
if not token:
    print("AAD token response missing access_token", file=sys.stderr)
    sys.exit(6)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# fetch workspaces list once and build map displayName -> id
wr = requests.get(f"{API_BASE}/workspaces", headers=headers, timeout=30)
wr.raise_for_status()
workspace_list = wr.json().get("value", [])
workspaces_by_name = {w.get("displayName"): w.get("id") for w in workspace_list if w.get("displayName")}

results = []

# helper: poll for item presence
def _poll_for_item(workspace_id: str, target_display: str) -> str | None:
    items_url = f"{API_BASE}/workspaces/{workspace_id}/items"
    for attempt in range(1, POLL_ATTEMPTS + 1):
        ir = requests.get(items_url, headers=headers, timeout=30)
        if ir.status_code == 200:
            vals = ir.json().get("value", [])
            for it in vals:
                if it.get("displayName") == target_display and it.get("type") == "Notebook":
                    return it.get("id")
        time.sleep(POLL_SLEEP)
    return None

# --- New: helpers to build parameter cells for notebooks before upload ---

def _find_and_replace_parameters_cell(ipynb: dict, new_cell_source: str) -> bool:
    """
    Find the code cell with spark.notebook.parameters (or tagged 'parameters') and replace its source.
    IMPORTANT: This function will NOT do any fallback replacement. It only replaces if a cell
    explicitly contains 'spark.notebook.parameters' or has the 'parameters' tag.
    Returns True if replaced, False otherwise.
    """
    cells = ipynb.get("cells", [])
    for c in cells:
        meta = c.get("metadata", {}) or {}
        tags = meta.get("tags", []) or []
        src_text = "".join(c.get("source", [])) if c.get("source") else ""
        if "parameters" in tags or "spark.notebook.parameters" in src_text:
            c["cell_type"] = "code"
            c["metadata"] = c.get("metadata", {})
            c["source"] = [new_cell_source]
            return True
    return False

def _make_generate_data_cell(datasets_list: list) -> str:
    # Use environment AZURE_SQL_SERVER/AZURE_SQL_DB (already required earlier); do not hard-code.
    inner = {
        "DATASETS_PARAM": datasets_list,
        "PUSH_TO_AZURE_SQL": True,
        "distribution": "uniform",
        "seed": 42
    }
    # populate from environment (we required these earlier)
    inner["AZURE_SQL_SERVER"] = az_server
    inner["AZURE_SQL_DB"] = az_db
    if az_schema:
        inner["AZURE_SQL_SCHEMA"] = az_schema
    outer = {
        "conf": {
            "spark.notebook.parameters": json.dumps(inner, ensure_ascii=False)
        },
        "defaultLakehouse": {"name": "DataSourceLakehouse"}
    }
    return "%%configure -f\n" + json.dumps(outer, indent=2, ensure_ascii=False) + "\n"

def _make_single_run_cell(param_obj: dict) -> str:
    """
    Build single-workspace parameter cell for ingest/apply_updates/queries.
    IMPORTANT: do not fabricate fallback parameter-sets. Replacement only happens when the parameter-set exists
    in cfg (matched by workspace name). Also do not synthesize missing AZURE_SQL_* values here because they
    are required from the environment at script start.
    """
    # copy the provided param_obj exactly
    p = dict(param_obj or {})
    outer = {
        "conf": {
            "spark.notebook.parameters": json.dumps(p, ensure_ascii=False)
        },
        "defaultLakehouse": {"name": "BenchmarkLakehouse"}
    }
    return "%%configure -f\n" + json.dumps(outer, indent=2, ensure_ascii=False) + "\n"

def _make_runs_cell(all_param_sets: list) -> str:
    """
    Build the 'runs' cell for run_benchmarks/visualize_metrics.

    For any parameter_set whose source == 'sql', ensure AZURE_SQL_SERVER and AZURE_SQL_DB
    are present in the run entry (populated from environment which is required earlier).
    This prevents notebook runs that expect SQL connection info from failing.
    """
    runs = []
    for p in all_param_sets:
        # Build a minimal run entry but propagate necessary SQL connection fields for source==sql
        run_entry = {
            "name": p.get("name"),
            "dataset_name": p.get("dataset_name"),
            "source": p.get("source"),
            "format": p.get("format"),
            "update_strategy": p.get("update_strategy")
        }
        if (p.get("source") or "").lower() == "sql":
            # ensure connection info present for SQL runs
            run_entry["AZURE_SQL_SERVER"] = az_server
            run_entry["AZURE_SQL_DB"] = az_db
            if az_schema:
                run_entry["AZURE_SQL_SCHEMA"] = az_schema
        runs.append(run_entry)
    outer = {
        "conf": {
            "spark.notebook.parameters": json.dumps({"runs": runs}, ensure_ascii=False)
        }
    }
    return "%%configure -f\n" + json.dumps(outer, indent=2, ensure_ascii=False) + "\n"

# process each notebook entry
for nb in notebooks:
    display = nb.get("displayName") or nb.get("displayname") or nb.get("name") or "unnamed"
    desc = nb.get("description", "") or ""
    file_path = nb.get("file")
    workspaces = nb.get("workspaces") or nb.get("workspace") or []
    if isinstance(workspaces, str):
        workspaces = [workspaces]

    if not workspaces:
        results.append({
            "displayName": display,
            "workspace": None,
            "file": file_path,
            "status": "skipped_no_workspace",
            "description": desc
        })
        continue

    # read notebook bytes once
    ipynb_bytes = None
    if file_path:
        src = Path(file_path)
        if src.exists():
            ipynb_bytes = src.read_bytes()

    for ws in workspaces:
        entry = {
            "displayName": display,
            "workspace": ws,
            "file": file_path,
            "description": desc,
            "workspace_id": None,
            "notebook_id": None,
            "status": None,
            "response_code": None,
            "response_text": None
        }

        ws_id = workspaces_by_name.get(ws)
        if not ws_id:
            entry["status"] = "workspace_not_found"
            results.append(entry)
            continue
        entry["workspace_id"] = ws_id

        if not ipynb_bytes:
            entry["status"] = "missing_source_or_read_failed"
            results.append(entry)
            continue

        # Happy path: assume ipynb bytes decode to UTF-8 and are valid JSON.
        ipynb_text = ipynb_bytes.decode("utf-8")
        ipynb_json = json.loads(ipynb_text)

        modified_payload_bytes = ipynb_bytes  # default to original bytes if we can't modify

        fname = Path(file_path).name.lower() if file_path else ""

        replaced = False
        # generate appropriate new configure cell source
        if "generate_data" in fname or "generate_data.ipynb" in fname:
            datasets_list = cfg.get("datasets", [])
            if datasets_list:  # only replace when datasets exist
                new_cell = _make_generate_data_cell(datasets_list)
                replaced = _find_and_replace_parameters_cell(ipynb_json, new_cell)
            else:
                replaced = False
        elif any(x in fname for x in ("ingest_data", "apply_updates", "queries")):
            # find the parameter set for this workspace (match by name) -- NO fallback
            param_set = next((p for p in cfg.get("parameter_sets", []) if p.get("name") == ws), None)
            if param_set:
                new_cell = _make_single_run_cell(param_set)
                replaced = _find_and_replace_parameters_cell(ipynb_json, new_cell)
            else:
                replaced = False
        elif any(x in fname for x in ("run_benchmarks", "visualize_metrics")):
            all_params = cfg.get("parameter_sets", [])
            if all_params:
                new_cell = _make_runs_cell(all_params)
                replaced = _find_and_replace_parameters_cell(ipynb_json, new_cell)
            else:
                replaced = False

        if replaced:
            modified_payload_bytes = json.dumps(ipynb_json, ensure_ascii=False).encode("utf-8")

        # base64 and upload (modified_payload_bytes or original ipynb_bytes)
        ipynb_b64 = base64.b64encode(modified_payload_bytes).decode("utf-8")
        upload_url = f"{API_BASE}/workspaces/{ws_id}/items"
        payload = {
            "displayName": display,
            "type": "Notebook",
            "description": desc,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": Path(file_path).name,
                        "payload": ipynb_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        upl = requests.post(upload_url, headers=headers, json=payload, timeout=UPLOAD_TIMEOUT)
        entry["response_code"] = upl.status_code
        entry["response_text"] = upl.text[:2000] if upl.text else ""
        if upl.status_code in (200, 201):
            entry["notebook_id"] = upl.json().get("id")
            entry["status"] = "created"
            results.append(entry)
            continue
        if upl.status_code == 202:
            nid = _poll_for_item(ws_id, display)
            if nid:
                entry["notebook_id"] = nid
                entry["status"] = "created_async"
            else:
                entry["status"] = "accepted_no_completion"
            results.append(entry)
            continue
        entry["status"] = "upload_failed"
        results.append(entry)

# write results
OUT_FILE.write_text(json.dumps(results, indent=2), encoding="utf-8")
print(f"Wrote notebook provisioning state -> {OUT_FILE}")
print(f"Processed {len(results)} notebook->workspace entries.")
sys.exit(0)
