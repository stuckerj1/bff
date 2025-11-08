#!/usr/bin/env python3
import os
import json
import sys
import time
import yaml
import requests

API_BASE = "https://api.fabric.microsoft.com/v1"

# load params and auth from env/secrets
params = yaml.safe_load(open("config/test_parameter_sets.yml", "r", encoding="utf-8")) or {}
tenant = os.environ["TENANT_ID"]
client = os.environ["CLIENT_ID"]
secret = os.environ["CLIENT_SECRET"]

# STANDARD NAMING FOR WORKSPACE AND NOTEBOOK
ws_name = "BFF Controller"
nb_display = "0.GenerateData"

# obtain token and headers
token_resp = requests.post(
    f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
    data={
        "grant_type": "client_credentials",
        "client_id": client,
        "client_secret": secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    },
    timeout=30,
)
tok = token_resp.json().get("access_token")
if not tok:
    print("Failed to obtain AAD token", file=sys.stderr)
    sys.exit(5)

hdr = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

# derive workspace_id from workspace display name
r = requests.get(f"{API_BASE}/workspaces", headers=hdr, timeout=30)
if r.status_code != 200:
    print("Failed to list workspaces:", r.status_code, r.text[:1000], file=sys.stderr)
    sys.exit(6)

ws_list = r.json().get("value", [])
ws_id = next((w.get("id") for w in ws_list if w.get("displayName") == ws_name), None)
if not ws_id:
    print(f"Workspace '{ws_name}' not found in /workspaces", file=sys.stderr)
    sys.exit(7)

# derive artifact_id from notebook displayName
items_r = requests.get(f"{API_BASE}/workspaces/{ws_id}/items", headers=hdr, timeout=30)
if items_r.status_code != 200:
    print(f"Failed to list items for workspace {ws_id}: {items_r.status_code} {items_r.text[:1000]}", file=sys.stderr)
    sys.exit(8)

items = items_r.json().get("value", [])
artifact_id = next(
    (it.get("id") for it in items if it.get("displayName") == nb_display and it.get("type") == "Notebook"),
    None,
)
if not artifact_id:
    print(f"Notebook '{nb_display}' not found in workspace '{ws_name}' items", file=sys.stderr)
    sys.exit(9)

# build execution parameters
exec_params = {}
if isinstance(params, dict):
    for k, v in params.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            value_str = "" if v is None else str(v)
        else:
            value_str = json.dumps(v, ensure_ascii=False)
        exec_params[str(k)] = {"value": value_str, "type": "string"}

# Ensure DATASETS_PARAM execution parameter exists (notebook expects this)
datasets_value = params.get("DATASETS_PARAM") or params.get("datasets") or params.get("DATASETS") or []
exec_params["DATASETS_PARAM"] = {"value": json.dumps(datasets_value, ensure_ascii=False), "type": "string"}

# Build configuration.conf payload so runtime sees spark.notebook.parameters exactly like the notebook's %%configure cell
conf_payload = {}
conf_payload["DATASETS_PARAM"] = datasets_value

# copy common keys if present
for key in ("PUSH_TO_AZURE_SQL", "AZURE_SQL_SERVER", "AZURE_SQL_DB", "AZURE_SQL_SCHEMA", "distribution", "seed"):
    if key in params:
        conf_payload[key] = params[key]

# preserve other top-level keys
for k, v in params.items():
    if k not in conf_payload:
        conf_payload[k] = v

# default lakehouse name (match notebook cell)
lakehouse_name = params.get("defaultLakehouse", {}).get("name") if isinstance(params.get("defaultLakehouse"), dict) else params.get("defaultLakehouse")
if not lakehouse_name:
    lakehouse_name = "DataSourceLakehouse"

# Resolve lakehouse id (so runtime definitely targets the intended lakehouse)
lakehouse_id = None
lh_r = requests.get(f"{API_BASE}/workspaces/{ws_id}/lakehouses", headers=hdr, timeout=30)
if lh_r.status_code == 200:
    for lh in lh_r.json().get("value", []):
        if lh.get("displayName") == lakehouse_name:
            lakehouse_id = lh.get("id")
            break

# Resolve environment id if the params specify an environment name (optional)
env_id = None
env_name = None
# Accept environment config from params (common shapes)
if isinstance(params.get("configuration"), dict) and isinstance(params["configuration"].get("environment"), dict):
    env_name = params["configuration"]["environment"].get("name") or params["configuration"]["environment"].get("displayName")
elif isinstance(params.get("environment"), dict):
    env_name = params["environment"].get("name")
elif params.get("environment"):
    env_name = params.get("environment")
if env_name:
    env_r = requests.get(f"{API_BASE}/workspaces/{ws_id}/environments", headers=hdr, timeout=30)
    if env_r.status_code == 200:
        for e in env_r.json().get("value", []):
            if e.get("displayName") == env_name or e.get("name") == env_name:
                env_id = e.get("id")
                break

# Build full configuration including defaultLakehouse id/workspaceId and environment if found
conf_configuration = {
    "conf": {"spark.notebook.parameters": json.dumps(conf_payload, ensure_ascii=False)},
    "defaultLakehouse": {"name": lakehouse_name}
}
if lakehouse_id:
    conf_configuration["defaultLakehouse"]["id"] = lakehouse_id
    conf_configuration["defaultLakehouse"]["workspaceId"] = ws_id

if env_id or env_name:
    conf_configuration["environment"] = {}
    if env_id:
        conf_configuration["environment"]["id"] = env_id
    if env_name:
        conf_configuration["environment"]["name"] = env_name

# POST RunNotebook
payload = {
    "executionData": {
        "parameters": exec_params,
        "configuration": conf_configuration,
    }
}

print("RunNotebook payload preview:", json.dumps(payload)[:1200], flush=True)
url = f"{API_BASE}/workspaces/{ws_id}/items/{artifact_id}/jobs/instances?jobType=RunNotebook"
resp = requests.post(url, headers=hdr, json=payload, timeout=120)
print("Run response status:", resp.status_code, flush=True)

loc = resp.headers.get("Location") or resp.headers.get("Operation-Location") or resp.headers.get("Azure-AsyncOperation")
if loc:
    print("Run response location/op:", loc, flush=True)
else:
    print(resp.text[:1000], flush=True)

# poll instance until terminal
instance_json = None
if resp.status_code == 202 and loc:
    print("Polling job instance at:", loc, flush=True)
    for attempt in range(1, 61):
        time.sleep(5)
        jr = requests.get(loc, headers=hdr, timeout=30)
        try:
            j = jr.json()
        except Exception:
            j = None
        status_hint = None
        if isinstance(j, dict):
            instance_json = j
            status_hint = j.get("status") or j.get("state") or (j.get("job") or {}).get("status")
        print(f"poll {attempt:02d}: code={jr.status_code} status_hint={status_hint}", flush=True)
        if isinstance(status_hint, str) and status_hint.lower() == "failed":
            print("Job failed; stopping polls and capturing failureReason.", flush=True)
            break
        if 200 <= jr.status_code < 300 and (status_hint is None or str(status_hint).lower() in ("succeeded", "finished", "completed", "completedwithwarnings")):
            print("Job reached a terminal success state.", flush=True)
            break

# persist run result
os.makedirs(".state", exist_ok=True)
run_result = {"status_code": resp.status_code, "location": loc, "instance": instance_json}
open(".state/notebook_run_result.json", "w", encoding="utf-8").write(json.dumps(run_result, indent=2))

# Try to fetch activities/outputs (best-effort)
if loc:
    try:
        inst_activities = requests.get(f"{loc}/activities", headers=hdr, timeout=30)
        open(".state/instance_activities.txt", "w", encoding="utf-8").write(inst_activities.text or "")
    except Exception:
        pass
    try:
        inst_outputs = requests.get(f"{loc}/outputs", headers=hdr, timeout=30)
        open(".state/instance_outputs.txt", "w", encoding="utf-8").write(inst_outputs.text or "")
    except Exception:
        pass

# also write datasets.json so upload-artifact step has something
datasets = []
if isinstance(params, dict) and "DATASETS_PARAM" in params:
    datasets = [d.get("name") for d in params["DATASETS_PARAM"] if isinstance(d, dict) and d.get("name")]
else:
    maybe = params.get("datasets") or params.get("parameter_sets") or params.get("DATASETS")
    if isinstance(maybe, list):
        datasets = [d.get("name") for d in maybe if isinstance(d, dict) and d.get("name")]
open(".state/datasets.json", "w", encoding="utf-8").write(json.dumps({"datasets": datasets}, indent=2))

# --- BEGIN: JOB INSTANCE STATUS DEBUG (inserted) ---
# This block fetches the full job instance JSON using the same token and prints a concise summary.
# It mirrors the get_job_instance_status.py logic you referenced and helps surface failureReason,
# activities, or other runtime diagnostics when the instance endpoint is accessible.
if loc:
    try:
        # Use Accept: application/json to encourage JSON response
        debug_headers = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
        # GET the instance resource (workspace/item/instance)
        r = requests.get(loc, headers=debug_headers, timeout=30)
        print("HTTP", r.status_code)
        try:
            j = r.json()
            print(json.dumps(j, indent=2)[:20000])
        except Exception:
            print("Non-JSON response:", r.text[:4000])

        # concise summary
        try:
            status = j.get("status") or j.get("state") or (j.get("job") or {}).get("status")
            failure = j.get("failureReason") or (j.get("job") or {}).get("failureReason")
            print("\nSummary:")
            print(" status:", status)
            if failure:
                print(" failureReason:", json.dumps(failure)[:4000])
        except Exception:
            pass
    except Exception as e:
        print("Failed to GET instance URL for debug:", e, file=sys.stderr)
# --- END: JOB INSTANCE STATUS DEBUG (inserted) ---

# exit non-zero only when the API call failed (non-2xx) or instance explicitly failed
if instance_json and isinstance(instance_json, dict) and instance_json.get("status") and instance_json.get("status").lower() == "failed":
    sys.exit(12)
if 200 <= resp.status_code < 300 or resp.status_code == 202:
    sys.exit(0)
else:
    sys.exit(11)
