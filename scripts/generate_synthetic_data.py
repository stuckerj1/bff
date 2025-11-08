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
ws_list = r.json().get("value", [])
ws_id = next((w.get("id") for w in ws_list if w.get("displayName") == ws_name), None)
if not ws_id:
    print(f"Workspace '{ws_name}' not found", file=sys.stderr)
    sys.exit(6)

# derive artifact_id from notebook displayName
items_r = requests.get(f"{API_BASE}/workspaces/{ws_id}/items", headers=hdr, timeout=30)
items = items_r.json().get("value", [])
artifact_id = next((it.get("id") for it in items if it.get("displayName") == nb_display and it.get("type") == "Notebook"), None)
if not artifact_id:
    print(f"Notebook '{nb_display}' not found", file=sys.stderr)
    sys.exit(7)

# build execution parameters (keep DATASETS_PARAM etc.)
exec_params = {}
if isinstance(params, dict):
    for k, v in params.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            value_str = "" if v is None else str(v)
        else:
            value_str = json.dumps(v, ensure_ascii=False)
        exec_params[str(k)] = {"value": value_str, "type": "string"}

datasets_value = params.get("DATASETS_PARAM") or params.get("datasets") or params.get("DATASETS") or []
exec_params["DATASETS_PARAM"] = {"value": json.dumps(datasets_value, ensure_ascii=False), "type": "string"}

# conf payload so runtime sees spark.notebook.parameters
conf_payload = {}
conf_payload["DATASETS_PARAM"] = datasets_value
for key in ("PUSH_TO_AZURE_SQL", "AZURE_SQL_SERVER", "AZURE_SQL_DB", "AZURE_SQL_SCHEMA", "distribution", "seed"):
    if key in params:
        conf_payload[key] = params[key]
for k, v in params.items():
    if k not in conf_payload:
        conf_payload[k] = v

lakehouse_name = params.get("defaultLakehouse", {}).get("name") if isinstance(params.get("defaultLakehouse"), dict) else params.get("defaultLakehouse")
if not lakehouse_name:
    lakehouse_name = "DataSourceLakehouse"
conf_configuration = {
    "conf": {"spark.notebook.parameters": json.dumps(conf_payload, ensure_ascii=False)},
    "defaultLakehouse": {"name": lakehouse_name},
}

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
print("Run response location/op:", loc or resp.text[:1000], flush=True)

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

# --- New debug: attempt to fetch activities and outputs for the instance (best-effort)
if loc:
    try:
        inst_activities = requests.get(f"{loc}/activities", headers=hdr, timeout=30)
        print("/activities status:", inst_activities.status_code, flush=True)
        try:
            ajson = inst_activities.json()
            print("activities keys/snippet:", list(ajson.keys()) if isinstance(ajson, dict) else type(ajson), flush=True)
            open(".state/instance_activities.json", "w", encoding="utf-8").write(json.dumps(ajson, indent=2))
        except Exception:
            print("activities text snippet:", inst_activities.text[:2000], flush=True)
            open(".state/instance_activities.txt", "w", encoding="utf-8").write(inst_activities.text)
    except Exception as e:
        print("Failed to GET instance /activities:", e, flush=True)

    try:
        inst_outputs = requests.get(f"{loc}/outputs", headers=hdr, timeout=30)
        print("/outputs status:", inst_outputs.status_code, flush=True)
        try:
            ojson = inst_outputs.json()
            print("outputs keys/snippet:", list(ojson.keys()) if isinstance(ojson, dict) else type(ojson), flush=True)
            open(".state/instance_outputs.json", "w", encoding="utf-8").write(json.dumps(ojson, indent=2))
        except Exception:
            print("outputs text snippet:", inst_outputs.text[:2000], flush=True)
            open(".state/instance_outputs.txt", "w", encoding="utf-8").write(inst_outputs.text)
    except Exception as e:
        print("Failed to GET instance /outputs:", e, flush=True)

    # also try item-scoped activities path as an alternative shape
    try:
        instance_id = loc.rstrip("/").split("/")[-1]
        alt_url = f"{API_BASE}/workspaces/{ws_id}/items/{artifact_id}/jobs/instances/{instance_id}/activities"
        alt = requests.get(alt_url, headers=hdr, timeout=30)
        print("item-scoped activities status:", alt.status_code, flush=True)
        try:
            alt_json = alt.json()
            print("item-scoped activities keys/snippet:", list(alt_json.keys()) if isinstance(alt_json, dict) else type(alt_json), flush=True)
            open(".state/alt_instance_activities.json", "w", encoding="utf-8").write(json.dumps(alt_json, indent=2))
        except Exception:
            print("alt activities text snippet:", alt.text[:2000], flush=True)
            open(".state/alt_instance_activities.txt", "w", encoding="utf-8").write(alt.text)
    except Exception as e:
        print("Failed to GET item-scoped activities:", e, flush=True)

print("Wrote .state/notebook_run_result.json and any instance activity/output artifacts (if available).", flush=True)

# also write datasets.json for upload step
datasets = []
if isinstance(params, dict) and "DATASETS_PARAM" in params:
    datasets = [d.get("name") for d in params["DATASETS_PARAM"] if isinstance(d, dict) and d.get("name")]
else:
    maybe = params.get("datasets") or params.get("parameter_sets") or params.get("DATASETS")
    if isinstance(maybe, list):
        datasets = [d.get("name") for d in maybe if isinstance(d, dict) and d.get("name")]
open(".state/datasets.json", "w", encoding="utf-8").write(json.dumps({"datasets": datasets}, indent=2))

if instance_json and isinstance(instance_json, dict) and instance_json.get("status") and instance_json.get("status").lower() == "failed":
    sys.exit(12)
if 200 <= resp.status_code < 300 or resp.status_code == 202:
    sys.exit(0)
else:
    sys.exit(11)
