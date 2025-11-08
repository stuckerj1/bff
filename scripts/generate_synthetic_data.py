#!/usr/bin/env python3
import os
import json
import sys
import time
import yaml
import requests

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
r = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=hdr, timeout=30)
if r.status_code != 200:
    print("Failed to list workspaces:", r.status_code, r.text[:1000], file=sys.stderr)
    sys.exit(6)

ws_list = r.json().get("value", [])
ws_id = next((w.get("id") for w in ws_list if w.get("displayName") == ws_name), None)
if not ws_id:
    print(f"Workspace '{ws_name}' not found in /workspaces", file=sys.stderr)
    sys.exit(7)

# derive artifact_id from notebook displayName
items_r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items", headers=hdr, timeout=30)
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

# put the full params JSON into configuration.conf so runtime sees spark.notebook.parameters
conf_params = {"spark.notebook.parameters": json.dumps(params, ensure_ascii=False)}

payload = {
    "executionData": {
        "parameters": exec_params,
        "configuration": {"conf": conf_params},
    }
}

# debug + POST
print("RunNotebook payload preview:", json.dumps(payload)[:1000], flush=True)
url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items/{artifact_id}/jobs/instances?jobType=RunNotebook"
resp = requests.post(url, headers=hdr, json=payload, timeout=120)
print("Run response status:", resp.status_code, flush=True)

loc = resp.headers.get("Location") or resp.headers.get("Operation-Location") or resp.headers.get("Azure-AsyncOperation")
if loc:
    print("Run response location/op:", loc, flush=True)
else:
    print(resp.text[:1000], flush=True)

# if the run was accepted async, poll the Location URL and print status each attempt
run_result = {"status_code": resp.status_code, "text": resp.text, "location": loc}
if resp.status_code == 202 and loc:
    print("Polling job instance at:", loc, flush=True)
    success = False
    for attempt in range(1, 61):
        time.sleep(5)
        jr = requests.get(loc, headers=hdr, timeout=30)
        code = jr.status_code
        txt = jr.text or ""
        try:
            j = jr.json()
        except Exception:
            j = None

        status_hint = None
        if isinstance(j, dict):
            status_hint = j.get("status") or j.get("state") or (j.get("job") or {}).get("status")

        print(f"poll {attempt:02d}: status_code={code} status_hint={status_hint} text_snippet={txt[:400]}", flush=True)
        run_result["polled_last"] = {"attempt": attempt, "status_code": code, "status_hint": status_hint, "text_snippet": txt[:1000]}

        if 200 <= code < 300 and (status_hint is None or str(status_hint).lower() in ("succeeded", "finished", "completed")):
            success = True
            break

    if not success:
        print("Timed out waiting for job instance to reach a terminal success state.", file=sys.stderr, flush=True)

# persist a tiny run result for downstream steps
os.makedirs(".state", exist_ok=True)
open(".state/notebook_run_result.json", "w", encoding="utf-8").write(json.dumps(run_result, indent=2))

# also write datasets.json so upload-artifact step has something
datasets = []
if isinstance(params, dict) and "DATASETS_PARAM" in params:
    datasets = [d.get("name") for d in params["DATASETS_PARAM"] if isinstance(d, dict) and d.get("name")]
else:
    maybe = params.get("datasets") or params.get("parameter_sets") or params.get("DATASETS")
    if isinstance(maybe, list):
        datasets = [d.get("name") for d in maybe if isinstance(d, dict) and d.get("name")]

open(".state/datasets.json", "w", encoding="utf-8").write(json.dumps({"datasets": datasets}, indent=2))

# exit success if initial call was 2xx or accepted (202); else non-zero
if 200 <= resp.status_code < 300 or resp.status_code == 202:
    sys.exit(0)
else:
    sys.exit(11)
