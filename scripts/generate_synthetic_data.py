#!/usr/bin/env python3
import os, json, sys
import yaml, requests

# load params and auth from env/secrets
params = yaml.safe_load(open("config/test_parameter_sets.yml", "r", encoding="utf-8")) or {}
tenant = os.environ["TENANT_ID"]
client = os.environ["CLIENT_ID"]
secret = os.environ["CLIENT_SECRET"]

# STANDARD NAMING FOR WORKSPACE AND NOTEBOOK
ws_name = "BFF Controller"
nb_display = "0.GenerateData"

# derive workspace_id from workspace display name "BFF Controller"
tok = requests.post(f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
    data={"grant_type":"client_credentials","client_id":client,"client_secret":secret,"scope":"https://api.fabric.microsoft.com/.default"}).json().get("access_token")
if not tok:
    print("Failed to obtain AAD token", file=sys.stderr); sys.exit(5)
hdr = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

r = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=hdr, timeout=30)
if r.status_code != 200:
    print("Failed to list workspaces:", r.status_code, r.text[:1000], file=sys.stderr); sys.exit(6)
ws_list = r.json().get("value", [])
ws_id = next((w.get("id") for w in ws_list if w.get("displayName") == ws_name), None)
if not ws_id:
    print(f"Workspace '{ws_name}' not found in /workspaces", file=sys.stderr); sys.exit(7)

# derive artifact_id from notebook displayName "0.GenerateData"
items_r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items", headers=hdr, timeout=30)
if items_r.status_code != 200:
    print(f"Failed to list items for workspace {ws_id}: {items_r.status_code} {items_r.text[:1000]}", file=sys.stderr); sys.exit(8)
items = items_r.json().get("value", [])
artifact_id = next((it.get("id") for it in items if it.get("displayName") == nb_display and it.get("type") == "Notebook"), None)
if not artifact_id:
    print(f"Notebook '{nb_display}' not found in workspace '{ws_name}' items", file=sys.stderr); sys.exit(9)

# build execution parameters (primitives as strings, complex encoded as JSON)
exec_params = {}
if isinstance(params, dict):
  for k,v in params.items():
    exec_params[str(k)] = {"value": (json.dumps(v, ensure_ascii=False) if not isinstance(v, (str,int,float,bool)) and v is not None else ("" if v is None else str(v))), "type":"string"}
exec_params["spark.notebook.parameters"] = {"value": json.dumps(params, ensure_ascii=False), "type":"string"}

payload = {"executionData": {"parameters": exec_params, "configuration": {}}}

# debug + POST
print("RunNotebook payload preview:", json.dumps(payload)[:1000], flush=True)
url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items/{artifact_id}/jobs/instances?jobType=RunNotebook"
resp = requests.post(url, headers=hdr, json=payload, timeout=120)
print("Run response status:", resp.status_code, flush=True)
print(resp.headers.get("Location") or resp.headers.get("Operation-Location") or resp.headers.get("Azure-AsyncOperation") or resp.text[:1000], flush=True)

# persist a tiny run result for downstream steps
os.makedirs(".state", exist_ok=True)
open(".state/notebook_run_result.json", "w", encoding="utf-8").write(json.dumps({"status_code": resp.status_code, "text": resp.text, "location": resp.headers.get("Location")}, indent=2))
sys.exit(0 if 200 <= resp.status_code < 300 or resp.status_code == 202 else 11)
