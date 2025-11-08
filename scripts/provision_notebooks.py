#!/usr/bin/env python3
"""
Notebook provisioning helper with upload support.

Behavior:
- Read a JSON file (--notebooks-file) listing notebooks to create.
  Each entry should have: displayName, description (opt), file, workspaces (list or string).
- Authenticate using client credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET).
- For each notebook->workspace:
  - find workspace id by displayName (via GET /workspaces)
  - base64-encode the .ipynb and POST to /workspaces/{workspace_id}/items
  - if the create returns 202, poll /workspaces/{workspace_id}/items until the notebook appears (best-effort)
- Write .state/notebooks_created.json with one entry per notebook->workspace mapping including status, workspace_id, notebook_id, and server response info.

Notes:
- This script performs network/API calls and requires environment variables:
    TENANT_ID, CLIENT_ID, CLIENT_SECRET
- The script is intentionally straightforward and focuses only on upload behavior.
"""
from __future__ import annotations
import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path

try:
    import requests
except Exception:
    print("Missing dependency: requests. Install via pip install requests", file=sys.stderr)
    sys.exit(1)

# constants
STATE_DIR = Path(".state")
STATE_DIR.mkdir(exist_ok=True)
OUT_FILE = STATE_DIR / "notebooks_created.json"

OAUTH_TIMEOUT = 30
API_BASE = "https://api.fabric.microsoft.com/v1"
UPLOAD_TIMEOUT = 60
POLL_SLEEP = 5
POLL_ATTEMPTS = 20

# parse args
p = argparse.ArgumentParser(description="Provision and upload notebooks to Fabric workspaces.")
p.add_argument("--notebooks-file", required=True, help="JSON file with notebooks to create")
args = p.parse_args()

nb_file = Path(args.notebooks_file)
if not nb_file.exists():
    print(f"Notebooks file not found: {nb_file}", file=sys.stderr)
    sys.exit(2)

# load notebooks list
try:
    notebooks = json.loads(nb_file.read_text(encoding="utf-8"))
except Exception as e:
    print(f"Failed to read/parse notebooks file {nb_file}: {e}", file=sys.stderr)
    sys.exit(3)

# read auth env
tenant = os.environ.get("TENANT_ID")
client = os.environ.get("CLIENT_ID")
secret = os.environ.get("CLIENT_SECRET")
if not (tenant and client and secret):
    print("TENANT_ID, CLIENT_ID, and CLIENT_SECRET environment variables are required.", file=sys.stderr)
    sys.exit(4)

# obtain AAD token via client credentials
token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
data = {
    "grant_type": "client_credentials",
    "client_id": client,
    "client_secret": secret,
    "scope": "https://api.fabric.microsoft.com/.default"
}
try:
    r = requests.post(token_url, data=data, timeout=OAUTH_TIMEOUT)
    if r.status_code != 200:
        print(f"Failed to obtain AAD token: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(5)
    token = r.json().get("access_token")
    if not token:
        print("AAD token response missing access_token", file=sys.stderr)
        sys.exit(6)
except Exception as e:
    print(f"Error obtaining AAD token: {e}", file=sys.stderr)
    sys.exit(7)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# fetch workspaces list once and build map displayName -> id
try:
    wr = requests.get(f"{API_BASE}/workspaces", headers=headers, timeout=30)
    wr.raise_for_status()
    workspace_list = wr.json().get("value", [])
    workspaces_by_name = {w.get("displayName"): w.get("id") for w in workspace_list if w.get("displayName")}
except Exception as e:
    print(f"Failed to list workspaces: {e}", file=sys.stderr)
    workspaces_by_name = {}

results = []

# helper inline: poll for item presence
def _poll_for_item(workspace_id: str, target_display: str) -> str | None:
    items_url = f"{API_BASE}/workspaces/{workspace_id}/items"
    for attempt in range(1, POLL_ATTEMPTS + 1):
        try:
            ir = requests.get(items_url, headers=headers, timeout=30)
            if ir.status_code == 200:
                vals = ir.json().get("value", [])
                for it in vals:
                    if it.get("displayName") == target_display and it.get("type") == "Notebook":
                        return it.get("id")
        except Exception:
            pass
        time.sleep(POLL_SLEEP)
    return None

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

    # read notebook bytes once (if present)
    ipynb_bytes = None
    if file_path:
        src = Path(file_path)
        if src.exists():
            try:
                ipynb_bytes = src.read_bytes()
            except Exception as e:
                # will record per-workspace failure below
                ipynb_bytes = None

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

        # build upload payload
        try:
            ipynb_b64 = base64.b64encode(ipynb_bytes).decode("utf-8")
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
                try:
                    entry["notebook_id"] = upl.json().get("id")
                except Exception:
                    entry["notebook_id"] = None
                entry["status"] = "created"
                results.append(entry)
                continue
            if upl.status_code == 202:
                # best-effort poll for the item to appear in /items
                nid = _poll_for_item(ws_id, display)
                if nid:
                    entry["notebook_id"] = nid
                    entry["status"] = "created_async"
                else:
                    entry["status"] = "accepted_no_completion"
                results.append(entry)
                continue
            # other error
            entry["status"] = "upload_failed"
            results.append(entry)
        except Exception as e:
            entry["status"] = "exception_during_upload"
            entry["response_text"] = str(e)
            results.append(entry)

# write results
try:
    OUT_FILE.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Wrote notebook provisioning state -> {OUT_FILE}")
    print(f"Processed {len(results)} notebook->workspace entries.")
except Exception as e:
    print(f"Failed to write state file {OUT_FILE}: {e}", file=sys.stderr)
    sys.exit(8)

sys.exit(0)
