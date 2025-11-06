#!/usr/bin/env python3
"""
Provision Metrics / Benchmark lakehouses and warehouses.

Minimal deterministic behavior aligned to your updated artifact location:
- Expects merged summary at .state/bff-workspaces-summary.json by default.
- Uses client-credentials auth only (TENANT_ID/CLIENT_ID/CLIENT_SECRET).
- Creates MetricsLakehouse in controller and BenchmarkLakehouse / BenchmarkWarehouse
  for each workspace listed in the summary.
- Writes per-resource JSON files into the output directory (default: .state).
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import datetime
from typing import Optional
import requests

API_BASE = "https://api.fabric.microsoft.com/v1"

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def get_token_via_client_credentials() -> str:
    tenant = os.environ.get("TENANT_ID")
    client = os.environ.get("CLIENT_ID")
    secret = os.environ.get("CLIENT_SECRET")
    if not (tenant and client and secret):
        die("Missing authentication: set TENANT_ID, CLIENT_ID, and CLIENT_SECRET in the environment")
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client,
        "client_secret": secret,
        "scope": "https://api.fabric.microsoft.com/.default"
    }
    r = requests.post(token_url, data=data, timeout=30)
    if r.status_code != 200:
        die(f"Failed to obtain AAD token: {r.status_code} {r.text}")
    return r.json().get("access_token")

def write_state(output_dir: str, resource_kind: str, display_name: str, workspace_id: str, resp: dict):
    os.makedirs(output_dir, exist_ok=True)
    # simple, predictable filename: replace path separators and spaces
    fname_comp = str(display_name).replace(os.path.sep, "_").replace(" ", "-")
    fname = os.path.join(output_dir, f"{resource_kind}-{fname_comp}.json")
    out = {
        "workspace_id": workspace_id,
        "resource_type": resource_kind,
        "displayName": display_name,
        "timestamp_utc": now_iso(),
        "api_response": resp
    }
    with open(fname, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    print(f"Wrote {fname}")

def create_lakehouse(session: requests.Session, token: str, workspace_id: str, display_name: str, capacity_id: Optional[str] = None) -> dict:
    url = f"{API_BASE}/workspaces/{workspace_id}/lakehouses"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"displayName": display_name}
    if capacity_id:
        payload["capacityId"] = capacity_id
    print(f"POST {url} -> {display_name}")
    r = session.post(url, headers=headers, json=payload, timeout=60)
    if not (200 <= r.status_code < 300):
        die(f"Create lakehouse '{display_name}' failed: {r.status_code} {r.text}")
    try:
        return r.json()
    except Exception:
        return {"raw_text": r.text}

def create_warehouse(session: requests.Session, token: str, workspace_id: str, display_name: str, capacity_id: Optional[str] = None, poll_interval: int = 5, poll_attempts: int = 12) -> dict:
    url = f"{API_BASE}/workspaces/{workspace_id}/warehouses"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"displayName": display_name}
    if capacity_id:
        payload["capacityId"] = capacity_id
    print(f"POST {url} -> {display_name}")
    r = session.post(url, headers=headers, json=payload, timeout=60)
    # Accept 201 (created) or 202 (accepted async). Fail on other non-2xx.
    if 200 <= r.status_code < 300:
        try:
            return r.json()
        except Exception:
            return {"raw_text": r.text}
    if r.status_code == 202:
        # Poll until the warehouse appears in GET /workspaces/{workspace_id}/warehouses
        print(f"Warehouse creation accepted (202). Polling for availability (every {poll_interval}s, up to {poll_attempts} attempts)...")
        poll_url = f"{API_BASE}/workspaces/{workspace_id}/warehouses"
        for attempt in range(1, poll_attempts + 1):
            time.sleep(poll_interval)
            pr = session.get(poll_url, headers=headers, timeout=30)
            if pr.status_code == 200:
                try:
                    val = pr.json().get("value", [])
                except Exception:
                    val = []
                for wh in val:
                    if wh.get("displayName") == display_name:
                        print(f"Found warehouse {display_name} after {attempt} polls.")
                        return wh
            print(f"Poll {attempt}/{poll_attempts}: {display_name} not available yet.")
        die(f"Warehouse {display_name} not found after polling (workspace {workspace_id}).")
    # otherwise error
    die(f"Create warehouse '{display_name}' failed: {r.status_code} {r.text}")

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--summary-path", default=".state/bff-workspaces-summary.json", help="Path to merged workspaces summary JSON (default: .state/bff-workspaces-summary.json)")
    p.add_argument("--output-dir", default=".state", help="Directory to write per-resource JSON files")
    p.add_argument("--capacity-id", default=None, help="Optional capacity id to include in resource create payloads (falls back to CAPACITY_ID env)")
    p.add_argument("--poll-interval", type=int, default=5, help="Seconds between warehouse availability polls")
    p.add_argument("--poll-attempts", type=int, default=12, help="Number of polls for warehouse creation")
    args = p.parse_args(argv)

    summary_path = args.summary_path
    if not os.path.exists(summary_path):
        die(f"workspaces summary not found at expected path: {summary_path}\nEnsure the assemble job wrote the merged summary to this path and that the provisioning job downloads/places it there (example: .state/bff-workspaces-summary.json)")

    summary = json.load(open(summary_path, "r", encoding="utf-8"))
    controller = summary.get("controller") or {}
    workspaces = summary.get("workspaces", [])

    token = get_token_via_client_credentials()
    session = requests.Session()
    capacity_id = args.capacity_id or os.environ.get("CAPACITY_ID")

    # Create MetricsLakehouse in controller
    ctrl_id = controller.get("workspace_id") or (controller.get("api_response") or {}).get("id")
    if not ctrl_id:
        die(f"controller.workspace_id missing in {summary_path}. Controller object: {controller}")
    print(f"Creating MetricsLakehouse in controller workspace {ctrl_id}")
    resp_metrics = create_lakehouse(session, token, ctrl_id, "MetricsLakehouse", capacity_id=capacity_id)
    write_state(args.output_dir, "lakehouse-metrics", "MetricsLakehouse", ctrl_id, resp_metrics)

    # For each action workspace create BenchmarkLakehouse and BenchmarkWarehouse
    for ws in workspaces:
        wid = ws.get("workspace_id") or (ws.get("api_response") or {}).get("id")
        if not wid:
            print("Skipping workspace with no workspace_id:", ws.get("workspace_name"))
            continue
        # Use provided sanitized_name if present, else use workspace_name as-is
        sname = ws.get("sanitized_name") or ws.get("workspace_name") or wid
        lh_name = "BenchmarkLakehouse"
        print(f"Creating {lh_name} in {wid}")
        resp_lh = create_lakehouse(session, token, wid, lh_name, capacity_id=capacity_id)
        write_state(args.output_dir, "lakehouse", lh_name, wid, resp_lh)

        wh_name = "BenchmarkWarehouse"
        print(f"Creating {wh_name} in {wid}")
        resp_wh = create_warehouse(session, token, wid, wh_name, capacity_id=capacity_id, poll_interval=args.poll_interval, poll_attempts=args.poll_attempts)
        write_state(args.output_dir, "warehouse", wh_name, wid, resp_wh)

    print("All resources created successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

