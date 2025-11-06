#!/usr/bin/env python3
"""
Provision Metrics / Benchmark lakehouses and warehouses.

Minimal, deterministic implementation intended to restore the previous working
behavior with as little change as possible.

Behavior:
- Expects the merged workspaces_summary.json to be downloaded by the workflow
  into ./artifacts/bff-workspaces-summary/workspaces_summary.json (what
  actions/download-artifact writes when the artifact name is
  `bff-workspaces-summary` and path is `./artifacts`).
- Uses client-credentials auth ONLY (TENANT_ID/CLIENT_ID/CLIENT_SECRET required).
- Creates a MetricsLakehouse in the controller workspace.
- For each action workspace creates:
    - BenchmarkLakehouse-<workspace_name_or_sanitized_name>
    - BenchmarkWarehouse-<workspace_name_or_sanitized_name>
- Writes one JSON file per created resource into the output directory (default .state).
- Fails loudly on any unexpected condition.
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
    """
    Strict client-credentials flow only. This intentionally does NOT consult
    alternative token env vars to avoid extra branches.
    """
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
    # Keep filename logic simple and predictable (replace path separators and spaces)
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

def find_summary_path() -> str:
    """
    Deterministic lookup: expect the workflow to have placed the artifact at:
      ./artifacts/bff-workspaces-summary/workspaces_summary.json

    If that exact path is not present, fail with a clear message explaining the expected location.
    """
    expected = "./artifacts/bff-workspaces-summary/workspaces_summary.json"
    if os.path.exists(expected):
        print(f"Using summary file: {expected}")
        return expected
    # also accept a direct ./artifacts/workspaces_summary.json if your workflow downloaded differently
    alt = "./artifacts/workspaces_summary.json"
    if os.path.exists(alt):
        print(f"Using summary file: {alt}")
        return alt
    die(
        "workspaces_summary.json not found at the expected artifact path.\n"
        "Expected: ./artifacts/bff-workspaces-summary/workspaces_summary.json (when using download-artifact with name: bff-workspaces-summary and path: ./artifacts)\n"
        "Or: ./artifacts/workspaces_summary.json if your download put files directly under ./artifacts.\n"
        "Please ensure the assemble-workspaces job uploaded the artifact using exactly:\n"
        "  uses: actions/upload-artifact@v4\n"
        "  with:\n"
        "    name: bff-workspaces-summary\n"
        "    path: path/to/workspaces_summary.json\n"
        "and the provisioning job downloads it with:\n"
        "  uses: actions/download-artifact@v4\n"
        "  with:\n"
        "    name: bff-workspaces-summary\n"
        "    path: ./artifacts\n"
    )

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--summary-path", default=None, help="Direct path to workspaces_summary.json (overrides expected artifact location)")
    p.add_argument("--output-dir", default=".state", help="Directory to write per-resource JSON files")
    p.add_argument("--capacity-id", default=None, help="Optional capacity id to include in resource create payloads (falls back to CAPACITY_ID env)")
    p.add_argument("--poll-interval", type=int, default=5, help="Seconds between warehouse availability polls")
    p.add_argument("--poll-attempts", type=int, default=12, help="Number of polls for warehouse creation")
    args = p.parse_args(argv)

    summary_path = args.summary_path or find_summary_path()
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
        lh_name = f"BenchmarkLakehouse-{sname}"
        print(f"Creating {lh_name} in {wid}")
        resp_lh = create_lakehouse(session, token, wid, lh_name, capacity_id=capacity_id)
        write_state(args.output_dir, "lakehouse", lh_name, wid, resp_lh)

        wh_name = f"BenchmarkWarehouse-{sname}"
        print(f"Creating {wh_name} in {wid}")
        resp_wh = create_warehouse(session, token, wid, wh_name, capacity_id=capacity_id, poll_interval=args.poll_interval, poll_attempts=args.poll_attempts)
        write_state(args.output_dir, "warehouse", wh_name, wid, resp_wh)

    print("All resources created successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
