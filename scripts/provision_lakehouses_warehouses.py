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

def write_state(output_dir: str, resource_kind: str, workspace_name, sanitized_name: str, workspace_id: str, resp: dict):
    os.makedirs(output_dir, exist_ok=True)

    fname = os.path.join(output_dir, f"{resource_kind}-{sanitized_name}.json")
    out = {
        "workspace_id": workspace_id,
        "resource_type": resource_kind,
        "workspace_name": workspace_name,
        "sanitized_name": sanitized_name,
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

def create_warehouse(session: requests.Session, token: str, workspace_id: str, display_name: str, capacity_id: Optional[str] = None, poll_interval: int = 5, poll_attempts: int = 60) -> dict:
    """
    Create a warehouse. On 201 return the parsed JSON. On 202 return a dict with status and headers,
    and attempt to poll for completion using any Location/Azure-AsyncOperation header or by listing warehouses.
    This function always returns a dict (never None) and logs responses for debugging.
    """
    url = f"{API_BASE}/workspaces/{workspace_id}/warehouses"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"displayName": display_name}
    if capacity_id:
        payload["capacityId"] = capacity_id

    print(f"POST {url} -> {display_name}")
    r = session.post(url, headers=headers, json=payload, timeout=60)

    # Prepare a concise body and headers preview
    body_preview = (r.text[:1000] + "...") if r.text and len(r.text) > 1000 else (r.text or "")
    hdrs = {k: v for k, v in r.headers.items()}
    print(f"  Response {r.status_code}; headers: { {k:v for k,v in list(hdrs.items())[:5]} } body: {body_preview!r}")

    # For normal 2xx responses try to parse JSON; always return a dict
    if 200 <= r.status_code < 300:
        try:
            parsed = r.json()
        except Exception:
            parsed = None
        if parsed is None:
            return {"raw_text": r.text or "", "status": r.status_code, "headers": hdrs}
        return parsed

    # If accepted async, attempt to follow operation headers or poll list
    if r.status_code == 202:
        resp_info = {"raw_text": r.text or "", "status": 202, "headers": hdrs}
        # Check common async operation headers
        op_url = hdrs.get("Location") or hdrs.get("Operation-Location") or hdrs.get("Azure-AsyncOperation")
        if op_url:
            print(f"  Async operation URL provided: {op_url}")
            # Poll operation URL (best-effort)
            for attempt in range(1, poll_attempts + 1):
                time.sleep(poll_interval)
                try:
                    pr = session.get(op_url, headers=headers, timeout=30)
                    print(f"  Poll op {attempt}: GET {op_url} -> {pr.status_code}")
                    if 200 <= pr.status_code < 300:
                        try:
                            parsed = pr.json()
                        except Exception:
                            parsed = {"raw_text": pr.text}
                        # If operation indicates resource is ready and includes an id or resource, return it
                        if isinstance(parsed, dict) and (parsed.get("status") in ("Succeeded", "succeeded") or parsed.get("id") or parsed.get("resource")):
                            print(f"  Operation reports completion: {parsed}")
                            return {"operation_result": parsed, "operation_url": op_url, "status": pr.status_code}
                except Exception as e:
                    print(f"  Poll op {attempt} failed: {e}")
            print(f"  Async operation at {op_url} did not indicate completion after polling.")
            # Fall through to polling the workspace list below

        # Poll the workspace warehouses list looking for the created displayName
        poll_url = f"{API_BASE}/workspaces/{workspace_id}/warehouses"
        print(f"Warehouse creation accepted (202). Polling workspace warehouses for {display_name} (every {poll_interval}s, up to {poll_attempts} attempts)...")
        for attempt in range(1, poll_attempts + 1):
            time.sleep(poll_interval)
            try:
                pr = session.get(poll_url, headers=headers, timeout=30)
                print(f"  Poll {attempt}: GET {poll_url} -> {pr.status_code}")
                if pr.status_code == 200:
                    try:
                        val = pr.json().get("value", [])
                    except Exception:
                        val = []
                    for wh in val:
                        if wh.get("displayName") == display_name:
                            print(f"Found warehouse {display_name} after {attempt} polls.")
                            return wh
            except Exception as e:
                print(f"  Poll {attempt} failed: {e}")
            print(f"Poll {attempt}/{poll_attempts}: {display_name} not available yet.")
        # If polling did not find it, return the original 202 info so the .state file reflects the async acceptance
        print(f"Warehouse {display_name} not found after polling (workspace {workspace_id}). Returning 202 info.")
        return resp_info

    # Otherwise return an error via die as before (or return structured error)
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
    write_state(args.output_dir, "lakehouse-metrics", controller.get("workspace_name"), controller.get("sanitized_name"), ctrl_id, resp_metrics)

    # For each action workspace create BenchmarkLakehouse and BenchmarkWarehouse
    for ws in workspaces:
        wid = ws.get("workspace_id") or (ws.get("api_response") or {}).get("id")
        if not wid:
            print("Skipping workspace with no workspace_id:", ws.get("workspace_name"))
            continue
        # Use sanitized_name (required). Let KeyError surface if it's missing.
        sname = ws["sanitized_name"]
        workspace_name = ws.get("workspace_name")

        lh_name = "BenchmarkLakehouse"
        print(f"Creating {lh_name} in {wid}")
        resp_lh = create_lakehouse(session, token, wid, lh_name, capacity_id=capacity_id)
        write_state(args.output_dir, "lakehouse", workspace_name, sname, wid, resp_lh)

        wh_name = "BenchmarkWarehouse"
        print(f"Creating {wh_name} in {wid}")
        resp_wh = create_warehouse(session, token, wid, wh_name, capacity_id=capacity_id, poll_interval=args.poll_interval, poll_attempts=args.poll_attempts)
        write_state(args.output_dir, "warehouse", workspace_name, sname, wid, resp_wh)

    print("All resources created successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())





