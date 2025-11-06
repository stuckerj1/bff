#!/usr/bin/env python3
"""
Provision a Fabric workspace and assign an admin.

This is a small, robust script based on the simple working pattern from commit cbe4bfa...
It accepts the parameters used by .github/workflows/provision_workspaces.yml and writes
a JSON summary to the --output path for downstream steps/artifacts.

Behavior:
- Fails loudly on any API error (no synthetic IDs or hidden fallbacks).
- Creates a workspace with the provided --workspace-name (displayName).
- Optionally assigns an admin principal (ADMIN_OBJECT_ID env or --admin-object-id).
- Retries admin assignment a few times with backoff and fails the script if assignment fails.
- Writes the result JSON to the given --output path.

Fields written to the output JSON:
- workspace_name, sanitized_name, dataset_name, row_count, source, format, update_strategy
- timestamp_utc, workspace_id, created (bool), admin_assignment (status/details), api_response (raw)
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import sys
import time
from typing import Optional
import requests

API_BASE = "https://api.fabric.microsoft.com/v1"

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)

def get_token_from_env() -> str:
    tenant = os.environ.get("TENANT_ID")
    client = os.environ.get("CLIENT_ID")
    secret = os.environ.get("CLIENT_SECRET")
    # Allow direct token via env for flexibility
    token = os.environ.get("FABRIC_API_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if token:
        return token
    if not (tenant and client and secret):
        die("Missing authentication: set FABRIC_API_TOKEN or TENANT_ID/CLIENT_ID/CLIENT_SECRET in the environment")
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

def create_workspace(session: requests.Session, token: str, display_name: str, capacity_id: Optional[str] = None) -> dict:
    url = f"{API_BASE}/workspaces"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"displayName": display_name}
    if capacity_id:
        payload["capacityId"] = capacity_id
    r = session.post(url, headers=headers, json=payload, timeout=60)
    if not (200 <= r.status_code < 300):
        # surface full response for diagnostics
        raise RuntimeError(f"Create workspace failed: {r.status_code} {r.text}")
    try:
        return r.json()
    except Exception:
        return {"raw_text": r.text}

def assign_admin(session: requests.Session, token: str, workspace_id: str, admin_object_id: str,
                 max_retries: int = 3, backoff_seconds: int = 5) -> dict:
    """
    Attempt to assign admin role to admin_object_id on the workspace. Retries on failure.
    Returns a dict describing the result or raises on final failure.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Candidate endpoint used in the simple script pattern
    assign_url = f"{API_BASE}/workspaces/{workspace_id}/roleAssignments"
    payload = {
        "principal": {"id": admin_object_id, "type": "User"},
        "role": "Admin"
    }

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.post(assign_url, headers=headers, json=payload, timeout=30)
        except Exception as e:
            last_exc = e
            print(f"[admin assign] attempt {attempt} exception: {e}", file=sys.stderr)
            time.sleep(backoff_seconds * attempt)
            continue

        if 200 <= r.status_code < 300 or r.status_code == 201:
            # success
            try:
                parsed = r.json()
            except Exception:
                parsed = {"raw_text": r.text}
            return {"status": "succeeded", "status_code": r.status_code, "response": parsed, "attempts": attempt}
        else:
            # show error details
            body = None
            try:
                body = r.json()
            except Exception:
                body = {"raw_text": r.text}
            print(f"[admin assign] attempt {attempt} returned {r.status_code}: {r.text}", file=sys.stderr)
            # If client error likely not transient (e.g., 403/409), surface immediately
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise RuntimeError(f"Admin assignment failed (non-retriable): {r.status_code} {body}")
            last_exc = RuntimeError(f"Attempt {attempt} failed: {r.status_code} {body}")
            time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"Admin assignment failed after {max_retries} attempts: {last_exc}")

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--workspace-name", required=True, dest="workspace_name")
    p.add_argument("--sanitized-name", required=False, dest="sanitized_name", default="")
    p.add_argument("--dataset-name", required=False, dest="dataset_name")
    p.add_argument("--row-count", required=False, dest="row_count")
    p.add_argument("--source", required=False, dest="source")
    p.add_argument("--format", required=False, dest="format")
    p.add_argument("--update-strategy", required=False, dest="update_strategy")
    p.add_argument("--output", required=True, dest="output")
    p.add_argument("--admin-object-id", required=False, dest="admin_object_id",
                   help="Principal object id to assign as Admin (can also be set via ADMIN_OBJECT_ID env var)")
    p.add_argument("--assign-max-retries", type=int, default=3)
    p.add_argument("--assign-backoff-seconds", type=int, default=5)
    args = p.parse_args(argv)

    # auth and session
    token = get_token_from_env()
    session = requests.Session()

    # Create workspace
    capacity_id = os.environ.get("CAPACITY_ID")
    try:
        create_resp = create_workspace(session, token, args.workspace_name, capacity_id)
    except Exception as e:
        die(f"ERROR: workspace creation failed for '{args.workspace_name}': {e}")

    # Extract workspace id robustly
    workspace_id = None
    for k in ("id", "workspaceId", "workspace_id"):
        if isinstance(create_resp, dict) and k in create_resp:
            workspace_id = create_resp.get(k)
            break
    if not workspace_id:
        # Defensive: include raw response in error
        die(f"ERROR: workspace created but no workspace id found in response: {json.dumps(create_resp)[:1000]}")

    # Prepare the output object
    out = {
        "workspace_name": args.workspace_name,
        "sanitized_name": args.sanitized_name or "",
        "dataset_name": args.dataset_name,
        "row_count": args.row_count,
        "source": args.source,
        "format": args.format,
        "update_strategy": args.update_strategy,
        "timestamp_utc": now_iso(),
        "workspace_id": workspace_id,
        "created": True,
        "api_response": create_resp,
        "admin_assignment": None
    }

    # Admin assignment (if provided)
    admin_object_id = args.admin_object_id or os.environ.get("ADMIN_OBJECT_ID")
    if admin_object_id:
        try:
            assign_result = assign_admin(
                session, token, workspace_id, admin_object_id,
                max_retries=args.assign_max_retries,
                backoff_seconds=args.assign_backoff_seconds
            )
            out["admin_assignment"] = assign_result
        except Exception as e:
            # Write the output (so logs/artifacts include create response) and then fail loudly
            out["admin_assignment"] = {"status": "failed", "error": str(e)}
            try:
                write_json(args.output, out)
            except Exception:
                pass
            die(f"ERROR: admin assignment failed for workspace {workspace_id}: {e}")

    # Write the successful output and exit 0
    try:
        write_json(args.output, out)
    except Exception as e:
        die(f"Failed to write output to {args.output}: {e}")

    print(json.dumps(out))
    return 0

if __name__ == "__main__":
    import argparse as _argparse  # satisfy linter about argparse used above
    sys.exit(main())
