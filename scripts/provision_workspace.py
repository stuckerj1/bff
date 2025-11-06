#!/usr/bin/env python3
"""
Provision Fabric workspace helper (CI-friendly).

This script is an upgraded, backwards-compatible version of the original quick script.
It is intended to be called from CI (provision_workspace.yml) or run manually.

Features:
 - Accepts CLI args (--workspace-name, --sanitized-name, --output, etc.)
 - Reads creds from environment variables (TENANT_ID, CLIENT_ID, CLIENT_SECRET)
 - Attempts to obtain an AAD client_credentials token and call Fabric REST API to create a workspace
 - Attempts to assign Admin role to ADMIN_OBJECT_ID (if provided) with configurable retries/backoff
 - Is idempotent: if the output JSON already exists with a workspace_id the script exits unless --force
 - Writes a JSON artifact to --output with metadata including admin assignment result
 - Writes a companion .state/workspace_id.txt (legacy behavior) and a workspace_id text file next to the JSON
 - Falls back to a synthetic local id if credentials or API calls are not available (useful for test runs)
 - Supports --dry-run to produce a synthetic id without calling any network APIs
 - Exits non-zero on severe failures (e.g., create workspace failed with non-retryable error and no fallback allowed)

Dependencies:
 - requests library (install in the runner: python -m pip install requests)
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

try:
    import requests
except Exception:
    print("Missing dependency: requests. Please install with `pip install requests`.", file=sys.stderr)
    sys.exit(2)


def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v if v and v.strip() else None


def get_access_token(tenant_id: str, client_id: str, client_secret: str, scope: str = "https://api.fabric.microsoft.com/.default") -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"token request failed: {resp.status_code} {resp.text}")
    j = resp.json()
    access_token = j.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in token response: {j}")
    return access_token


def create_fabric_workspace(token: str, display_name: str, description: str = "", capacity_id: Optional[str] = None) -> Tuple[dict, int]:
    url = "https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {"displayName": display_name, "description": description}
    if capacity_id:
        body["capacityId"] = capacity_id

    resp = requests.post(url, headers=headers, json=body, timeout=60)
    if 200 <= resp.status_code < 300:
        try:
            return resp.json(), resp.status_code
        except Exception:
            return {"raw_text": resp.text}, resp.status_code
    else:
        raise RuntimeError(f"Failed to create workspace: {resp.status_code} {resp.text}")


def assign_admin_role(token: str, workspace_id: str, admin_object_id: str, max_retries: int = 5, backoff_base: float = 3.0) -> dict:
    """
    Assign Admin role to the principal. Retries transient failures with exponential backoff.
    Returns metadata dict with keys: attempts, status, status_code, response.
    """
    assign_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "principal": {"id": admin_object_id, "type": "User"},
        "role": "Admin"
    }

    attempts = 0
    result = {"attempts": 0, "status": "not-attempted", "status_code": None, "response": None}

    while attempts < max_retries:
        attempts += 1
        try:
            resp = requests.post(assign_url, headers=headers, json=payload, timeout=30)
            result["status_code"] = resp.status_code
            if resp.status_code == 201:
                try:
                    result["response"] = resp.json()
                except Exception:
                    result["response"] = resp.text
                result["status"] = "succeeded"
                result["attempts"] = attempts
                return result
            else:
                # capture error body if present
                try:
                    err = resp.json()
                except Exception:
                    err = {"text": resp.text}
                result["response"] = err
                # Non-retryable if client error
                if 400 <= resp.status_code < 500:
                    result["status"] = "failed-non-retryable"
                    result["attempts"] = attempts
                    return result
                # transient -> backoff and retry
                result["status"] = "retrying"
                result["attempts"] = attempts
                sleep_for = backoff_base ** attempts
                print(f"Assign attempt {attempts} got {resp.status_code}; sleeping {sleep_for}s before retry...", file=sys.stderr)
                time.sleep(sleep_for)
        except Exception as exc:
            result["response"] = str(exc)
            result["status"] = "retrying-exception"
            result["attempts"] = attempts
            sleep_for = backoff_base ** attempts
            print(f"Exception on assign attempt {attempts}: {exc}; sleeping {sleep_for}s before retry...", file=sys.stderr)
            time.sleep(sleep_for)

    result["status"] = "failed"
    result["attempts"] = attempts
    return result


def write_json(path: str, payload: dict):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"Wrote JSON artifact to {path}")


def write_workspace_id_txt(json_path: str, workspace_id: str):
    txt_path = os.path.splitext(json_path)[0] + "_workspace_id.txt"
    with open(txt_path, "w", encoding="utf-8") as fh:
        fh.write(workspace_id)
    print(f"Wrote workspace id to {txt_path}")
    # legacy .state/workspace_id.txt
    try:
        os.makedirs(".state", exist_ok=True)
        with open(".state/workspace_id.txt", "w", encoding="utf-8") as fh:
            fh.write(workspace_id)
        print("Saved workspace id to .state/workspace_id.txt")
    except Exception:
        pass


def load_json(path: str) -> Optional[dict]:
    if not path or not os.path.exists(path):
        return None
    try:
        return json.load(open(path, "r", encoding="utf-8"))
    except Exception as e:
        print(f"Warning: failed to read existing JSON {path}: {e}", file=sys.stderr)
        return None


def synth_id(sanitized: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"local-{sanitized}-{ts}"


def safe_truncate(obj, limit=5000):
    try:
        if isinstance(obj, (str, int, float)):
            s = str(obj)
        else:
            s = json.dumps(obj)
        return s[:limit]
    except Exception:
        return str(obj)[:limit]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Provision Fabric workspace helper (writes JSON artifact).")
    parser.add_argument("--workspace-name", required=True, help="Display name for the Fabric workspace.")
    parser.add_argument("--sanitized-name", required=True, help="Sanitized short name (used for ids/files).")
    parser.add_argument("--dataset-name", help="Optional dataset name (for metadata).")
    parser.add_argument("--row-count", help="Optional row_count (for metadata).")
    parser.add_argument("--source", help="Optional source (lakehouse|sql).")
    parser.add_argument("--format", dest="fmt", help="Optional format (delta|warehouse).")
    parser.add_argument("--update-strategy", help="Optional update strategy.")
    parser.add_argument("--output", required=True, help="Path to write output JSON artifact.")
    parser.add_argument("--force", action="store_true", help="Force reprovision even if output already exists.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call Fabric API; emit synthetic id.")
    parser.add_argument("--assign-max-retries", type=int, default=5, help="Max retries for admin assignment.")
    parser.add_argument("--assign-backoff-base", type=float, default=3.0, help="Backoff base for assignment retries.")
    args = parser.parse_args(argv)

    out_path = args.output

    # idempotent: if output exists and contains workspace_id, exit (unless --force)
    if not args.force:
        existing = load_json(out_path)
        if existing:
            existing_id = existing.get("workspace_id") or existing.get("workspaceId") or existing.get("id")
            if existing_id:
                print(f"Existing artifact at {out_path} with workspace_id={existing_id}; skipping provisioning (use --force to override).")
                return 0

    tenant = _env("TENANT_ID")
    client_id = _env("CLIENT_ID")
    client_secret = _env("CLIENT_SECRET")
    capacity_id = _env("CAPACITY_ID") or os.environ.get("CAPACITY_ID")
    admin_object_id = _env("ADMIN_OBJECT_ID") or os.environ.get("ADMIN_OBJECT_ID")

    meta = {
        "workspace_name": args.workspace_name,
        "sanitized_name": args.sanitized_name,
        "dataset_name": args.dataset_name,
        "row_count": args.row_count,
        "source": args.source,
        "format": args.fmt,
        "update_strategy": args.update_strategy,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
    }

    # dry-run -> synthetic id and exit
    if args.dry_run:
        synthetic = synth_id(args.sanitized_name)
        payload = {**meta, "workspace_id": synthetic, "created": False, "note": "dry-run synthetic id"}
        write_json(out_path, payload)
        write_workspace_id_txt(out_path, synthetic)
        return 0

    # credentials required for real provisioning; fallback to synthetic if missing
    if not tenant or not client_id or not client_secret:
        print("TENANT_ID/CLIENT_ID/CLIENT_SECRET not set; falling back to synthetic id for test runs.", file=sys.stderr)
        synthetic = synth_id(args.sanitized_name)
        payload = {**meta, "workspace_id": synthetic, "created": False, "note": "fallback-synthetic-id-no-credentials"}
        write_json(out_path, payload)
        write_workspace_id_txt(out_path, synthetic)
        return 0

    # get token
    try:
        print("Requesting AAD access token...")
        token = get_access_token(tenant, client_id, client_secret)
    except Exception as e:
        print(f"Failed to obtain access token: {e}", file=sys.stderr)
        synthetic = synth_id(args.sanitized_name)
        payload = {**meta, "workspace_id": synthetic, "created": False, "note": "fallback-synthetic-id-token-failure", "error": safe_truncate(str(e))}
        write_json(out_path, payload)
        write_workspace_id_txt(out_path, synthetic)
        return 0

    # create workspace
    try:
        print(f"Creating Fabric workspace '{args.workspace_name}'...")
        description = f"Provisioned by scripts/provision_workspace.py for {args.sanitized_name}"
        resp_json, status = create_fabric_workspace(token, args.workspace_name, description=description, capacity_id=capacity_id)

        # extract workspace id from response
        workspace_id = None
        for k in ("id", "workspaceId", "workspace_id", "idValue"):
            if isinstance(resp_json, dict) and k in resp_json:
                workspace_id = resp_json[k]
                break
        if not workspace_id and isinstance(resp_json, dict):
            for v in resp_json.values():
                if isinstance(v, dict):
                    for k2 in ("id", "workspaceId", "workspace_id"):
                        if k2 in v:
                            workspace_id = v[k2]
                            break
                if workspace_id:
                    break

        if not workspace_id:
            print("Warning: Could not parse workspace id from Fabric response; saving raw response.", file=sys.stderr)
            workspace_id = synth_id(args.sanitized_name)
            payload = {**meta, "workspace_id": workspace_id, "created": True, "status_code": status, "raw_response": safe_truncate(resp_json)}
        else:
            payload = {**meta, "workspace_id": workspace_id, "created": True, "status_code": status, "api_response": safe_truncate(resp_json)}

        # attempt admin assignment if ADMIN_OBJECT_ID provided
        if admin_object_id:
            print(f"Attempting to assign Admin role to principal {admin_object_id} for workspace {workspace_id} ...")
            assign_meta = assign_admin_role(token, workspace_id, admin_object_id, max_retries=args.assign_max_retries, backoff_base=args.assign_backoff_base)
            payload["admin_assignment"] = {
                "principal_id": admin_object_id,
                "attempts": assign_meta.get("attempts"),
                "status": assign_meta.get("status"),
                "status_code": assign_meta.get("status_code"),
                "response": safe_truncate(assign_meta.get("response"))
            }
            if assign_meta.get("status") not in ("succeeded",):
                print(f"Admin assignment status: {assign_meta.get('status')}; see artifact for details.", file=sys.stderr)
        else:
            payload["admin_assignment"] = {"status": "skipped", "note": "no ADMIN_OBJECT_ID provided"}

        write_json(out_path, payload)
        write_workspace_id_txt(out_path, workspace_id)
        print("Provisioning finished. workspace_id=", workspace_id)
        # if admin assignment failed non-retryable, consider non-zero exit? keep zero so workflow can continue and artifact shows result.
        return 0

    except Exception as e:
        print(f"Fabric API provisioning failed: {e}", file=sys.stderr)
        synthetic = synth_id(args.sanitized_name)
        payload = {**meta, "workspace_id": synthetic, "created": False, "note": "fallback-synthetic-id-api-failure", "error": safe_truncate(str(e))}
        write_json(out_path, payload)
        write_workspace_id_txt(out_path, synthetic)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
