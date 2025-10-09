"""
Provision two lakehouses in an existing Fabric workspace:
- DataSourceLakehouse (serves as external data source)
- BenchmarkLakehouse (target for benchmarking ingestion)

Usage:
    Set environment variables for secrets/config:
        TENANT_ID
        CLIENT_ID
        CLIENT_SECRET
        FABRIC_WORKSPACE_ID

    python scripts/provision_lakehouses.py
"""

import os
import requests
import sys

# Load secrets/config from environment variables
TENANT_ID = os.environ.get("TENANT_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
WORKSPACE_ID = os.environ.get("FABRIC_WORKSPACE_ID")

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID]):
    print("Error: One or more required environment variables are missing.")
    sys.exit(1)

# OAuth2 token request (same as in provision_workspace.py)
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

fabric_token_data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": FABRIC_SCOPE
}
fabric_token_response = requests.post(TOKEN_URL, data=fabric_token_data)
fabric_token_response.raise_for_status()
fabric_access_token = fabric_token_response.json()["access_token"]

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

def create_lakehouse(workspace_id, access_token, display_name, description):
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "displayName": display_name,
        "description": description
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 201:
        lakehouse_id = resp.json().get("id")
        print(f"Lakehouse '{display_name}' created successfully. ID: {lakehouse_id}")
        return lakehouse_id
    else:
        print(f"Failed to create lakehouse '{display_name}'. Status: {resp.status_code}, Message: {resp.text}")
        return None

def main():
    ds_lakehouse_id = create_lakehouse(
        WORKSPACE_ID,
        fabric_access_token,
        "DataSourceLakehouse",
        "Lakehouse serving as the external data source for ingestion simulation"
    )

    bm_lakehouse_id = create_lakehouse(
        WORKSPACE_ID,
        fabric_access_token,
        "BenchmarkLakehouse",
        "Lakehouse for benchmarking synthetic data and update strategies"
    )

    if ds_lakehouse_id and bm_lakehouse_id:
        print("\nLakehouses provisioned successfully:")
        print(f"  DataSourceLakehouse ID:   {ds_lakehouse_id}")
        print(f"  BenchmarkLakehouse ID:    {bm_lakehouse_id}")
    else:
        print("\nError: One or more lakehouses could not be created.")
        sys.exit(1)

if __name__ == "__main__":
    main()