import os
import sys
import json
import base64
import requests

# Load secrets
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

# Read workspace and lakehouse IDs
def read_state_file(filename):
    path = os.path.join('.state', filename)
    if not os.path.exists(path):
        print(f"State file not found: {filename}")
        sys.exit(1)
    with open(path, "r") as f:
        return f.read().strip()

workspace_id = read_state_file("workspace_id.txt")

# Read first lakehouse ID from .state/lakehouse_ids.txt
lakehouse_ids_path = os.path.join('.state', 'lakehouse_ids.txt')
if not os.path.exists(lakehouse_ids_path):
    print(f"Lakehouse IDs file not found: {lakehouse_ids_path}")
    sys.exit(1)

with open(lakehouse_ids_path, "r") as f:
    lakehouse_ids = [line.strip() for line in f if line.strip()]
if not lakehouse_ids:
    print("No lakehouse IDs found in lakehouse_ids.txt")
    sys.exit(1)
lakehouse_id = lakehouse_ids[0]  # Use the first Lakehouse ID

# Get access token
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
fabric_scope = "https://api.fabric.microsoft.com/.default"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": fabric_scope
}
token_resp = requests.post(token_url, data=token_data)
token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Prepare notebook file
notebook_path = "notebooks/generate_data.ipynb"
if not os.path.exists(notebook_path):
    print(f"Notebook file not found: {notebook_path}")
    sys.exit(1)

with open(notebook_path, "rb") as f:
    notebook_bytes = f.read()
notebook_base64 = base64.b64encode(notebook_bytes).decode("utf-8")

# Create the payload for /items
payload = {
    "displayName": "1.GenerateData",
    "type": "Notebook",
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "generate_data.ipynb",
                "payload": notebook_base64,
                "payloadType": "InlineBase64"
            }
        ]
    },
    "dataSources": [
        {
            "id": lakehouse_id,
            "type": "Lakehouse"
        }
    ]
}

notebook_api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
response = requests.post(notebook_api_url, headers=headers, json=payload)
print("Status:", response.status_code)
print("Response:", response.text)
