import os
import requests
import sys
import json

tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

if not all([tenant_id, client_id, client_secret]):
    print("Missing secrets: TENANT_ID, CLIENT_ID, CLIENT_SECRET must be set")
    sys.exit(1)

def read_id(filename):
    path = os.path.join('.state', filename)
    if not os.path.exists(path):
        print(f"Missing state file: {filename}")
        sys.exit(1)
    with open(path, 'r') as f:
        return f.read().strip()

workspace_id = read_id("workspace_id.txt")

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

notebooks_to_create = [
    {
        "displayName": "1.GenerateData",
        "description": "Notebook for synthetic data generation",
        "file": "notebooks/generate_data.ipynb",
    }
    # ... add others as needed ...
]

notebook_ids = []
notebook_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"

for notebook in notebooks_to_create:
    ipynb_path = notebook["file"]
    if not os.path.exists(ipynb_path):
        print(f"Notebook file not found: {ipynb_path}")
        sys.exit(1)
    with open(ipynb_path, "r", encoding="utf-8") as f:
        ipynb_json = json.load(f)  # load as JSON

    payload = {
        "createItemRequest": {
            "displayName": notebook["displayName"],
            "description": notebook["description"]
        },
        "definition": ipynb_json
    }

    response = requests.post(notebook_url, headers=headers, json=payload)
    print(f"Uploading notebook '{notebook['displayName']}' from {ipynb_path}...")
    print("Status:", response.status_code)
    print("Response:", response.text)
    if response.status_code == 201:
        notebook_id = response.json()["id"]
        print(f"Created notebook '{notebook['displayName']}' (ID: {notebook_id})")
        notebook_ids.append(notebook_id)
    elif response.status_code == 409:
        print(f"Notebook '{notebook['displayName']}' already exists. Skipping.")
    else:
        print(f"Error creating notebook '{notebook['displayName']}': {response.text}")
        sys.exit(1)

print("Notebook provisioning complete.")
