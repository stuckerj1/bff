import os
import requests
import sys
import json

# Load environment variables (secrets)
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

if not all([tenant_id, client_id, client_secret]):
    print("Missing secrets: TENANT_ID, CLIENT_ID, CLIENT_SECRET must be set")
    sys.exit(1)

# Read IDs from state files
def read_id(filename):
    path = os.path.join('.state', filename)
    if not os.path.exists(path):
        print(f"Missing state file: {filename}")
        sys.exit(1)
    with open(path, 'r') as f:
        return f.read().strip()

workspace_id = read_id("workspace_id.txt")
lakehouse_ids = []
lakehouse_ids_path = os.path.join('.state', 'lakehouse_ids.txt')
if os.path.exists(lakehouse_ids_path):
    with open(lakehouse_ids_path) as f:
        lakehouse_ids = [line.strip() for line in f if line.strip()]
warehouse_id = read_id("warehouse_id.txt")

# Authenticate: get Fabric access token
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

# Define notebooks to create, with file paths
notebooks_to_create = [
    {
        "displayName": "1.GenerateData",
        "description": "Notebook for synthetic data generation",
        "file": "notebooks/generate_data.ipynb",
    },
    {
        "displayName": "2.IngestData",
        "description": "Notebook for ingesting data into lakehouse/warehouse",
        "file": "notebooks/ingest_data.ipynb",
    },
    {
        "displayName": "3.ApplyUpdates",
        "description": "Notebook for applying incremental updates",
        "file": "notebooks/apply_updates.ipynb",
    },
    {
        "displayName": "4.RunQueries",
        "description": "Notebook for running benchmark queries",
        "file": "notebooks/run_queries.ipynb",
    }
]

notebook_ids = []
notebook_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"

for notebook in notebooks_to_create:
    # Read notebook content from file
    ipynb_path = notebook["file"]
    if not os.path.exists(ipynb_path):
        print(f"Notebook file not found: {ipynb_path}")
        sys.exit(1)
    with open(ipynb_path, "r", encoding="utf-8") as f:
        notebook_content = json.load(f)  # Load as JSON object

    payload = {
        "displayName": notebook["displayName"],
        "description": notebook["description"],
        "content": notebook_content,  # Pass as JSON object
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
        # Optionally: fetch notebook ID here if needed
    else:
        print(f"Error creating notebook '{notebook['displayName']}': {response.text}")
        sys.exit(1)

# Write notebook IDs to state file
os.makedirs('.state', exist_ok=True)
with open('.state/notebook_ids.txt', 'w') as f:
    for notebook_id in notebook_ids:
        f.write(f"{notebook_id}\n")

# Optionally log metadata
metadata = {
    "workspace_id": workspace_id,
    "lakehouse_ids": lakehouse_ids,
    "warehouse_id": warehouse_id,
    "notebook_ids": notebook_ids
}
with open('.state/notebook_ids.meta.json', 'w') as f:
    json.dump(metadata, f, indent=2)

print("Notebook provisioning complete. IDs saved to .state/notebook_ids.txt")
