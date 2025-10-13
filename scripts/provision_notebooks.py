import os
import sys
import json
import base64
import requests

# Helper to read state file contents
def read_state_file(filename):
    path = os.path.join('.state', filename)
    if not os.path.exists(path):
        print(f"State file not found: {filename}")
        sys.exit(1)
    with open(path, "r") as f:
        return f.read().strip()

# Load secrets and IDs
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")
workspace_id = read_state_file("workspace_id.txt")

lakehouse_ids_path = os.path.join('.state', 'lakehouse_ids.txt')
if not os.path.exists(lakehouse_ids_path):
    print(f"Lakehouse IDs file not found: {lakehouse_ids_path}")
    sys.exit(1)
with open(lakehouse_ids_path, "r") as f:
    lakehouse_ids = [line.strip() for line in f if line.strip()]
if not lakehouse_ids or len(lakehouse_ids) < 2:
    print("Insufficient Lakehouse IDs found in lakehouse_ids.txt")
    sys.exit(1)
lakehouse_id = lakehouse_ids[1]  # DataSourceLakehouse (second entry)
lakehouse_name = "DataSourceLakehouse" # Update if your lakehouse name is different

print(f"[DEBUG] Using workspace_id: {workspace_id}")
print(f"[DEBUG] Using lakehouse_id for data source: {lakehouse_id}")
print(f"[DEBUG] Lakehouse name: {lakehouse_name}")

# Notebook info
notebook_display_name = "1.GenerateData"
notebook_path = "notebooks/generate_data.ipynb"
platform_path = ".platform"

if not os.path.exists(notebook_path):
    print(f"Notebook file not found: {notebook_path}")
    sys.exit(1)

# Create .platform metadata file (correct JSON structure for Fabric)
platform_metadata = (
    "# Fabric notebook source\n"
    "# METADATA ********************\n"
    "# META {\n"
    "# META   \"kernel_info\": {\n"
    "# META     \"name\": \"synapse_pyspark\"\n"
    "# META   },\n"
    "# META   \"dependencies\": {\n"
    "# META     \"lakehouse\": {\n"
    "# META       \"default_lakehouse\": \"" + lakehouse_id + "\",\n"
    "# META       \"default_lakehouse_name\": \"" + lakehouse_name + "\",\n"
    "# META       \"default_lakehouse_workspace_id\": \"" + workspace_id + "\"\n"
    "# META     }\n"
    "# META   }\n"
    "# META }\n"
)

with open(platform_path, "w", encoding="utf-8") as f:
    f.write(platform_metadata)

# Helper to base64 encode files
def encode_file(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

platform_encoded = encode_file(platform_path)
ipynb_encoded = encode_file(notebook_path)

# Get Fabric access token
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "https://api.fabric.microsoft.com/.default"
}
token_resp = requests.post(token_url, data=token_data)
token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Compose notebook upload payload with .platform metadata
payload = {
    "displayName": notebook_display_name,
    "type": "Notebook",
    "definition": {
        "parts": [
            {
                "path": ".platform",
                "payload": platform_encoded,
                "payloadType": "InlineBase64"
            },
            {
                "path": "generate_data.ipynb",
                "payload": ipynb_encoded,
                "payloadType": "InlineBase64"
            }
        ]
    }
}

upload_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
response = requests.post(upload_url, headers=headers, data=json.dumps(payload))
print("Status:", response.status_code)
print("Response:", response.text)

# Populate .state/notebook_ids.txt artifact if notebook was created
notebook_ids_path = os.path.join('.state', 'notebook_ids.txt')
try:
    if response.status_code in (200, 201):
        notebook_id = response.json()["id"]
        with open(notebook_ids_path, "w") as f:
            f.write(f"{notebook_id}\n")
        print(f"Notebook ID saved to {notebook_ids_path}")
    elif response.status_code == 202:
        notebook_id = response.json().get("id")
        if notebook_id:
            with open(notebook_ids_path, "w") as f:
                f.write(f"{notebook_id}\n")
            print(f"Notebook ID saved to {notebook_ids_path}")
        else:
            print("Notebook creation is asynchronous (202). Notebook ID not available yet.")
    else:
        print("Notebook was not created. No ID saved to .state/notebook_ids.txt.")
except Exception as e:
    print(f"Could not extract notebook ID from response: {e}")
