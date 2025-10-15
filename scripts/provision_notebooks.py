import os
import json
import base64
import sys
import requests
import time

# === CONFIGURATION ===
notebook_display_name = "1.GenerateData"
notebook_path = "notebooks/generate_data.ipynb"
lakehouse_name = "DataSourceLakehouse"
workspace_name = "FabricBenchmarking"

# === AUTHENTICATION ===
tenant_id = os.environ["TENANT_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

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

# === RESOLVE WORKSPACE ID ===
ws_resp = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers)
ws_resp.raise_for_status()
workspace_id = next(
    (w["id"] for w in ws_resp.json()["value"] if w["displayName"] == workspace_name),
    None
)
if not workspace_id:
    raise Exception(f"Workspace '{workspace_name}' not found.")

# === RESOLVE LAKEHOUSE ID ===
lh_resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses", headers=headers)
lh_resp.raise_for_status()
lakehouse_id = next(
    (l["id"] for l in lh_resp.json()["value"] if l["displayName"] == lakehouse_name),
    None
)
if not lakehouse_id:
    raise Exception(f"Lakehouse '{lakehouse_name}' not found in workspace '{workspace_name}'.")

print(f"[DEBUG] Using workspace_id: {workspace_id}")
print(f"[DEBUG] Using lakehouse_id for data source: {lakehouse_id}")
print(f"[DEBUG] Lakehouse name: {lakehouse_name}")

# === BASE64 ENCODE FILES ===
def encode_file(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

ipynb_encoded = encode_file(notebook_path)

# === UPLOAD NOTEBOOK ===
upload_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
payload = {
    "displayName": notebook_display_name,
    "type": "Notebook",    
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "generate_data.ipynb",
                "payload": ipynb_encoded,
                "payloadType": "InlineBase64"
            }
        ]
    }
}
upload_resp = requests.post(upload_url, headers=headers, data=json.dumps(payload))
print("Status:", upload_resp.status_code)
print("Response:", upload_resp.text)

notebook_ids_path = os.path.join('.state', 'notebook_ids.txt')
notebook_id_saved = False
notebook_id = None

if upload_resp.status_code in (200, 201):
    notebook_id = upload_resp.json()["id"]
    with open(notebook_ids_path, "w") as f:
        f.write(f"{notebook_id}\n")
    print(f"Notebook ID saved to {notebook_ids_path}")
    notebook_id_saved = True
elif upload_resp.status_code == 202:
    # Poll for notebook creation
    print("Notebook creation is asynchronous. Waiting for notebook to appear in workspace...")
    MAX_ATTEMPTS = 20
    SLEEP_SECONDS = 10
    attempts = 0
    while attempts < MAX_ATTEMPTS and not notebook_id:
        time.sleep(SLEEP_SECONDS)
        print(f"Polling attempt {attempts+1}...")
        nb_resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks", headers=headers)
        nb_resp.raise_for_status()
        notebooks = nb_resp.json().get("value", [])
        for nb in notebooks:
            if nb.get("displayName") == notebook_display_name:
                notebook_id = nb.get("id")
                break
        attempts += 1
    if notebook_id:
        with open(notebook_ids_path, "w") as f:
            f.write(f"{notebook_id}\n")
        print(f"Notebook ID (async) saved to {notebook_ids_path}")
        notebook_id_saved = True
    else:
        print("ERROR: Notebook was not provisioned after max attempts.")
        sys.exit(1)
else:
    print("Notebook was not created. No ID saved to .state/notebook_ids.txt.")

if not notebook_id_saved or not notebook_id:
    print("ERROR: Notebook provisioning failed. See above for details.")
    sys.exit(1)

# === Poll for notebook to appear in /notebooks endpoint ===
attempts = 0
found_in_notebooks = False
while attempts < MAX_ATTEMPTS and not found_in_notebooks:
    time.sleep(SLEEP_SECONDS)
    print(f"Polling for notebook in /notebooks (attempt {attempts+1})...", flush=True)
    nb_resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks", headers=headers)
    nb_resp.raise_for_status()
    notebooks = nb_resp.json().get("value", [])
    for nb in notebooks:
        if nb.get("id") == notebook_id:
            found_in_notebooks = True
            break
    attempts += 1

if not found_in_notebooks:
    print("ERROR: Notebook did not appear in /notebooks endpoint after max attempts.", flush=True)
    sys.exit(1)

# === UPDATE DEFAULT LAKEHOUSE FOR THE NOTEBOOK ===
print(f"Updating default lakehouse for notebook {notebook_id} ...")
update_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}/NotebookUtils.update_default_lakehouse"
update_payload = {
    "lakehouseId": lakehouse_id,
    "lakehouseName": lakehouse_name,
    "workspaceId": workspace_id
}
update_resp = requests.post(update_url, headers=headers, json=update_payload)
print("Lakehouse update status:", update_resp.status_code)
print("Lakehouse update response:", update_resp.text)
if update_resp.status_code not in (200, 204):
    print("ERROR: Failed to update default lakehouse.")
    sys.exit(1)
else:
    print("Successfully updated default lakehouse for notebook.")
