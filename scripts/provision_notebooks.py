import os
import json
import base64
import sys
import requests

# === CONFIGURATION ===
notebook_display_name = "1.GenerateData"
notebook_path = "notebooks/generate_data.ipynb"
lakehouse_name = "DataSourceLakehouse"
workspace_name = "FabricBenchmarking"
platform_py_path = "platform_metadata.py"

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

# === GENERATE .py METADATA FILE ===
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
    "\n"
)
# platform_metadata += "\npass  # Required to make this a valid Python file"
with open(platform_py_path, "w", encoding="utf-8") as f:
    f.write(platform_metadata)
print("----- platform_metadata.py contents -----")
with open(platform_py_path, "r", encoding="utf-8") as debug_f:
    print(debug_f.read())
print("----- end platform_metadata.py -----")

# === BASE64 ENCODE FILES ===
def encode_file(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

platform_encoded = encode_file(platform_py_path)
ipynb_encoded = encode_file(notebook_path)

# === UPLOAD NOTEBOOK ===
upload_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
payload = {
    "displayName": notebook_display_name,
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
upload_resp = requests.post(upload_url, headers=headers, data=json.dumps(payload))
print("Status:", upload_resp.status_code)
print("Response:", upload_resp.text)

# === Save Notebook ID if created ===
notebook_ids_path = os.path.join('.state', 'notebook_ids.txt')
notebook_id_saved = False
try:
    if upload_resp.status_code in (200, 201):
        notebook_id = upload_resp.json()["id"]
        with open(notebook_ids_path, "w") as f:
            f.write(f"{notebook_id}\n")
        print(f"Notebook ID saved to {notebook_ids_path}")
        notebook_id_saved = True
    elif upload_resp.status_code == 202:
        notebook_id = upload_resp.json().get("id")
        if notebook_id:
            with open(notebook_ids_path, "w") as f:
                f.write(f"{notebook_id}\n")
            print(f"Notebook ID saved to {notebook_ids_path}")
            notebook_id_saved = True
        else:
            print("Notebook creation is asynchronous (202). Notebook ID not available yet.")
    else:
        print("Notebook was not created. No ID saved to .state/notebook_ids.txt.")
except Exception as e:
    print(f"Could not extract notebook ID from response: {e}")

if not notebook_id_saved:
    print("ERROR: Notebook provisioning failed. See above for details.")
    sys.exit(1)  # Explicit failure for CI
