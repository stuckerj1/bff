import os
import requests

# Step 1: OAuth2 Token Request
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")
scope = "https://graph.microsoft.com/.default"

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": scope
}
token_response = requests.post(token_url, data=token_data)
token_response.raise_for_status()
access_token = token_response.json()["access_token"]

# Step 2: Create Fabric Workspace
workspace_url = "https://graph.microsoft.com/v1.0/fabric/workspaces"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
workspace_payload = {
    "displayName": "FabricBenchmarking",
    "description": "Benchmarking workspace for synthetic data tests",
    "capacityId": os.environ.get("CAPACITY_ID")
}
workspace_response = requests.post(workspace_url, headers=headers, json=workspace_payload)

# Step 3: Output Result
if workspace_response.status_code == 201:
    print("Workspace created successfully.")
    print("Workspace ID:", workspace_response.json()["id"])
else:
    print("Error creating workspace:", workspace_response.text)
