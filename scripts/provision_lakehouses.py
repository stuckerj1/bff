import os
import requests

# Load environment variables
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

# Read workspace ID from state file
with open('.state/workspace_id.txt', 'r') as f:
    workspace_id = f.read().strip()

# Step 1: OAuth2 Token Request
fabric_scope = "https://api.fabric.microsoft.com/.default"
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
fabric_token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": fabric_scope
}
fabric_token_response = requests.post(token_url, data=fabric_token_data)
fabric_token_response.raise_for_status()
fabric_access_token = fabric_token_response.json()["access_token"]

# Step 2: Create Lakehouses
lakehouse_names = ["BenchmarkLakehouse", "DataSourceLakehouse"]
lakehouse_ids = []
lakehouse_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
headers = {
    "Authorization": f"Bearer {fabric_access_token}",
    "Content-Type": "application/json"
}

for name in lakehouse_names:
    payload = {
        "displayName": name,
        "description": f"Lakehouse for {name}"
    }
    response = requests.post(lakehouse_url, headers=headers, json=payload)
    if response.status_code == 201:
        lakehouse_id = response.json()["id"]
        print(f"{name} created. ID: {lakehouse_id}")
        lakehouse_ids.append(lakehouse_id)
    else:
        print(f"Error creating {name}: {response.text}")

# Step 3: Save Lakehouse IDs to state file
os.makedirs('.state', exist_ok=True)
with open('.state/lakehouse_ids.txt', 'w') as f:
    for lakehouse_id in lakehouse_ids:
        f.write(f"{lakehouse_id}\n")
print("Lakehouse IDs saved to .state/lakehouse_ids.txt")
