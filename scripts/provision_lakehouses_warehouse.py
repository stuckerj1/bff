import os
import sys
import time
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

headers = {
    "Authorization": f"Bearer {fabric_access_token}",
    "Content-Type": "application/json"
}

# Step 2: Create Lakehouses
lakehouse_names = ["BenchmarkLakehouse", "DataSourceLakehouse"]
lakehouse_ids = []
lakehouse_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"

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
    elif response.status_code == 409:
        print(f"{name} already exists. Proceeding.")
        # Optionally: fetch lakehouse ID here if needed
    else:
        print(f"Error creating {name}: {response.text}")
        sys.exit(1)

# Step 3: Create Warehouse
warehouse_name = "BenchmarkWarehouse"
warehouse_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
warehouse_payload = {
    "displayName": warehouse_name,
    "description": "Warehouse for benchmarking copy and shortcut scenarios"
}
warehouse_response = requests.post(warehouse_url, headers=headers, json=warehouse_payload)

print(f"Warehouse creation status code: {warehouse_response.status_code}")
print(f"Warehouse creation response text: {warehouse_response.text}")

warehouse_id = None
if warehouse_response.status_code == 201:
    warehouse_id = warehouse_response.json()["id"]
    print(f"{warehouse_name} created. ID: {warehouse_id}")
elif warehouse_response.status_code == 202:
    print(f"{warehouse_name} creation accepted (async). Polling for completion...")
    # Poll for warehouse existence
    poll_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    max_attempts = 12  # 12 x 5s = 1 minute
    for attempt in range(max_attempts):
        time.sleep(5)
        poll_response = requests.get(poll_url, headers=headers)
        if poll_response.status_code == 200:
            warehouses = poll_response.json().get("value", [])
            for wh in warehouses:
                if wh.get("displayName") == warehouse_name:
                    warehouse_id = wh.get("id")
                    print(f"{warehouse_name} is now available. ID: {warehouse_id}")
                    break
        if warehouse_id:
            break
        print(f"Attempt {attempt+1}/{max_attempts}: {warehouse_name} not available yet.")
    if not warehouse_id:
        print(f"Error: {warehouse_name} was not found after polling.")
        sys.exit(1)
elif warehouse_response.status_code == 409:
    print(f"{warehouse_name} already exists. Proceeding.")
    # Optionally: fetch warehouse ID here if needed
else:
    print(f"Error creating {warehouse_name}: {warehouse_response.text}")
    sys.exit(1)

# Step 4: Save IDs to state files
os.makedirs('.state', exist_ok=True)
with open('.state/lakehouse_ids.txt', 'w') as f:
    for lakehouse_id in lakehouse_ids:
        f.write(f"{lakehouse_id}\n")
print("Lakehouse IDs saved to .state/lakehouse_ids.txt")

if warehouse_id:
    with open('.state/warehouse_id.txt', 'w') as f:
        f.write(warehouse_id)
    print("Warehouse ID saved to .state/warehouse_id.txt")
else:
    print("Warehouse ID was not obtained.")
    sys.exit(1)
