import os
import requests

# Load environment variables
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")
capacity_id = os.environ.get("CAPACITY_ID")
admin_object_id = os.environ.get("ADMIN_OBJECT_ID")

# Step 1: OAuth2 Token Request
fabric_scope = "https://api.fabric.microsoft.com/.default"
graph_scope = "https://graph.microsoft.com/.default"

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Fabric token
fabric_token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": fabric_scope
}
fabric_token_response = requests.post(token_url, data=fabric_token_data)
fabric_token_response.raise_for_status()
fabric_access_token = fabric_token_response.json()["access_token"]

# Graph token
graph_token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": graph_scope
}
graph_token_response = requests.post(token_url, data=graph_token_data)
graph_token_response.raise_for_status()
graph_access_token = graph_token_response.json()["access_token"]

# Step 2: Create Fabric Workspace
workspace_url = "https://api.fabric.microsoft.com/v1/workspaces"
headers = {
    "Authorization": f"Bearer {fabric_access_token}",
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
    workspace_id = workspace_response.json()["id"]
    print("Workspace created successfully.")
    print("Workspace ID:", workspace_id)

    # Step 4: Assign initial admin role
    admin_object_id = os.environ.get("ADMIN_OBJECT_ID")
    assign_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments"
    assign_payload = {
    "principal": {
        "id": admin_object_id,
        "type": "User"
    },
    "role": "Admin"
}

    print("Assign payload:", assign_payload)
    # ✅ Graph API lookup to verify object ID
    graph_url = f"https://graph.microsoft.com/v1.0/users/{admin_object_id}"
    graph_headers = {
        "Authorization": f"Bearer {graph_access_token}"
    }
    graph_response = requests.get(graph_url, headers=graph_headers)
    print("Graph lookup result:", graph_response.json())

    assign_response = requests.post(assign_url, headers=headers, json=assign_payload)

    if assign_response.status_code == 200:
        print(f"Assigned {admin_object_id} as Admin.")
    else:
        print("Error assigning admin:", assign_response.text)
else:
    print("Error creating workspace:", workspace_response.text)
