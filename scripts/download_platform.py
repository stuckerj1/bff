import os
import requests
import base64

# === CONFIGURATION ===
tenant_id = os.environ["TENANT_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]
workspace_id = "8bd0a1d4-ac54-4daf-b936-43c5c037b0d5"
notebook_id = "65deb31c-0971-452e-b1e9-7b4cd3e3e989"

# === AUTHENTICATION ===
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

# === GET NOTEBOOK DEFINITION ===
get_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}"
resp = requests.get(get_url, headers=headers)
resp.raise_for_status()
parts = resp.json()["definition"]["parts"]

# === FIND & DECODE .platform PART ===
for part in parts:
    if part["path"] == ".platform":
        platform_content = base64.b64decode(part["payload"]).decode("utf-8")
        print("----- .platform file contents -----")
        print(platform_content)
        with open(".platform", "w", encoding="utf-8") as f:
            f.write(platform_content)
        print("Saved as .platform")
        break
else:
    print("No .platform file found in notebook definition!")
