import os
import requests

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

url = f"{DATABRICKS_HOST}/api/2.0/clusters/list"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}

print("Calling:", url)

response = requests.get(url, headers=headers)

print("Status code:", response.status_code)

if response.status_code != 200:
    print("Error:")
    print(response.text)
else:
    print("SUCCESS 🎉")
    print("Clusters returned:", len(response.json().get("clusters", [])))

