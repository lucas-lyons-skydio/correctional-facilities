import os
import requests

class DatabricksClient:
    def __init__(self):
        self.host = os.environ["DATABRICKS_HOST"]
        self.token = os.environ["DATABRICKS_TOKEN"]

        self.headers = {
            "Authorization": f"Bearer {self.token}"
        }

    def get(self, path: str):
        url = f"{self.host}{path}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
