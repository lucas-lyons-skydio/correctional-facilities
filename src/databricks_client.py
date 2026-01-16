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

from databricks import sql

def get_sql_connection():
    databricks_host = os.getenv("DATABRICKS_HOST")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    token = os.getenv("DATABRICKS_TOKEN")

    if not databricks_host:
        raise RuntimeError("DATABRICKS_HOST is not set")
    if not warehouse_id:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN is not set")

    # Strip scheme for SQL connector
    server_hostname = databricks_host.replace("https://", "").replace("http://", "")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )

