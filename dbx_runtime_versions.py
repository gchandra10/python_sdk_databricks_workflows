import requests
from utilities.logger import setup_logger


class DBXRuntime:
    def __init__(self, workspace_url, token) -> None:
        self.logger = setup_logger("Get Runtime Versions")
        self.workspace_url = workspace_url
        self.token = token

    def get_databricks_runtimes(self):
        """
        Fetches the list of available Databricks runtimes.

        Args:
            workspace_url (str): The URL of the Databricks workspace.
            token (str): The API token for authentication.

        Returns:
            list: A list of available Databricks runtimes, or None if an error occurs.
        """
        try:
            url = f"{self.workspace_url}/api/2.0/clusters/spark-versions"
            headers = {"Authorization": f"Bearer {self.token}"}

            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            self.logger.info(f"Returning Runtime Versions.")
            return response.json().get("versions", [])

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching Databricks runtimes: {e}")
            print(f"Error fetching Databricks runtimes: {e}")
            return None
