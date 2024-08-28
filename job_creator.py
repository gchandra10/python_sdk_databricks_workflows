import os
import requests
from utilities.logger import setup_logger


class JobCreator:
    def __init__(self, workspace_url, token) -> None:
        self.logger = setup_logger("Create JSON Jobs")
        self.workspace_url = workspace_url
        self.token = token

    def create_job(self, json_data):
        """
        Creates a job in Databricks using the given JSON data.

        Parameters:
        json_data (str): JSON string containing job configuration.

        Returns:
        dict: Response from the Databricks API containing job details.
        """
        url = f"{self.workspace_url}/api/2.1/jobs/create"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        self.logger.info(f"Creating job on {self.workspace_url}")

        try:
            response = requests.post(url, headers=headers, data=json_data)
            response.raise_for_status()
            self.logger.info(f"Job created successfully: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create job: {e}")
            return None

    def load_json_jobs(self, directory):
        """
        Process all JSON files in a given directory and create jobs.

        Parameters:
        directory (str): Path to the directory containing JSON files.

        Returns:
        None
        """
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                json_path = os.path.join(directory, filename)
                with open(json_path, "r") as file:
                    json_data = file.read()
                self.logger.info(f"Processing file: {json_path}")
                self.create_job(json_data)
