import os
import json
import pprint
from utilities.logger import setup_logger


class JobExporter:
    def __init__(self, wsclient):
        self.wsclient = wsclient
        self.logger = setup_logger("Job Exporter")

    def clean_job_data(self, job_data):
        job_data.pop("job_id", None)
        job_data.pop("created_time", None)
        settings = job_data.pop("settings", {})
        cleaned_data = {**job_data, **settings}
        return cleaned_data

    def save_jobs_as_json(self, job_name_filter="gc-test"):
        """
        Save the configuration of all jobs in the workspace as JSON files.
        """
        try:
            for job in self.wsclient.jobs.list():
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name
                if job_name.startswith(job_name_filter):
                    self.logger.info(
                        f"Saving Config for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    pprint.pprint(
                        f"Saving Config for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    directory = "./jobs_json"
                    if not os.path.exists(directory):
                        os.makedirs(directory)
                    file_path = os.path.join(directory, f"{job_name}.json")
                    job_details_json_data = self.wsclient.jobs.get(job_id).as_dict()
                    with open(file_path, "w") as file:
                        json.dump(
                            self.clean_job_data(job_details_json_data), file, indent=4
                        )
        except Exception as e:
            self.logger.error(f"Failed to save jobs as JSON: {str(e)}")
            raise
