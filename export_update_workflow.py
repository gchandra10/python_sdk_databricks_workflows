import pprint
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import logging
from databricks.sdk.service.compute import DataSecurityMode
import os


class DBXWorkspaceJobs:
    """
    A class to manage and update jobs in Databricks Workspace.

    Attributes:
    -----------
    profile : str
        The profile name used for the Databricks client.

    Methods:
    --------
    change_dbr(dbr_version):
        Update the Databricks runtime version for all jobs in the workspace.

    change_default_catalog(spark_config_dict):
        Update the default Spark catalog configuration for all jobs in the workspace.

    save_jobs_as_json():
        Save the configuration of all jobs in the workspace as JSON files.
    """

    def __init__(self, profile="default"):
        """
        Initialize the DBXWorkspaceJobs class with a specific Databricks profile.

        Parameters:
        -----------
        profile : str, optional
            The profile name used for the Databricks client (default is "default").
        """
        self.profile = profile
        try:
            ## Change the profile name, default if there is no profile name
            self.wsclient = WorkspaceClient(profile=f"{profile}")
        except Exception as e:
            logging.error(f"Failed to initialize WorkspaceClient: {str(e)}")
            raise

    def change_dbr(self, dbr_version):
        """
        Update the Databricks runtime version for all jobs in the workspace.

        Parameters:
        -----------
        dbr_version : str
            The new Databricks runtime version to be applied.
        """
        try:
            ## Loop through the list of Jobs
            for job in self.wsclient.jobs.list():
                ## Get the jobid, name, owner
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name

                # Change this condition based on your requirement
                ## if job_name in ['','','']
                ## if job_name.startswith("Clone") and job_owner == "user@company.com":

                if job_name.startswith("gc-test"):
                    logging.info(
                        f"Changing DBR for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    print(
                        f"Changing DBR for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )

                    ## Read JOB Details
                    job_details = self.wsclient.jobs.get(job_id)

                    ## Pretty Print the data
                    # pprint.pprint(job_details)

                    for jc in job_details.settings.job_clusters:
                        nc = jc.new_cluster

                        ## Print the value before change
                        # pprint.pprint(jc)

                        nc.spark_version = dbr_version
                        nc.data_security_mode = DataSecurityMode.SINGLE_USER

                        ## Print the value after change
                        # pprint.pprint(jc)

                        ## Update the Databricks JOB
                        self.wsclient.jobs.update(
                            job_id=job_id,
                            new_settings=jobs.JobSettings(job_clusters=[jc]),
                        )

        except Exception as e:
            logging.error(f"Failed to change DBR for jobs: {str(e)}")
            raise

    def change_default_catalog(self, spark_config_dict):
        """
        Update the default Spark catalog configuration for all jobs in the workspace.

        Parameters:
        -----------
        spark_config_dict : dict
            A dictionary containing Spark configuration settings.
        """
        try:
            ## Loop through the list of Jobs
            for job in self.wsclient.jobs.list():
                ## Get the jobid, name, owner
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name
                # Change this condition based on your requirement

                ## if job_name in ['','','']
                ## if job_name.startswith("Clone") and job_owner == "user@company.com":

                if job_name.startswith("gc-test"):
                    logging.info(
                        f"Processing job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    print(
                        f"Processing job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )

                    ## Read JOB Details
                    job_details = self.wsclient.jobs.get(job_id)

                    ## Pretty Print the data
                    # pprint.pprint(job_details)

                    for jc in job_details.settings.job_clusters:
                        nc = jc.new_cluster

                        ## Print the value before change
                        # pprint.pprint(jc)

                        ## If spark configuration is missing in the cluster then initialize else update
                        if nc.spark_conf is None:
                            nc.spark_conf = spark_config_dict
                        else:
                            nc.spark_conf.update(spark_config_dict)

                        ## Print the value after change
                        # pprint.pprint(jc)

                        ## Update the Databricks JOB
                        self.wsclient.jobs.update(
                            job_id=job_id,
                            new_settings=jobs.JobSettings(job_clusters=[jc]),
                        )

        except Exception as e:
            logging.error(f"Failed to change default catalog for jobs: {str(e)}")
            raise

    def save_jobs_as_json(self):
        """
        Save the configuration of all jobs in the workspace as JSON files.
        """
        try:
            ## Loop through the list of Jobs
            for job in self.wsclient.jobs.list():
                ## Get the jobid, name, owner
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name

                # Change the condition based on your requirement
                if job_name.startswith("gc-test"):
                    logging.info(
                        f"Saving Config for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    print(
                        f"Saving Config for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )

                    directory = "./jobs_json"

                    # Check if the directory exists, if not, create it
                    if not os.path.exists(directory):
                        os.makedirs(directory)

                    # Define the file path
                    file_path = os.path.join(directory, f"{job_name}.json")

                    job_details_json_data = self.wsclient.jobs.get(job_id).as_dict()
                    with open(file_path, "w") as file:
                        json.dump(job_details_json_data, file, indent=4)

        except Exception as e:
            logging.error(f"Failed to save jobs as JSON: {str(e)}")
            raise


if __name__ == "__main__":
    log_directory = "./logs"

    # Check if the directory exists, if not, create it
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Define the file path
    log_file = os.path.join(log_directory, "jobs.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(name)s][%(levelname)s] %(message)s",
    )

    obj_ws_jobs = DBXWorkspaceJobs("demo-east-us2")

    ## Saves all jobs as JSON file
    obj_ws_jobs.save_jobs_as_json()

    ## Changing the default Catalog to UC catalog

    # spark_config_dict = {"spark.databricks.sql.initial.catalog.name": "gannychan"}
    # obj_ws_jobs.change_default_catalog(spark_config_dict)

    # obj_ws_jobs.change_dbr("14.3.x-scala2.12")
