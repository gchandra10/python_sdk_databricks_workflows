import pprint
from databricks.sdk.service import jobs
from databricks.sdk.service.compute import DataSecurityMode
from utilities.logger import setup_logger


class JobOperations:
    def __init__(self, wsclient):
        self.wsclient = wsclient
        self.logger = setup_logger("Job Operations")

    def change_dbr(self, dbr_version,job_name_filter=""):
        """
        Update the Databricks runtime version for all jobs in the workspace.

        Parameters:
        -----------
        dbr_version : str
            The new Databricks runtime version to be applied.
        """

        try:
            for job in self.wsclient.jobs.list():
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name
                
                if job_name.startswith(job_name_filter):
                    self.logger.info(
                        f"Changing DBR for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    pprint.pprint(
                        f"Changing DBR for job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    
                    job_details = self.wsclient.jobs.get(job_id)
                    for jc in job_details.settings.job_clusters:
                        nc = jc.new_cluster
                        nc.spark_version = dbr_version
                        nc.data_security_mode = DataSecurityMode.SINGLE_USER
                        self.wsclient.jobs.update(
                            job_id=job_id,
                            new_settings=jobs.JobSettings(job_clusters=[jc]),
                        )
        except Exception as e:
            self.logger.error(f"Failed to change DBR for jobs: {str(e)}")
            raise

    def change_default_catalog(self, spark_config_dict,job_name_filter=""):
        """
        Update the default Spark catalog configuration for all jobs in the workspace.

        Parameters:
        -----------
        spark_config_dict : dict
            A dictionary containing Spark configuration settings.
        """

        try:
            for job in self.wsclient.jobs.list():
                job_id = job.job_id
                job_name = job.settings.name
                job_owner = job.creator_user_name
                
                if job_name.startswith(job_name_filter):
                    self.logger.info(
                        f"Processing job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    pprint.pprint(
                        f"Processing job ID:{job_id}, Name: {job_name}, Owner: {job_owner}"
                    )
                    job_details = self.wsclient.jobs.get(job_id)
                    for jc in job_details.settings.job_clusters:
                        nc = jc.new_cluster
                        if nc.spark_conf is None:
                            nc.spark_conf = spark_config_dict
                        else:
                            nc.spark_conf.update(spark_config_dict)
                        self.wsclient.jobs.update(
                            job_id=job_id,
                            new_settings=jobs.JobSettings(job_clusters=[jc]),
                        )
        except Exception as e:
            self.logger.error(f"Failed to change default catalog for jobs: {str(e)}")
            raise
