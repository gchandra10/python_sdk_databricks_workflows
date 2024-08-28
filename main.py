from dbx_workspace_client import DBXWorkspaceClient
from job_operations import JobOperations
from job_exporter import JobExporter
from utilities.logger import setup_logger
from utilities.common import load_env_variables
from dbx_runtime_versions import DBXRuntime
from job_creator import JobCreator

if __name__ == "__main__":
    logger = setup_logger("Main Program")

    # Initialize the Databricks Workspace Client
    ws_client_wrapper = DBXWorkspaceClient("demo-east-us2")
    ws_client = ws_client_wrapper.get_client()

    # Get DBX Runtime Versions

    try:
        workspace_url, token = load_env_variables()
        logger.info(f"Retrieving DBX Runtime from given Workspace {workspace_url}.")
    except ValueError as e:
        logger.error(f"Error loading environment variables: {e}")
        print(f"Error loading environment variables: {e}")

    ## Get DBR Runtime
    
    obj_runtimes = DBXRuntime(workspace_url, token)
    runtimes = obj_runtimes.get_databricks_runtimes()

    if runtimes is None:
        logger.error("Failed to retrieve Databricks runtimes.")
        print("Failed to retrieve Databricks runtimes.")

    sorted_runtimes = sorted(runtimes, key=lambda x: x["key"], reverse=True)

    # if sorted_runtimes:
    #     for version in sorted_runtimes:
    #         print(version)
    # else:
    #     logger.error("No runtimes found.")
    #     print("No runtimes found.")

    ###########

    # Save all jobs as JSON files
    job_exporter = JobExporter(ws_client)
    # job_exporter.save_jobs_as_json("gc-test")

    # Perform job operations
    job_ops = JobOperations(ws_client)

    # Change the default Catalog to Unity Catalog
    # spark_config_dict = {"spark.databricks.sql.initial.catalog.name": "hive_metastore"}
    # job_ops.change_default_catalog(spark_config_dict,"gc-test")

    # Change the Databricks Runtime version for all jobs
    # job_ops.change_dbr("14.3.x-scala2.12","gc-test")

    ####

    job_creator = JobCreator(workspace_url, token)
    jobs_json_path = "./jobs_json"
    job_creator.load_json_jobs(jobs_json_path)
