# Databricks - Python SDK

## Install Databricks SDK

https://docs.databricks.com/en/dev-tools/sdk-python.html

## Databricks CLI Profile

https://docs.databricks.com/en/dev-tools/cli/profiles.html

## Python SDK Documentation

https://databricks-sdk-py.readthedocs.io/en/latest/index.html

## Github Examples

https://github.com/databricks/databricks-sdk-py/tree/main/examples


## Install Poetry

https://python-poetry.org/docs/


## Notes

### Export Update Workflows

- Add databricks-sdk

> poetry add databricks-sdk

- Update the Profile Name

> obj_ws_jobs = DBXWorkspaceJobs("databricks-profile-name")

- Update the DBR

> obj_ws_jobs.change_dbr("14.3.x-scala2.12")

- Update the Catalog Name

> spark_config_dict = {"spark.databricks.sql.initial.catalog.name": "hive_metastore"}
> obj_ws_jobs.change_default_catalog(spark_config_dict)

- Uncomment necessary Actions

### Get Databricks Runtime

- Rename **.env_template** to **.env** and replace the values.
- Install the following libraries using PIP or Poetry

> poetry add python-dotenv
> poetry add requests

#### Run

> poetry run python main.py