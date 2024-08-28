from databricks.sdk import WorkspaceClient
from utilities.logger import setup_logger


class DBXWorkspaceClient:
    """
    A class to manage and update jobs in Databricks Workspace.

    Attributes:
    -----------
    profile : str
        The profile name used for the Databricks client.
    """

    def __init__(self, profile="default"):
        self.profile = profile
        try:
            self.wsclient = WorkspaceClient(profile=f"{profile}")
        except Exception as e:
            logger = setup_logger("DBXWorkspaceClient")
            logger.error(f"Failed to initialize WorkspaceClient: {str(e)}")
            raise

    def get_client(self):
        return self.wsclient
