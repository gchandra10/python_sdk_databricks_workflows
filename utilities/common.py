from dotenv import load_dotenv
import os


def load_env_variables():
    """
    Loads environment variables from a .env file.

    Returns:
        tuple: A tuple containing the Databricks URL and API token.
    """
    load_dotenv()

    workspace_url = os.getenv("WORKSPACE_URL")
    token = os.getenv("TOKEN")

    if not workspace_url or not token:
        raise ValueError("WORKSPACE_URL and TOKEN must be set in the .env file.")

    return workspace_url, token
