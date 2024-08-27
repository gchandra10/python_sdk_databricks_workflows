import requests
import os
from dotenv import load_dotenv

def load_env_variables():
    """
    Loads environment variables from a .env file.

    Returns:
        tuple: A tuple containing the Databricks URL and API token.
    """
    load_dotenv()
    databricks_url = os.getenv("DATABRICKS_URL")
    api_token = os.getenv("API_TOKEN")
    
    if not databricks_url or not api_token:
        raise ValueError("DATABRICKS_URL and API_TOKEN must be set in the .env file.")
    
    return databricks_url, api_token

def get_databricks_runtimes(databricks_url, api_token):
    """
    Fetches the list of available Databricks runtimes.

    Args:
        databricks_url (str): The URL of the Databricks workspace.
        api_token (str): The API token for authentication.

    Returns:
        list: A list of available Databricks runtimes, or None if an error occurs.
    """
    try:
        url = f"{databricks_url}/api/2.0/clusters/spark-versions"
        headers = {"Authorization": f"Bearer {api_token}"}
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get('versions', [])
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Databricks runtimes: {e}")
        return None

# def filter_runtimes(runtimes, scala_version="scala2.12", runtime_version="14.3.x"):
#     """
#     Filters the Databricks runtimes by Scala version and runtime version.

#     Args:
#         runtimes (list): The list of available runtimes.
#         scala_version (str): The Scala version to filter by. Default is "scala2.12".
#         runtime_version (str): The runtime version to filter by. Default is "14.3.x".

#     Returns:
#         list: A list of filtered runtime keys.
#     """
#     if not runtimes:
#         return []
    
#     return [
#         runtime['key'] for runtime in runtimes
#         if scala_version in runtime['key'] and runtime_version in runtime['key']
#     ]

def main():
    """
    Main function to get and print the filtered Databricks runtimes.
    """
    try:
        databricks_url, api_token = load_env_variables()
    except ValueError as e:
        print(f"Error loading environment variables: {e}")
        return

    runtimes = get_databricks_runtimes(databricks_url, api_token)
            
    if runtimes is None:
        print("Failed to retrieve Databricks runtimes.")
        return
    
    #filtered_versions = filter_runtimes(runtimes)
    filtered_versions = sorted(runtimes, key=lambda x: x['key'], reverse=True)
    
    if filtered_versions:
        for version in filtered_versions:
            print(version)
    else:
        print("No runtimes found matching the criteria.")

if __name__ == "__main__":
    main()
