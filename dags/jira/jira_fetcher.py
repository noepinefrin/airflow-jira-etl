from typing import Literal
import requests
import logging
import os

logger = logging.getLogger(__name__)

class JiraFetcher:
    def __init__(self, token_type: Literal['Bearer', 'Basic'], token_env_var_name: str):
        """
        Initialize the JiraTempoFetcher with the bearer token from environment variables.
        """
        self.token_type = token_type
        self.token = os.getenv(token_env_var_name)
        if not self.token:
            raise EnvironmentError(f"{token_env_var_name} environment variable is not set")
        logger.info(f"JiraFetcher initialized with {token_type} token.")

    def fetch(self, url: str, params: dict | None = None):
        """
        Fetch worklogs from the given URL using the bearer token for authentication.

        Args:
            url (str): The API endpoint to fetch data from.

        Returns:
            dict: The JSON response from the API.
        """
        headers = {
            'Authorization': f'{self.token_type} {self.token}',
            'Content-Type': 'application/json'
        }
        logger.info("Fetching data from URL: %s", url)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        logger.info("Data fetched successfully from URL: %s", url)
        return response.json()
