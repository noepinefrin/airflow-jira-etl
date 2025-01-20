from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from jira.jira_fetcher import JiraFetcher
from jira.db_ops import DatabaseOperations

logger = logging.getLogger(__name__)

class BaseProcessor:

    # If you going to update this worklog mapping, you should also update the sql table schema in dag part.
    MAPPING: dict = {}

    STOP_AFTER_ATTEMPT: int = 5
    WAIT_EXPONENTIAL_MULTIPLIER: int = 1
    WAIT_EXPONENTIAL_MIN: int = 4
    WAIT_EXPONENTIAL_MAX: int = 10

    def __init__(
            self,
            fetcher: JiraFetcher,
            db_operations: DatabaseOperations,
            initial_url: str,
            db_table_name: str,
            unique_column: str
        ) -> None:
        """
        Initialize the WorklogFetcher with the specified components.

        Args:
            fetcher (JiraFetcher): Instance for retrieving data from the Jira API.
            postgres_hook (PostgresHook): Hook for executing database operations.
            initial_url (str): The starting URL for fetching worklogs.
        """
        # Assign the fetcher, PostgreSQL hook, and initial URL for subsequent operations
        self.fetcher = fetcher
        self.db_operations = db_operations
        self.url = initial_url
        self.db_table_name = db_table_name
        self.unique_column = unique_column

    @retry(
        stop=stop_after_attempt(STOP_AFTER_ATTEMPT),
        wait=wait_exponential(
            multiplier=WAIT_EXPONENTIAL_MULTIPLIER,
            min=WAIT_EXPONENTIAL_MIN,
            max=WAIT_EXPONENTIAL_MAX
        )
    )
    def retrieve_all_records(
            self,
            params: dict | None = None
        ) -> None:
        """
        Retrieve all records from the Jira Tempo API, managing pagination.

        Args:
            params (dict | None): Optional parameters for the API request.

        Returns:
            None: This function does not return a value. It processes and saves records directly to the database.
        """
        raise NotImplementedError('You should implement your fetch logic.')

    def map_response_to_database_format(
            self,
            record: dict,
            mapping: dict,
            parent_key: str = '',
            join_char: str = '//'
        ) -> dict:
        """
        Map the response data from the API to the desired format based on the provided mapping.

        Args:
            data (dict): The raw data from the API response that needs to be transformed.
            mapping (dict): A dictionary defining how to map the raw data fields to the desired output format.
            parent_key (str, optional): The key prefix for nested mappings. Defaults to an empty string.

        Returns:
            dict: A dictionary containing the transformed data according to the specified mapping.
        """

        result = {}

        for key, value in record.items():
            # Get the corresponding mapped key from the mapping
            mapped_key = mapping.get(key)

            if not mapped_key:
                continue

            # If the current mapping is a dictionary, recurse
            if isinstance(mapped_key, dict) and isinstance(value, dict):
                nested_result = self.map_response_to_database_format(value, mapped_key, f"{parent_key}_{key}" if parent_key else key)
                result.update(nested_result)

            # If the value is a list, join the elements with //
            elif isinstance(value, list):
                result[f"{parent_key}_{mapped_key}" if parent_key else mapped_key] = f'{join_char}'.join(map(str, value))

            # If it's a direct mapping, apply it
            elif isinstance(mapped_key, str):
                result[f"{parent_key}_{mapped_key}" if parent_key else mapped_key] = value

        return result

    def create_table(self, schema: str) -> None:
        self.db_operations.create_table(schema)

    def persist_records_to_database(self, records: list[dict]) -> None:
        self.db_operations.upsert_records(self.db_table_name, records, self.unique_column)