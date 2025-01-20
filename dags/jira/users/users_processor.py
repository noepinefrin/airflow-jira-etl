from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from jira.jira_fetcher import JiraFetcher
from jira.jira_base_processor import BaseProcessor
from jira.db_ops import DatabaseOperations

logger = logging.getLogger(__name__)

class JiraUsersProcessor(BaseProcessor):

    # If you going to update this user mapping, you should also update the "create_jira_tempo_table_sql" variable in dag part.
    MAPPING: dict = {
        'self': 'url',
        'accountId': 'account_id',
        'accountType': 'account_type',
        'avatarUrls': {
            '48x48': 'avatar_url'
        },
        'displayName': 'display_name',
        'active': 'active'
    }

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
        super().__init__(
            fetcher=fetcher,
            db_operations=db_operations,
            initial_url=initial_url,
            db_table_name=db_table_name,
            unique_column=unique_column
        )

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
        Retrieve all users from the Jira REST API V3, managing pagination.

        Args:
            params (dict | None): Optional parameters for the API request.

        Returns:
            None: This function does not return a value. It processes and saves users directly to the database.
        """
        batch_count: int = 0  # Counter for the number of batches processed

        logger.info(f"--- Processing Batch Number: {batch_count} ---")

        # Retrieve users from the current URL
        response: dict | None = self.fetcher.fetch(self.url, params)

        # Log the initiation of data mapping
        logger.info("Initiating data mapping.")

        # Transform the fetched data into the required format
        mapped_data: list[dict] = [
            self.map_response_to_database_format(
                record,
                self.MAPPING
            )
            for record in response
        ]

        # Log successful completion of data mapping
        logger.info("Data mapping completed successfully.")
        # Persist the transformed users into the database
        self.persist_records_to_database(records=mapped_data)

        # Log the successful completion of the batch processing
        logger.info("--- Batch processed successfully. ---")
        # Update the startAt for subsequent fetch attempts

        batch_count += 1  # Increment the batch counter

        logger.info("All users have been successfully saved.")