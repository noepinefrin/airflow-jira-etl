from airflow.providers.postgres.hooks.postgres import PostgresHook

from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from jira.jira_fetcher import JiraFetcher
from jira.jira_base_processor import BaseProcessor
from jira.db_ops import DatabaseOperations


logger = logging.getLogger(__name__)

class JiraWorklogProcessor(BaseProcessor):

    # If you going to update this worklog mapping, you should also update the "create_jira_tempo_table_sql" variable in dag part.
    MAPPING: dict = {
        'self': 'url',
        'tempoWorklogId': 'tempo_worklog_id',
        'issue': {
            'id': 'id',
            'self': 'url'
        },
        'timeSpentSeconds': 'time_spent_seconds',
        'billableSeconds': 'billable_seconds',
        'startDate': 'start_date',
        'startTime': 'start_time',
        'description': 'description',
        'createdAt': 'created_at',
        'updatedAt': 'updated_at',
        'author': {
            'accountId': 'id', # It turns into author_url after mapped.
            'self': 'url' # author_url
        }
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
        Retrieve all worklogs from the Jira Tempo API, managing pagination.

        Args:
            params (dict | None): Optional parameters for the API request.

        Returns:
            None: This function does not return a value. It processes and saves worklogs directly to the database.
        """
        batch_count: int = 0  # Counter for the number of batches processed

        while self.url:  # Continue fetching while there is a subsequent URL
            logger.info(f"--- Processing Batch Number: {batch_count} ---")

            # Retrieve worklogs from the current URL
            response: dict | None = self.fetcher.fetch(self.url, params)

            # Log the initiation of data mapping
            logger.info("Initiating data mapping.")

            # Transform the fetched data into the required format
            mapped_data: list[dict] = [
                self.map_response_to_database_format(
                    record,
                    self.MAPPING
                )
                for record in response['results']
            ]

            # Log successful completion of data mapping
            logger.info("Data mapping completed successfully.")
            # Persist the transformed worklogs into the database
            self.persist_records_to_database(records=mapped_data)

            # Log the successful completion of the batch processing
            logger.info("--- Batch processed successfully. ---")
            # Update the next URL for subsequent fetch attempts
            self.url: str = response['metadata'].get('next', None)

            batch_count += 1  # Increment the batch counter

        logger.info("All worklogs have been successfully saved.")