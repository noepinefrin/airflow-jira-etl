from airflow.providers.postgres.hooks.postgres import PostgresHook

from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from jira.jira_fetcher import JiraFetcher
from jira.jira_base_processor import BaseProcessor
from jira.db_ops import DatabaseOperations


logger = logging.getLogger(__name__)

class JiraIssueProcessor(BaseProcessor):

    # If you going to update this worklog mapping, you should also update the "create_jira_tempo_table_sql" variable in dag part.
    MAPPING: dict = {
        'id': 'issue_id',
        'self': 'issue_url',
        'key': 'issue_key',
        'fields': {
            'resolution': {
                'self': 'url',
                'id': 'id',
                'description': 'description',
                'name': 'name'
            },
            'priority': {
                'name': 'name'
            },
            'labels': 'labels',
            'assignee': {
                'self': 'url',
                'accountId': 'account_id',
                'displayName': 'displayname',
                'active': 'active',
                'timeZone': 'timezone',
                'accountType': 'accounttype'
            },
            'status': {
                'self': 'url',
                'description': 'description',
                'name': 'name',
                'statusCategory': {
                    'self': 'url',
                    'key': 'key',
                    'name': 'name'
                }
            },
            'creator': {
                'self': 'url',
                'accountId': 'account_id',
                'displayName': 'displayname',
                'active': 'active',
                'timeZone': 'timezone',
                'accountType': 'accounttype'
            },
            'reporter': {
                'self': 'url',
                'accountId': 'account_id',
                'displayName': 'displayname',
                'active': 'active',
                'timeZone': 'timezone',
                'accountType': 'accounttype'
            },
            'progress': {
                'progress': 'progress',
                'total': 'total',
                'percent': 'percent'
            },
            'timespent': 'timespent',
            'project': {
                'self': 'url',
                'id': 'id',
                'key': 'key',
                'name': 'name',
                'projectTypeKey': 'projecttypekey'
            },
            'summary': 'summary'

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
        self.startAt: int = 0
        self.total: int = 0

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

        while self.startAt <= self.total:  # Continue fetching while there is a subsequent URL
            logger.info(f"--- Processing Batch Number: {batch_count} ---")

            params.update(
                {
                    'startAt': self.startAt,
                }
            )

            # Retrieve worklogs from the current URL
            response: dict | None = self.fetcher.fetch(self.url, params)

            if self.startAt == 0:
                self.total = response['total']

            # Log the initiation of data mapping
            logger.info("Initiating data mapping.")

            # Transform the fetched data into the required format
            mapped_data: list[dict] = [
                self.map_response_to_database_format(
                    record,
                    self.MAPPING
                )
                for record in response['issues']
            ]

            # Log successful completion of data mapping
            logger.info("Data mapping completed successfully.")
            # Persist the transformed worklogs into the database
            self.persist_records_to_database(records=mapped_data)

            # Log the successful completion of the batch processing
            logger.info("--- Batch processed successfully. ---")
            # Update the startAt for subsequent fetch attempts
            self.startAt += response['maxResults']

            batch_count += 1  # Increment the batch counter

        logger.info("All worklogs have been successfully saved.")