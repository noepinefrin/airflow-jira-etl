from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime, timedelta

import logging
import os

from jira.jira_fetcher import JiraFetcher
from jira.users.users_processor import JiraUsersProcessor
from jira.db_ops import DatabaseOperationsFactory

# Define constants for better readability and maintainability
SCHEDULE_INTERVAL: str = '@daily'
DAG_OWNER: str = 'Berkay Özçelik'

default_args: dict = {
    'owner': DAG_OWNER,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retry
    'depends_on_past': False  # Do not depend on past DAG runs
}

logger = logging.getLogger(__name__)

# Initial URL for the first fetch
JIRA_ATLASSIAN_DOMAIN_NAME = os.environ.get('JIRA_ATLASSIAN_DOMAIN_NAME', None)
INITIAL_FETCH_URL = f"https://{JIRA_ATLASSIAN_DOMAIN_NAME}.atlassian.net/rest/api/3/users"
JIRA_USER_SQL_TABLE_NAME = os.environ.get('JIRA_USER_SQL_TABLE_NAME', 'AF_JIRA_USERS')
JIRA_USER_SQL_TABLE_UNIQUE_COLUMN_NAME = os.environ.get('JIRA_USER_SQL_TABLE_UNIQUE_COLUMN_NAME', 'account_id')

# PostgreSQL connection should be defined in your .env file as:
# AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
CONN_ID: str = 'postgres_default'

def main():
    """
    Task to fetch and process records from the Jira Tempo API.
    """
    logger.info("Starting fetch_and_process task.")
    assert JIRA_ATLASSIAN_DOMAIN_NAME != None, "You should assign JIRA_ATLASSIAN_DOMAIN_NAME .env variable before executing this dag."
    assert JIRA_USER_SQL_TABLE_NAME != None, "You should assign JIRA_USER_SQL_TABLE_NAME .env variable before executing this dag."

    # Ensure your Tempo API Key is defined in the environment variables.
    # This process expects JIRA_REST_API_TOKEN to be defined in the .env file.
    # Do not add 'Bearer' at the beginning of the token.
    fetcher: JiraFetcher = JiraFetcher(token_type='Basic', token_env_var_name='JIRA_REST_API_TOKEN')

    # PostgreSQL connection should be defined in your .env file as:
    # AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow

    db_operations = DatabaseOperationsFactory.get_operations(conn_id=CONN_ID)

    params: dict = {}

    record_processor: JiraUsersProcessor = JiraUsersProcessor(
        fetcher=fetcher,
        db_operations=db_operations,
        initial_url=INITIAL_FETCH_URL,
        db_table_name=JIRA_USER_SQL_TABLE_NAME,
        unique_column=JIRA_USER_SQL_TABLE_UNIQUE_COLUMN_NAME
    )

    record_processor.retrieve_all_records(params=params)

    logger.info("Completed fetch_and_process task.")

create_jira_tempo_table_sql: str = f"""
CREATE TABLE IF NOT EXISTS {JIRA_USER_SQL_TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL UNIQUE,
    url TEXT,
    account_type TEXT,
    avatarUrls_avatar_url TEXT,
    display_name TEXT,
    active BOOLEAN
);
"""

# Define the DAG
with DAG('fetch_jira_rest_users',
         default_args=default_args,
         schedule_interval=SCHEDULE_INTERVAL,
         catchup=False) as dag:

    create_jira_users_table_task = SQLExecuteQueryOperator(
        task_id='create_jira_users_table',
        conn_id=CONN_ID,
        sql=create_jira_tempo_table_sql
    )

    fetch_task = PythonOperator(
        task_id='fetch_users',
        python_callable=main
    )

    # Define task dependencies
    create_jira_users_table_task >> fetch_task

