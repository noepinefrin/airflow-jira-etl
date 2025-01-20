from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime, timedelta

import logging
import os

from jira.jira_fetcher import JiraFetcher
from jira.worklogs.worklog_processor import JiraWorklogProcessor
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
INITIAL_FETCH_URL = "https://api.tempo.io/4/worklogs?offset=0&limit=100"
JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME = os.environ.get('JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME', None)
JIRA_TEMPO_WORKLOG_SQL_TABLE_UNIQUE_COLUMN_NAME = os.environ.get('JIRA_TEMPO_WORKLOG_SQL_TABLE_UNIQUE_COLUMN_NAME', 'tempo_worklog_id')

# PostgreSQL connection should be defined in your .env file as:
# AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
CONN_ID: str = 'postgres_default'

def main():
    """
    Task to fetch and process worklogs from the Jira Tempo API.
    """
    logger.info("Starting fetch_and_process task.")

    assert JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME != None, "You should assign JIRA_TEMPO_SQL_TABLE_NAME .env variable before executing this dag."

    # Ensure your Tempo API Key is defined in the environment variables.
    # This process expects TEMPO_API_BEARER_TOKEN to be defined in the .env file.
    # Do not add 'Bearer' at the beginning of the token.
    fetcher: JiraFetcher = JiraFetcher(
        token_type='Bearer',
        token_env_var_name='TEMPO_API_BEARER_TOKEN'
    )

    # PostgreSQL connection should be defined in your .env file as:
    # AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
    # your conn id should be start as your database type, for instance if you are using mssql then your conn_id must be start with mssql

    db_operations = DatabaseOperationsFactory.get_operations(conn_id=CONN_ID)
    # If you don't want to fetch all from the beginning, you should use parameters,
    # for instance 'from', 'projectId', or 'issueId'.
    # More information on parameters can be found here -> https://apidocs.tempo.io/#tag/Worklogs
    params: dict = {}

    worklog_processor: JiraWorklogProcessor = JiraWorklogProcessor(
        fetcher=fetcher,
        db_operations=db_operations,
        initial_url=INITIAL_FETCH_URL,
        db_table_name=JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME,
        unique_column=JIRA_TEMPO_WORKLOG_SQL_TABLE_UNIQUE_COLUMN_NAME
    )

    worklog_processor.retrieve_all_records(params=params)

    logger.info("Completed fetch_and_process task.")

create_jira_tempo_table_sql: str = f"""
    CREATE TABLE IF NOT EXISTS {JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        url TEXT,
        tempo_worklog_id INTEGER NOT NULL UNIQUE,
        issue_id INTEGER NOT NULL,
        issue_url TEXT,
        time_spent_seconds INTEGER,
        billable_seconds INTEGER,
        start_date DATE,
        start_time TIME,
        description TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        author_id TEXT,
        author_url TEXT
    );
"""

# Define the DAG
with DAG('fetch_jira_tempo_worklogs',
         default_args=default_args,
         schedule_interval=SCHEDULE_INTERVAL,
         catchup=False) as dag:

    create_jira_tempo_table_task = SQLExecuteQueryOperator(
        task_id='create_jira_tempo_table',
        conn_id=CONN_ID,
        sql=create_jira_tempo_table_sql
    )

    fetch_task = PythonOperator(
        task_id='fetch_worklogs',
        python_callable=main
    )

    # Define task dependencies
    create_jira_tempo_table_task >> fetch_task

