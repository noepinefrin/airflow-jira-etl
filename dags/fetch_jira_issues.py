from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime, timedelta

import logging
import os

from jira.jira_fetcher import JiraFetcher
from jira.issues.issue_processor import JiraIssueProcessor
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
INITIAL_FETCH_URL = f"https://{JIRA_ATLASSIAN_DOMAIN_NAME}.atlassian.net/rest/api/3/search"
JIRA_ISSUE_SQL_TABLE_NAME = os.environ.get('JIRA_ISSUE_SQL_TABLE_NAME', 'AF_JIRA_ISSUES')
JIRA_ISSUE_SQL_TABLE_UNIQUE_COLUMN_NAME = os.environ.get('JIRA_ISSUE_SQL_TABLE_UNIQUE_COLUMN_NAME', 'issue_id')

# PostgreSQL connection should be defined in your .env file as:
# AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
CONN_ID: str = 'postgres_default'

def main():
    """
    Task to fetch and process records from the Jira Tempo API.
    """
    logger.info("Starting fetch_and_process task.")
    assert JIRA_ATLASSIAN_DOMAIN_NAME != None, "You should assign JIRA_ATLASSIAN_DOMAIN_NAME .env variable before executing this dag."
    assert JIRA_ISSUE_SQL_TABLE_NAME != None, "You should assign JIRA_ISSUE_SQL_TABLE_NAME .env variable before executing this dag."

    # Ensure your Tempo API Key is defined in the environment variables.
    # This process expects JIRA_REST_API_TOKEN to be defined in the .env file.
    # Do not add 'Bearer' at the beginning of the token.
    fetcher: JiraFetcher = JiraFetcher(token_type='Basic', token_env_var_name='JIRA_REST_API_TOKEN')

    db_operations = DatabaseOperationsFactory.get_operations(conn_id=CONN_ID)

    params: dict = {}

    record_processor: JiraIssueProcessor = JiraIssueProcessor(
        fetcher=fetcher,
        db_operations=db_operations,
        initial_url=INITIAL_FETCH_URL,
        db_table_name=JIRA_ISSUE_SQL_TABLE_NAME,
        unique_column=JIRA_ISSUE_SQL_TABLE_UNIQUE_COLUMN_NAME
    )

    record_processor.retrieve_all_records(params=params)

    logger.info("Completed fetch_and_process task.")

create_jira_tempo_table_sql: str = f"""
CREATE TABLE IF NOT EXISTS {JIRA_ISSUE_SQL_TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    issue_id INTEGER NOT NULL UNIQUE,
    issue_url TEXT,
    issue_key TEXT,
    fields_resolution_url TEXT,
    fields_resolution_id INTEGER,
    fields_resolution_description TEXT,
    fields_resolution_name TEXT,
    fields_priority_name TEXT,
    fields_labels TEXT,
    fields_assignee_url TEXT,
    fields_assignee_account_id TEXT,
    fields_assignee_displayname TEXT,
    fields_assignee_active BOOLEAN,
    fields_assignee_timezone TEXT,
    fields_assignee_accounttype TEXT,
    fields_status_url TEXT,
    fields_status_description TEXT,
    fields_status_name TEXT,
    fields_status_statusCategory_url TEXT,
    fields_status_statusCategory_key TEXT,
    fields_status_statusCategory_name TEXT,
    fields_creator_url TEXT,
    fields_creator_account_id TEXT,
    fields_creator_displayname TEXT,
    fields_creator_active BOOLEAN,
    fields_creator_timezone TEXT,
    fields_creator_accounttype TEXT,
    fields_reporter_url TEXT,
    fields_reporter_account_id TEXT,
    fields_reporter_displayname TEXT,
    fields_reporter_active BOOLEAN,
    fields_reporter_timezone TEXT,
    fields_reporter_accounttype TEXT,
    fields_progress_progress INTEGER,
    fields_progress_total INTEGER,
    fields_progress_percent INTEGER,
    fields_timespent INTEGER,
    fields_project_url TEXT,
    fields_project_id INTEGER,
    fields_project_key TEXT,
    fields_project_name TEXT,
    fields_project_projecttypekey TEXT,
    fields_summary TEXT
);
"""

# Define the DAG
with DAG('fetch_jira_rest_issues',
         default_args=default_args,
         schedule_interval=SCHEDULE_INTERVAL,
         catchup=False) as dag:

    create_jira_issue_table_task = SQLExecuteQueryOperator(
        task_id='create_jira_issue_table',
        conn_id=CONN_ID,
        sql=create_jira_tempo_table_sql
    )

    fetch_task = PythonOperator(
        task_id='fetch_issues',
        python_callable=main
    )

    # Define task dependencies
    create_jira_issue_table_task >> fetch_task

