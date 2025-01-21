# Jira Data Integration Pipeline
![alt text](img/worklog_example.jpeg)

A comprehensive Apache Airflow implementation for fetching and managing Jira data through multiple APIs. This project provides automated solutions for retrieving and storing Jira issues, users, and Tempo worklogs in various SQL databases (PostgreSQL, MySQL, MSSQL).

## Features

- Multiple data pipelines for:
  - Jira Issues (with detailed field mapping)
  - Jira Users (with account details)
  - Tempo Worklogs (with time tracking)
- Support for multiple SQL databases:
  - PostgreSQL
  - MySQL
  - Microsoft SQL Server
- Advanced error handling with exponential backoff
- Configurable retry mechanisms
- Efficient pagination handling
- Docker-based deployment with monitoring tools

## Prerequisites

- Docker and Docker Compose
- Jira API Access Token (Basic Auth)
- Tempo API Bearer Token
- Git (for cloning the repository)
- One of the supported databases:
  - PostgreSQL
  - MySQL
  - MSSQL

## Project Structure

```
.
├── dags/
│   ├── fetch_jira_issues.py           # DAG for Jira issues
│   ├── fetch_jira_users.py            # DAG for Jira users
│   ├── fetch_jira_tempo_worklogs.py   # DAG for Tempo worklogs
│   └── jira/
│       ├── db_ops.py                  # Database operations factory
│       ├── jira_fetcher.py            # Core API fetcher
│       ├── jira_base_processor.py     # Base processor class
│       ├── issues/
│       │   └── issue_processor.py     # Issue data processing
│       ├── users/
│       │   └── users_processor.py     # User data processing
│       └── worklogs/
│           └── worklog_processor.py   # Worklog data processing
├── docker-compose.yaml                # Docker services configuration
├── Dockerfile                         # Custom Airflow image build
├── .env                              # Environment variables
├── .dockerignore                     # Docker build exclusions
└── README.md                         # Project documentation
```

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/noepinefrin/airflow-jira-etl.git
   cd airflow-jira-etl
   ```

2. **Configure environment variables**
   Create a `.env` file:
   ```bash
   # Airflow settings
   AIRFLOW_UID=50000
   AIRFLOW_GID=0

   # Jira Configuration
   JIRA_ATLASSIAN_DOMAIN_NAME=your-domain
   JIRA_REST_API_TOKEN=your_base64_encoded_token
   TEMPO_API_BEARER_TOKEN=your_tempo_token

   # Database Settings
   JIRA_TEMPO_WORKLOG_SQL_TABLE_NAME=AF_JIRA_WORKLOGS
   JIRA_USER_SQL_TABLE_NAME=AF_JIRA_USERS
   JIRA_ISSUE_SQL_TABLE_NAME=AF_JIRA_ISSUES

   # Database Connection (Choose one based on your database)
   AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
   # AIRFLOW_CONN_MYSQL_DEFAULT=mysql://user:pass@mysql/db
   # AIRFLOW_CONN_MSSQL_DEFAULT=mssql://user:pass@mssql/db
   ```

3. **Launch services**
   ```bash
   docker compose up -d
   ```

4. **Access interfaces**
   - Airflow UI: [http://localhost:8080](http://localhost:8080)
   - Adminer: [http://localhost:8081](http://localhost:8081)

## DAG Configurations

### Common Settings for All DAGs
- Schedule: Daily
- Start Date: 2025-01-01
- Retries: 1
- Retry Delay: 5 minutes

### Jira Issues DAG
- Fetches detailed issue information
- Includes fields like:
  - Resolution details
  - Priority
  - Labels
  - Assignee information
  - Status and progress
  - Project details

### Jira Users DAG
- Retrieves user account information
- Stores:
  - Account IDs
  - Display names
  - Avatar URLs
  - Account status

### Tempo Worklogs DAG
- Captures time tracking data
- Records:
  - Time spent
  - Billable time
  - Work descriptions
  - Start dates and times

## Database Support

The project uses a factory pattern for database operations (`DatabaseOperationsFactory`), supporting:

### PostgreSQL
```python
AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://airflow:airflow@postgres/airflow
```

### MySQL
```python
AIRFLOW_CONN_MYSQL_DEFAULT=mysql://user:pass@mysql/db
```

### Microsoft SQL Server
```python
AIRFLOW_CONN_MSSQL_DEFAULT=mssql://user:pass@mssql/db
```

## Error Handling

- Retry mechanism with exponential backoff
- Configurable retry attempts (default: 5)
- Adjustable wait times:
  - Multiplier: 1
  - Minimum: 4 seconds
  - Maximum: 10 seconds

## Development

### Adding New Features
1. Create new processor in appropriate subdirectory
2. Implement `BaseProcessor` methods
3. Update database operations if needed
4. Create or modify DAG file

### Testing
```bash
# Test specific task
docker compose run airflow-worker airflow tasks test [dag_id] [task_id] [date]

# Test database connection
docker compose run airflow-worker airflow connections test [conn_id]
```

## Troubleshooting

1. **Permission Issues**
   ```bash
   sudo chown -R 50000:0 .
   ```

2. **Port Conflicts**
   ```bash
   # Check ports
   sudo lsof -i :5432
   sudo lsof -i :8080

   # Stop conflicting services
   sudo service postgresql stop
   ```

3. **Database Connection Issues**
   - Verify connection string format
   - Check database service status
   - Ensure proper credentials

4. **API Issues**
   - Verify API tokens
   - Check domain configuration
   - Review Airflow logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.