from abc import ABC, abstractmethod
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

class DatabaseOperations(ABC):
    @abstractmethod
    def create_table(self, schema: str) -> None:
        pass

    @abstractmethod
    def upsert_records(self, table_name: str, records: list[dict], unique_column: str) -> None:
        pass

class PostgresOperations(DatabaseOperations):
    def __init__(self, hook: PostgresHook):
        self.hook = hook

    def create_table(self, schema: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(schema)
            conn.commit()
            logger.info("PostgreSQL table created successfully.")

    def upsert_records(self, table_name: str, records: list[dict], unique_column: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            for record in records:
                _columns = ', '.join(record.keys())
                _placeholders = ', '.join(['%s'] * len(record))
                _values = tuple(record.values())
                cursor.execute(f"""
                    INSERT INTO {table_name} ({_columns})
                    VALUES ({_placeholders})
                    ON CONFLICT ({unique_column}) DO UPDATE
                    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in record.keys() if col != unique_column])}
                """, _values)
            conn.commit()
            logger.info("Batched saved into PostgreSQL database.")

class MySqlOperations(DatabaseOperations):
    def __init__(self, hook: MySqlHook):
        self.hook = hook

    def create_table(self, schema: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(schema)
            conn.commit()
            logger.info("MySQL table created successfully.")

    def upsert_records(self, table_name: str, records: list[dict], unique_column: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            for record in records:
                _columns = ', '.join(record.keys())
                _placeholders = ', '.join(['%s'] * len(record))
                _values = tuple(record.values())
                cursor.execute(f"""
                    INSERT INTO {table_name} ({_columns})
                    VALUES ({_placeholders})
                    ON DUPLICATE KEY UPDATE
                    {', '.join([f"{col} = VALUES({col})" for col in record.keys() if col != unique_column])}
                """, _values)
            conn.commit()
            logger.info("Batched saved into MySQL database.")

class MsSqlOperations(DatabaseOperations):
    def __init__(self, hook: MsSqlHook):
        self.hook = hook

    def create_table(self, schema: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(schema)
            conn.commit()
            logger.info("MSSQL table created successfully.")

    def upsert_records(self, table_name: str, records: list[dict], unique_column: str) -> None:
        with self.hook.get_conn() as conn:
            cursor = conn.cursor()
            for record in records:
                _columns = ', '.join(record.keys())
                _placeholders = ', '.join(['%s'] * len(record))
                _values = tuple(record.values())
                cursor.execute(f"""
                    MERGE INTO {table_name} AS target
                    USING (SELECT {', '.join(['%s'] * len(record))}) AS source ({_columns})
                    ON target.{unique_column} = source.{unique_column}
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in record.keys() if col != unique_column])}
                    WHEN NOT MATCHED THEN
                        INSERT ({_columns}) VALUES ({_placeholders});
                """, _values)
            conn.commit()
            logger.info("Batched saved into MSSQL database.")

class DatabaseOperationsFactory:
    @staticmethod
    def get_operations(conn_id: str) -> DatabaseOperations:
        if conn_id.startswith('postgres'):
            return PostgresOperations(PostgresHook(conn_id))
        elif conn_id.startswith('mysql'):
            return MySqlOperations(MySqlHook(conn_id))
        elif conn_id.startswith('mssql'):
            return MsSqlOperations(MsSqlHook(conn_id))
        else:
            raise ValueError(f"Unsupported connection ID: {conn_id}, your connection must be startwith one of these postgres, mssql or mysql.")