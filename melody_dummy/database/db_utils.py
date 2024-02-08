import os
from typing import Any, Dict, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, engine, text
from sqlalchemy.orm import sessionmaker, Session


class DBEngineContextManager:
    """
    Context manager for creating and disposing a SQLAlchemy engine.

    Attributes:
        conn_string (str): The connection string for the database.
    """

    def __init__(self, conn_string: str, db_should_exist: bool = True):
        self.conn_string = conn_string
        self.is_sqlite = "sqlite" in conn_string
        self.db_should_exist = db_should_exist
        self.engine: Optional[engine.Engine] = None

    @property
    def exists(self) -> bool:
        if self.is_sqlite:
            db_path = self.conn_string.split("///")[1]
            return os.path.exists(db_path)
        # Add logic for other DB types if needed
        else:
            raise ValueError("Unable to check if DB exists. Currently No integrations with other DB types.")

    def __enter__(self) -> engine.Engine:
        if not self.exists and self.db_should_exist:
            raise FileNotFoundError(f"Database does not exist at {self.conn_string}")

        self.engine = create_engine(self.conn_string)
        return self.engine

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        if self.engine:
            self.engine.dispose()


class DBSessionContextManager:
    """
    Context manager for creating and managing a SQLAlchemy session.

    Attributes:
        engine (engine.Engine): SQLAlchemy engine instance.
    """

    def __init__(self, engine: engine.Engine):
        self.session_factory = sessionmaker(bind=engine)
        self.session: Optional[Session] = None

    def __enter__(self) -> Session:
        self.session = self.session_factory()
        return self.session

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        if self.session:
            if exc_type:
                self.session.rollback()
            else:
                self.session.commit()
            self.session.close()


def prevent_write_in_sql_string(sql_string: str) -> None:
    """
    Raises an error if the SQL string contains INSERT, UPDATE, or DELETE statements.

    Args:
        sql_string (str): The SQL query string.
    """
    prohibited_operations = ["INSERT", "UPDATE", "DELETE"]
    for op in prohibited_operations:
        if op in sql_string:
            raise ValueError(f"{op} statements are not allowed in this function.")


def sql_read_to_pandas(
        sql_statement: str, conn_string: str,
        params: Optional[Union[Dict[str, Any], tuple]] = None,
) -> pd.DataFrame:
    """
    Executes a SQL statement and returns the results as a pandas DataFrame.

    Args:
        sql_statement (str): SQL query as a string.
        conn_string (str): Database connection string.
        params (Optional[Union[Dict[str, Any], tuple]]): Parameters to substitute into the query.
        allow_insert (bool): If False, prevents executing INSERT, UPDATE, and DELETE statements.

    Returns:
        pd.DataFrame: DataFrame containing the query results.
    """
    prevent_write_in_sql_string(sql_statement)

    with DBEngineContextManager(conn_string) as engine:
        with engine.connect() as connection:
            result = pd.read_sql_query(sql_statement, connection, params=params)

    return result


def execute_raw_sql_file(conn_or_engine: Union[str, engine.Engine], sql_file_path: str):
    """
    Executes a SQL file against a database using SQLAlchemy.
    It can accept either an existing engine instance or a connection string.

    Parameters:
    - conn_or_engine: Union[str, sa_engine.Engine]. A database connection string or an existing SQLAlchemy engine.
    - sql_file_path: str. The path to the .sql file containing SQL commands to be executed.
    """

    # Read the SQL command from the file
    with open(sql_file_path, 'r') as file:
        sql_command = file.read()

    # Execute the SQL command
    execute_raw_sql(conn_or_engine, sql_command)


def execute_raw_sql(conn_or_engine: Union[str, engine.Engine], sql_command: str):
    """
    Executes a raw SQL command against a database using SQLAlchemy.
    :param conn_or_engine:  A database connection string or an existing SQLAlchemy engine.
    :param sql_command:  The SQL command to be executed.
    :return:
    """
    # Wrap the SQL command with text() for explicit execution as raw SQL
    sql_command = text(sql_command)
    # Check if conn_or_engine is a connection string
    if isinstance(conn_or_engine, str):
        with DBEngineContextManager(conn_or_engine) as DB_engine:
            with DBSessionContextManager(DB_engine) as session:
                session.execute(sql_command)
    # Check if conn_or_engine is an SQLAlchemy Engine instance
    elif isinstance(conn_or_engine, engine.Engine):
        with DBSessionContextManager(conn_or_engine) as DB_session:
            DB_session.execute(sql_command)
    else:
        raise TypeError("conn_or_engine must be either a connection string or an SQLAlchemy Engine instance.")
