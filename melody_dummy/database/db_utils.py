import os
from typing import Any, Dict, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, engine
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

    def __enter__(self) -> engine.Engine:
        if self.is_sqlite:
            db_path = self.conn_string.split("///")[1]
            if not os.path.exists(db_path) and self.db_should_exist:
                raise FileNotFoundError(f"SQLite database does not exist at {db_path}")

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