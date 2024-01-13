from sqlalchemy.exc import IntegrityError

from database.db_utils import DBSessionContextManager, DBEngineContextManager

# Function to insert DataFrame into the database
def insert_dataframe_to_table(df, table_name, engine, if_exists='fail'):
    """Insert a DataFrame into a database table."""
    with DBSessionContextManager(engine) as session:
        try:
            df.to_sql(
                table_name,
                con=session.bind,
                if_exists=if_exists,
                index=False
            )
        except IntegrityError as e:
            print(f"Error inserting data: {e}")
            session.rollback()


def populate_db_with_dataframes(df_dict: dict, conn_string: str):
    """Populate the database with data from a dictionary of DataFrames."""
    with DBEngineContextManager(conn_string) as engine:
        for table_name, df in df_dict.items():
            print(f"Inserting {table_name} data")
            insert_dataframe_to_table(df, table_name, engine, if_exists='append')
