from pathlib import Path

from database.setup import create_database
from database.populate import populate_db_with_dataframes
from database.db_utils import DBEngineContextManager, execute_raw_sql_file, execute_raw_sql
from data_processing.dummy_data import create_dummy_dataframes
from utils import load_config, get_db_connection_string

config = load_config()
use_dummy_data = config['ingestion_pipeline']['use_dummy_data']

if not use_dummy_data:
    try:
        from data_processing.data_ingestion_pipeline import run_data_ingestion
    except ImportError as e:
        print("Warning: data_ingestion_pipeline not found, Defaulting to dummy data")
        use_dummy_data = True


def initialise_database():
    """Initialise the database with data."""

    conn_string = get_db_connection_string()
    overwrite_db = config['database']['overwrite']
    create_views = config['database']['create_views']

    # Create database and tables
    create_database(conn_string, overwrite=overwrite_db)

    if use_dummy_data:
        # Generate dummy data
        n_patients = config['dummy_data']['n_patients']
        start_date = config['dummy_data']['start_date']
        end_date = config['dummy_data']['end_date']

        df_dict = create_dummy_dataframes(
            n_patients, start_date, end_date
        )
    else:
        df_dict = run_data_ingestion()

    # Populate database with dummy data
    populate_db_with_dataframes(df_dict, conn_string)

    # Run all views
    if create_views:
        run_all_view_files(conn_string)


def run_all_view_files(conn_string: str, views_dir: Path = Path("views")):
    """Run all .sql files in the views directory."""
    if views_dir.exists():
        print(f"\nRunning views in '{views_dir}' directory")
        with DBEngineContextManager(conn_string) as engine:
            for sql_file_path in views_dir.iterdir():
                if sql_file_path.suffix == ".sql":
                    print(f" -- DROPPING VIEW IF EXISTS {sql_file_path.stem}")
                    execute_raw_sql(engine, f"DROP VIEW IF EXISTS {sql_file_path.stem}")
                    try:
                        print(f" -- Executing {sql_file_path.name}")
                        execute_raw_sql_file(engine, str(sql_file_path))
                    except Exception as e:
                        print(f"Error running {sql_file_path.name}: {e}")
    else:
        print("No views directory found; skipping view creation.")


# Run the initialization
if __name__ == "__main__":
    initialise_database()
