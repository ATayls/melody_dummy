from pathlib import Path

import yaml

# Path to the config file
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'

def load_config():
    """Loads the config file and returns the config dictionary."""

    # Check if config file exists
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as file:
        config = yaml.safe_load(file)

    return config

def get_db_connection_string(filename_override=None):
    """
    Creates the database connection string from the config file.
    The database file is assumed to be in the same folder as the config file.
    """

    if filename_override:
        db_filename = filename_override
    else:
        config = load_config()
        db_filename = config['database']['db_filename']
    config_folder = CONFIG_PATH.parent
    conn_string = f"sqlite:///{config_folder}/{db_filename}"

    return conn_string
