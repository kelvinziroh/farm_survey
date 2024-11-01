"""
This module provides functionalities for interacting with a database and importing CSV data.

**Key Functionalities:**
- **create_db_engine(db_path):** Creates a SQLAlchemy engine to connect to the database
- **query_data(engine, sql_query):** Retrieves data from the database and returns it as a pandas DataFrame
- **read_from_web_CSV(URL):** Imports a CSV file from a URL and returns it as a pandas DataFrame
"""

# Library importation
from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger("data_ingestion")
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Set the db path
db_path = 'sqlite:///../assets/mn_farm_survey_small.db'

# Set the SQL query for data extraction from the database
sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

def create_db_engine(db_path):
    """Create a db engine

    Args:
        db_path (str): Path to the serverless database file

    Raises:
        e: ImportError if SQLAlchemy is not installed in runtime environment
        e: Any other Exception if database engine creation is unsuccessful

    Returns:
        sqlalchemy.engine.base.Engine: SQLAlchemy engine object
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except (
        ImportError
    ):  # If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error(
            "SQLAlchemy is required to use this function. Please install it first."
        )
        raise e
    except Exception as e:  # If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """Retrieve data from the database and return a pandas DataFrame

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object
        sql_query (str): SQL query to interact with the database

    Raises:
        ValueError: ValueError if the SQL query returns and empty DataFrame
        e: ValueError if there was a problem with execution of the SQL query
        e: Any other Exception that occured while querying the database

    Returns:
        pandas.core.frame.DataFrame: pandas DataFrame object
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e


def read_from_web_CSV(URL):
    """Import CSV file from URL and return a pandas DataFrame

    Args:
        URL (str): URL of the CSV file

    Raises:
        e: pd.errors.EmptyDataError if the DataFrame object returned is empty
        e: Any other Exception if failed to read CSV from the web

    Returns:
        pandas.core.frame.DataFrame: pandas DataFrame object
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error(
            "The URL does not point to a valid CSV file. Please check the URL and try again."
        )
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
