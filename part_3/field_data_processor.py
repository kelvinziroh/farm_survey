"""
Data transformation pipeline: Transform data extracted to a standardized format for analysis.

**Key Functionalities:**
- **ingest_sql_data(self):** Ingest data from a SQL database and CSV file URLs
- **rename_columns(self):** Rename columns errornoursly named columns
- **apply_corrections(self, column_name, abs_column):** Apply corrections pertinent to extracted data
- **weather_station_mapping(self):** Merge weather station data to the field data
"""

from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging


class FieldDataProcessor:
    """Transform the data extracted from the database and the CSV file URL."""

    def __init__(self, config_params, logging_level="INFO"):
        """Initialize the Class attributes.
        
        Args:
            config_params: (dict): Configuration parameters to initialise the class attributes.
            logging_level: (str, optional): Implement a flexible event logging system for the class. defaults to "INFO".
        """
        self.db_path = config_params["db_path"]
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]

        # Initialize loggin in the class
        self.initialize_logging(logging_level)

        # Create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Set up logging for a class instance.
        
        Args:
            logging_level (str): Add convinient logging levels.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = (
            False  # Prevent log messages from being propagated to the root logger
        )

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.

    def ingest_sql_data(self):
        """Implement a data ingestion method."""
        self.engine = create_db_engine(self.db_path)  # Create a SQLite engine
        self.df = query_data(
            self.engine, self.sql_query
        )  # Extract the data from the database
        self.logger.info("Successfully loaded data.")

    def rename_columns(self):
        """Rename errornoursly named columns in the original dataset."""
        # Extract the columns to rename from the configuration parameters
        column1, column2 = (
            list(self.columns_to_rename.keys())[0],
            list(self.columns_to_rename.values())[0],
        )

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        # Log information on successful swap of column names
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name="Crop_type", abs_column="Elevation"):
        """Apply data specific corrections.

        Args:
            column_name (str, optional): Column to rename values. Defaults to 'Crop_type' column.
            abs_column (str, optional): Column to eliminate negative values. Defaults to 'Elevation' column.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )

    def weather_station_mapping(self):
        """Extract the weather mapping data from URL and merge it to the original dataframe."""
        # Merge the weather station data to the main DataFrame
        weather_map_df = read_from_web_CSV(self.weather_map_data)
        self.df = self.df.merge(weather_map_df, on="Field_ID", how="left")

    def process(self):
        """Call all class methods to transform the data."""
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.df.drop(columns="Unnamed: 0", inplace=True)
