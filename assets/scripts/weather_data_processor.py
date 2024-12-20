"""
Weather data transformation pipeline: Extract and transform weather data extracted to a standardized format for analysis.

**Key Functionalities:**
- **weather_station_mapping(self):** Extract weather station data from the CSV file URL
- **extract_measurement(self):** Extract the measurement values from text using regex
- **process_messages(self, column_name, abs_column):** Extract the measurement and their appropriate value.
- **calculate_means(self):** Aggregate the average weather measurements by weather station
"""

from data_ingestion import read_from_web_CSV
import re
import logging


class WeatherDataProcessor:
    """Extract weather data from its URL and transform it."""

    def __init__(
        self, config_params, logging_level="INFO"):  
        """Initiate the weather data extracted from the URL and other attributes.

        Args:
            config_params (_type_): Configuration parameters to initialise weather CSV file URL and regex patterns.
            logging_level (str, optional): Implement a flexible event logging system for the class. Defaults to "INFO".
        """
        self.weather_station_data = config_params["weather_csv_path"]
        self.patterns = config_params["regex_patterns"]
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """Set up logging for a class instance.

        Args:
            logging_level (str): Add convenient logging levels.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = (
            False  # Prevents log messages from being propagated to the root logger
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

    def weather_station_mapping(self):
        """Load weather station data from the web URL."""
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.")
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    def extract_measurement(self, message):
        """Extract measurements from text.

        Args:
            message (str): Text that contains the measurement.

        Returns:
            float, None: Measurement value, None if the text does not contain numerical value.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """Extract the measurement and their appropriate value.

        Returns:
            pandas.core.frame.DataFrame: The weather DataFrame object.
        """
        if self.weather_df is not None:
            result = self.weather_df["Message"].apply(self.extract_measurement)
            self.weather_df["Measurement"], self.weather_df["Value"] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning(
                "weather_df is not initialized, skipping message processing."
            )
        return self.weather_df

    def calculate_means(self):
        """Aggregate the average weather measurements by weather stations.

        Returns:
            pandas.core.frame.DataFrame: Average of measurements per weather stations
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=["Weather_station_ID", "Measurement"])[
                "Value"
            ].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning(
                "weather_df is not initialized, cannot calculate means."
            )
            return None

    def process(self):
        """Call all class methods to transfrom the weather station data."""
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")
