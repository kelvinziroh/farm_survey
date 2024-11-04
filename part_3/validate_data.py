import os
import pandas as pd
import pytest

# Define file paths for weather and field data CSVs
WEATHER_CSV_PATH = 'sampled_weather_df.csv'
FIELD_CSV_PATH = 'sampled_field_df.csv'

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    """Fixture to read test data from CSV files."""
    # Check if the weather and field CSV files exist
    if not os.path.exists(WEATHER_CSV_PATH):
        pytest.fail(f"{WEATHER_CSV_PATH} does not exist.")
    if not os.path.exists(FIELD_CSV_PATH):
        pytest.fail(f"{FIELD_CSV_PATH} does not exist.")
    
    # Read the data from CSVs
    weather_df = pd.read_csv(WEATHER_CSV_PATH)
    field_df = pd.read_csv(FIELD_CSV_PATH)

    # Provide DataFrames to tests
    yield weather_df, field_df

    # Teardown: Delete CSV files after tests
    if os.path.exists(WEATHER_CSV_PATH):
        os.remove(WEATHER_CSV_PATH)
        print(f"Deleted {WEATHER_CSV_PATH}")
    if os.path.exists(FIELD_CSV_PATH):
        os.remove(FIELD_CSV_PATH)
        print(f"Deleted {FIELD_CSV_PATH}")

def test_read_weather_DataFrame_shape(setup_and_teardown):
    """Test to validate the shape of the weather DataFrame."""
    weather_df, _ = setup_and_teardown
    assert weather_df.shape[0] > 0, "Weather DataFrame should have at least one row."

def test_read_field_DataFrame_shape(setup_and_teardown):
    """Test to validate the shape of the field DataFrame."""
    _, field_df = setup_and_teardown
    assert field_df.shape[0] > 0, "Field DataFrame should have at least one row."

def test_weather_DataFrame_columns(setup_and_teardown):
    """Test to validate the expected columns in the weather DataFrame."""
    weather_df, _ = setup_and_teardown
    expected_columns = {'Weather_station_ID', 'Message', 'Measurement','Value'}
    assert expected_columns.issubset(weather_df.columns), "Weather DataFrame does not contain expected columns."

def test_field_DataFrame_columns(setup_and_teardown):
    """Test to validate the expected columns in the field DataFrame."""
    _, field_df = setup_and_teardown
    expected_columns = {
    'Field_ID', 
    'Elevation', 
    'Latitude', 
    'Longitude', 
    'Location', 
    'Slope', 
    'Rainfall', 
    'Min_temperature_C', 
    'Max_temperature_C', 
    'Ave_temps', 
    'Soil_fertility', 
    'Soil_type', 
    'pH', 
    'Pollution_level', 
    'Plot_size', 
    'Annual_yield', 
    'Crop_type', 
    'Standard_yield', 
    'Weather_station'
}

    assert expected_columns.issubset(field_df.columns), "Field DataFrame does not contain expected columns."

def test_field_DataFrame_non_negative_elevation(setup_and_teardown):
    """Test to ensure that elevation values in field DataFrame are non-negative."""
    _, field_df = setup_and_teardown
    assert (field_df['Elevation'] >= 0).all(), "Elevation values must be non-negative."

def test_crop_types_are_valid(setup_and_teardown):
    """Test to validate that crop types in the field DataFrame are within expected values."""
    _, field_df = setup_and_teardown
    valid_crop_types = {'cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice',
       'maize', 'wheat ', 'tea ', 'cassava '}
    assert field_df['Crop_type'].isin(valid_crop_types).all(), "Field DataFrame contains invalid crop types."

def test_positive_rainfall_values(setup_and_teardown):
    """Test to ensure that rainfall values in weather DataFrame are non-negative."""
    field_df, _ = setup_and_teardown
    
    # Print columns to confirm `Rainfall` is in this specific DataFrame instance
    print("Columns in field_df:", field_df.columns)

    # Check if 'Rainfall' column exists
    assert "Rainfall" in field_df.columns, "Rainfall column is missing from field_df."

    # Check if all values are non-negative
    assert field_df["Rainfall"].notnull().all(), "Rainfall column contains NaN values."
    assert (field_df["Rainfall"] >= 0).all(), "Rainfall values must be non-negative."





# Add more tests as needed for your specific data validation
