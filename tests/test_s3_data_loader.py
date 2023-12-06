"""
Test module for the S3DataLoader class.
"""
import os
import sys
import pytest
from pandas import DataFrame

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_standardization.s3_data_loader import S3DataLoader

# input variables
BUCKET_NAME = 'luminex'
CSV_KEY = 'input-data/csv/sales_data_large.csv'
JSON_KEY = 'input-data/json/EmployeeData.json'
PARQUET_KEY = 'input-data/parquet/FlightsData.parquet'

@pytest.fixture
def test_s3_loader():
    """
    Create and return an instance of the S3DataLoader class.

    Returns:
    - s3_loader (S3DataLoader): An instance of the S3DataLoader class.
    """
    return S3DataLoader()

def test_create_s3_client(test_s3_loader):
    """
    Tests the create_s3_client method, ensuring it returns a valid S3 client.

    Expected Outcome:
    - Test passes if create_s3_client returns an S3 client instance.
    """
    assert test_s3_loader.create_s3_client() is not None

def test_read_data_from_s3_supported_file_type(test_s3_loader):
    """
    Tests read_data_from_s3 with a supported file type.

    Expected Outcome:
    - Test passes if the method behaves correctly.
    """
    file_type = 'csv'
    result = test_s3_loader.read_data_from_s3(BUCKET_NAME, CSV_KEY, file_type)

    assert isinstance(result, DataFrame)

def test_read_data_from_s3_unsupported_file_type(capsys, test_s3_loader):
    """
    Tests read_data_from_s3 with an unsupported file type.

    Expected Outcome:
    - Test passes if the method prints the correct error message.
    """
    file_type = 'unsupported_type'
    result = test_s3_loader.read_data_from_s3(BUCKET_NAME, CSV_KEY, file_type)

    captured = capsys.readouterr()
    expected_error_message = "Unsupported file type. Choose 'csv', 'json', or 'parquet'."

    error_msg = f"Expected: '{expected_error_message}', Actual: '{captured.out}'"
    assert expected_error_message in captured.out, error_msg
    assert result is None

def test_read_csv_from_s3(test_s3_loader):
    """
    Tests read_csv_from_s3 for reading a CSV file from an S3 bucket.

    Expected Outcome:
    - Passes if CSV file is read successfully and DataFrame is returned.
    """
    assert isinstance(test_s3_loader.read_csv_from_s3(BUCKET_NAME, CSV_KEY), DataFrame)

def test_read_json_from_s3(test_s3_loader):
    """
    Tests read_json_from_s3 for reading a JSON file from an S3 bucket.

    Expected Outcome:
    - Passes if JSON file is read successfully and DataFrame is returned.
    """
    assert isinstance(test_s3_loader.read_json_from_s3(BUCKET_NAME, JSON_KEY), DataFrame)

def test_read_parquet_from_s3(test_s3_loader):
    """
    Tests read_parquet_from_s3 for reading a Parquet file from an S3 bucket.

    Expected Outcome:
    - Passes if Parquet file is read successfully and DataFrame is returned.
    """
    assert isinstance(test_s3_loader.read_parquet_from_s3(BUCKET_NAME, PARQUET_KEY), DataFrame)
