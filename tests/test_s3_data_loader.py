import pytest
import os
import sys
from pandas import DataFrame

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_standardization.s3_data_loader import S3DataLoader

# input variables
bucket_name = 'luminex'
csv_key = 'input-data/csv/sales_data_large.csv'
json_key = 'input-data/json/EmployeeData.json'
parquet_key = 'input-data/parquet/FlightsData.parquet'

@pytest.fixture
def s3_data_loader():
    return S3DataLoader()

def test_create_s3_client(s3_data_loader):
    """
    Tests the create_s3_client method, ensuring it returns a valid S3 client.

    Expected Outcome:
    - Test passes if create_s3_client returns an S3 client instance.
    """
    assert s3_data_loader.create_s3_client() is not None

def test_read_data_from_s3_supported_file_type(s3_data_loader):
    """
    Tests read_data_from_s3 with an supported file type.

    Expected Outcome:
    - Test passes if the method behaves correctly.
    """
    file_type = 'csv'
    result = s3_data_loader.read_data_from_s3(bucket_name, csv_key, file_type)

    assert isinstance(result, DataFrame)

def test_read_data_from_s3_unsupported_file_type(capsys, s3_data_loader):
    """
    Tests read_data_from_s3 with an unsupported file type.

    Expected Outcome:
    - Test passes if the method prints the correct error message.
    """
    file_type = 'unsupported_type'
    result = s3_data_loader.read_data_from_s3(bucket_name, csv_key, file_type)

    captured = capsys.readouterr()
    expected_error_message = "Unsupported file type. Choose 'csv', 'json', or 'parquet'."

    assert expected_error_message in captured.out, f"Expected: '{expected_error_message}', Actual: '{captured.out}'"
    assert result is None

def test_read_csv_from_s3(s3_data_loader):
    """
    Tests read_csv_from_s3 for reading a CSV file from an S3 bucket.

    Expected Outcome:
    - Passes if CSV file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_data_loader.read_csv_from_s3(bucket_name, csv_key), DataFrame)

def test_read_json_from_s3(s3_data_loader):
    """
    Tests read_json_from_s3 for reading a JSON file from an S3 bucket.

    Expected Outcome:
    - Passes if JSON file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_data_loader.read_json_from_s3(bucket_name, json_key), DataFrame)

def test_read_parquet_from_s3(s3_data_loader):
    """
    Tests read_parquet_from_s3 for reading a Parquet file from an S3 bucket.

    Expected Outcome:
    - Passes if Parquet file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_data_loader.read_parquet_from_s3(bucket_name, parquet_key), DataFrame)