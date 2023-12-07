"""
Test module for the S3DataLoader class.
"""
import os
import sys
from pandas import DataFrame
from dotenv import load_dotenv

load_dotenv()

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# input variables
CSV_KEY = os.getenv("CSV_KEY_PYTEST")
JSON_KEY = os.getenv("JSON_KEY_PYTEST")
PARQUET_KEY = os.getenv("PARQUET_KEY_PYTEST")

# def test_read_data_from_s3_supported_file_type(s3_loader):
#     """
#     Tests read_data_from_s3 with a supported file type.
#
#     Expected Outcome:
#     - Test passes if the method behaves correctly.
#     """
#     file_type = 'csv'
#     result = s3_loader.read_data_from_s3(key=CSV_KEY, file_type=file_type)
#
#     assert isinstance(result, DataFrame)


def test_read_data_from_s3_unsupported_file_type(capsys, s3_loader):
    """
    Tests read_data_from_s3 with an unsupported file type.

    Expected Outcome:
    - Test passes if the method prints the correct error message.
    """
    file_type = 'unsupported_type'
    result = s3_loader.read_data_from_s3(CSV_KEY, file_type)

    captured = capsys.readouterr()
    expected_error_message = "Unsupported file type. Choose 'csv', 'json', or 'parquet'."

    error_msg = f"Expected: '{expected_error_message}', Actual: '{captured.out}'"
    assert expected_error_message in captured.out, error_msg
    assert result is None


def test_read_csv_from_s3(s3_loader):
    """
    Tests read_csv_from_s3 for reading a CSV file from an S3 bucket.

    Expected Outcome:
    - Passes if CSV file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_loader.read_csv_from_s3(CSV_KEY), DataFrame)


def test_read_json_from_s3(s3_loader):
    """
    Tests read_json_from_s3 for reading a JSON file from an S3 bucket.

    Expected Outcome:
    - Passes if JSON file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_loader.read_json_from_s3(JSON_KEY), DataFrame)


def test_read_parquet_from_s3(s3_loader):
    """
    Tests read_parquet_from_s3 for reading a Parquet file from an S3 bucket.

    Expected Outcome:
    - Passes if Parquet file is read successfully and DataFrame is returned.
    """
    assert isinstance(s3_loader.read_parquet_from_s3(PARQUET_KEY), DataFrame)
