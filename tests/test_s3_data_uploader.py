import os
import sys
import pytest
import json
import pandas as pd


# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from luminex.data_standardization.s3_json_uploader import S3DataUploader

##local df passed for local testing 
file_path = r'/Users/madhu.bandi/Downloads/transformed_sales_data.csv'
df = pd.read_csv(file_path)

@pytest.fixture
def s3_uploader():
    return S3DataUploader()

def test_convert_df_to_json(s3_uploader,df):
    """
    Tests converting df to json
    Expected Outcome:
    - Test passes if the method behaves correctly.
    """
    result = s3_uploader.convert_df_to_json(df)
    assert isinstance(result, dict)
    assert result is not None


def test_upload_json_data_to_s3(s3_uploader, capsys):

    #input variables
    result = {"key": "value"}
    bucket_name = 'luminex'
    s3_key='transformed-data'
    file_name = 'sales-transformed'

    # Call the method
    result = s3_uploader.upload_json_data_to_s3(json.dumps(result), bucket_name, file_name, s3_key)

    # Assertions
    assert result is not None  # Assuming you want to assert that the method returns a non-None value

    # Verify console output
    captured = capsys.readouterr()
    expected_output = "Data successfully standardized to json"
    assert expected_output in captured.out

    # Additional assertions based on the expected behavior of your method
    assert "put_object" in dir(s3_uploader.s3_utils_instance.create_s3_client())  # Replace with specific assertions