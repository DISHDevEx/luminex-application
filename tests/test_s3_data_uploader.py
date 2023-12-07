import pytest
import os
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_standardization.s3_json_uploader import S3DataUploader
import pandas as pd

#input variables
bucket_name = "luminex"
file_name = "test-output-json"
s3_key = "transformed-data"


file_path = r'/Users/madhu.bandi/Downloads/transformed_sales_data.csv'

@pytest.fixture
def s3_data_uploader_instance():
    return S3DataUploader()

@pytest.fixture
def sample_dataframe(file_path):
    # Create a sample DataFrame for testing
    sample_dataframe = pd.read_csv(file_path)
    return pd.DataFrame(sample_dataframe)

def test_convert_df_to_json(s3_data_uploader_instance, sample_dataframe):
    # Test the convert_df_to_json method
    json_data = s3_data_uploader_instance.convert_df_to_json(sample_dataframe)
    assert isinstance(json_data, str)
    # Add more assertions based on your specific requirements

def test_upload_json_data_to_s3(s3_data_uploader_instance, sample_dataframe,bucket_name, file_name, s3_key):
    # Test the upload_json_data_to_s3 method
    json_data = s3_data_uploader_instance.convert_df_to_json(sample_dataframe)

    # Create an S3 bucket if it doesn't exist
    conn = s3_data_uploader_instance.s3_utils_instance.create_s3_client()
    try:
        conn.head_bucket(Bucket=bucket_name)
    except conn.exceptions.NoSuchBucket:
        conn.create_bucket(Bucket=bucket_name)

    # Mock the S3 client and other dependencies as needed for testing
    # For example, you can use a library like moto to mock AWS services

    # Call the method and assert the expected behavior
    result = s3_data_uploader_instance.upload_json_data_to_s3(json_data, bucket_name, file_name, s3_key)

    # Assert that the S3 object was created
    objects = conn.list_objects(Bucket=bucket_name)
    assert len(objects.get('Contents', [])) == 1
    assert objects['Contents'][0]['Key'] == f'{s3_key}/{file_name}'
    assert result is not None 