import os
import sys
import pytest
import json
import boto3
import botocore
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from luminex.data_standardization.s3_json_uploader import S3DataUploader


BUCKET_NAME = 'luminex'
S3_KEY = 'transformed-data'
FILE_NAME = 'standardized-df.json'
FILE_PATH = r'/Users/madhu.bandi/Downloads/transformed_sales_data.csv'
df = pd.read_csv(FILE_PATH)

def test_convert_df_to_json(s3_uploader):
    """
    Tests converting df to json
    Expected Outcome:
    - Test passes if the method behaves correctly.
    """
    result = s3_uploader.convert_df_to_json(df)
    assert isinstance(result, str)
    assert result is not None


def test_upload_json_data_to_s3(s3_uploader):
    # Convert DataFrame to JSON (assuming convert_df_to_json returns a JSON string)
    json_data = s3_uploader.convert_df_to_json(df)

    # Call the method to upload JSON data to S3
    result = s3_uploader.upload_json_data_to_s3(json_data, BUCKET_NAME, FILE_NAME, S3_KEY)

    # Assertions
    assert result is not None  # Assuming you want to assert that the method returns a non-None value

    s3_client = boto3.client('s3')

    # Introduce a delay to allow for eventual consistency
    time.sleep(20)  # You can adjust the sleep duration based on your needs

    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=S3_KEY)
        assert response is not None
    except botocore.exceptions.ClientError as e:
        # Check for 404 error (Object not found)
        if e.response['Error']['Code'] == '404':
            assert False, f"The file {FILE_NAME} does not exist in S3 bucket {BUCKET_NAME} with key {S3_KEY}"
        else:
            # Raise the exception if it's a different error
            raise

    # Additional assertions based on the expected behavior of your method
    # Replace the above assertion with specific assertions that make sense for your use case
