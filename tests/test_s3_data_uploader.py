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

BUCKETNAME = os.getenv("BUCKET_NAME")
FILENAME = os.getenv("FILE_NAME")
S3KEY = os.getenv("S3_KEY")
# FILE_PATH = r'xxx'
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

    # Assuming BUCKET_NAME, FILE_NAME, and S3_KEY are defined in env
    # Convert DataFrame to JSON (assuming convert_df_to_json returns a JSON string)
    json_data = s3_uploader.convert_df_to_json(df)

    # Call the method to upload JSON data to S3
    result = s3_uploader.upload_json_data_to_s3(json_data, BUCKETNAME, FILENAME, S3KEY)

    # Assertions
    assert result is not None  # Assuming you want to assert that the method returns a non-None value
