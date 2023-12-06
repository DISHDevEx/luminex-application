import pytest
import os
import sys
import json 

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_standardization.df_json import S3DataUploader

# input variables
bucket_name = 'luminex'
json_key = 'output-data/json/EmployeeData.json'

@pytest.fixture
def s3_data_loader():
    return S3DataUploader()

def test_create_s3_client(s3_data_loader):
    """
    Tests the create_s3_client method, ensuring it returns a valid S3 client.
    Expected Outcome:
    - Test passes if create_s3_client returns an S3 client instance.
    """
    assert s3_data_loader.create_s3_client() is not None
