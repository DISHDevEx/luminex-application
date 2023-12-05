import pytest
import os
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import aws_config

from unittest.mock import MagicMock, patch
from data_standardization.s3_data_loader import S3DataLoader

@pytest.fixture
def s3_data_loader():
    return S3DataLoader()

@patch('data_standardization.s3_data_loader.boto3.client')
def test_create_s3_client(mock_boto3_client, s3_data_loader):
    # Mock boto3.client to return a MagicMock instance
    mock_client = MagicMock()
    mock_boto3_client.return_value = mock_client

    # Call the method you want to test
    result = s3_data_loader.create_s3_client()

    # Check whether boto3.client was called with the expected parameters
    mock_boto3_client.assert_called_once_with(
        's3',
        aws_access_key_id=aws_config.aws_access_key_id,
        aws_secret_access_key=aws_config.aws_secret_access_key,
        aws_session_token=aws_config.aws_session_token,
        region_name='us-east-1',
        verify=False
    )

    # Check if the method returns the expected client instance
    assert result == mock_client
