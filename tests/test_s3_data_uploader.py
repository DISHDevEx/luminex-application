import pytest
import pandas as pd
from unittest.mock import patch
from ..data_standardization.s3_data_loader import S3DataLoader
from ..data_standardization.s3_json_uploader import S3DataUploader
from ..config import aws_config

@pytest.fixture
def s3_data_loader_instance():
    return S3DataLoader()

@pytest.fixture
def s3_data_uploader_instance():
    return S3DataUploader()

@patch('builtins.input', side_effect=['csv', 'test_bucket', 'test_key', 'test_file'])
def test_main_flow(mock_input, s3_data_uploader_instance):
    with patch('s3_data_loader.S3DataLoader.read_data_from_s3', return_value=pd.DataFrame()):
        with patch('builtins.print') as mock_print:
            s3_data_uploader_instance.main()

    mock_input.assert_called_with("Enter the file type (csv/json/parquet):")
    mock_print.assert_called_with('****-----Data successfully standardized to json-----****')

@patch('builtins.input', side_effect=['csv', 'test_bucket', 'test_file_without_extension', 'test_key'])
@patch('s3_data_loader.S3DataLoader.read_data_from_s3', return_value=pd.DataFrame())
@patch('s3_data_uploader.S3DataUploader.convert_df_to_json', return_value='{"test": "json"}')
@patch('s3_data_uploader.S3DataUploader.upload_json_data_to_s3')

def test_upload_json_data(mock_input, mock_read_data, mock_convert_to_json, mock_upload_to_s3, s3_data_uploader_instance):

    s3_data_uploader_instance.main()
    mock_input.assert_called_with("Enter the file type (csv/json/parquet):")
    mock_read_data.assert_called_with('test_bucket', 'test_key', 'csv')
    mock_convert_to_json.assert_called_with(pd.DataFrame())
    mock_upload_to_s3.assert_called_with('{"test": "json"}', 'test_bucket', 'test_file.json', 'test_key')

def test_s3_data_loader_create_s3_client(s3_data_loader_instance):
    s3_client = s3_data_loader_instance.create_s3_client()
    assert s3_client is not None
    assert s3_client._service_model.service_name == 's3'
