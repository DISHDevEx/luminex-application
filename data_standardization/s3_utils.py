# s3_utils.py
import boto3
import urllib3
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import aws_config

class S3Utils:
    def __init__(self):
        """
        Initialize S3Utils with an S3 client using configured credentials.
        """
        self.s3_client = self.create_s3_client()

    def create_s3_client(self):
        """
        Creates an S3 client with configured credentials.

        Returns:
        - s3_client: Configured S3 client.
        """
        s3_client = boto3.client(
            's3', aws_access_key_id=aws_config.aws_access_key_id,
            aws_secret_access_key=aws_config.aws_secret_access_key,
            aws_session_token=aws_config.aws_session_token,
            region_name='us-east-1', verify=False)
        return s3_client
    