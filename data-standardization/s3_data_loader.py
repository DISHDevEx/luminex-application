import sys
import os
from io import BytesIO

import pandas as pd
import boto3
import urllib3
import pyarrow.parquet as pq
from botocore.exceptions import EndpointConnectionError

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import aws_config

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class S3DataLoader:
    def __init__(self):
        self.s3_client = self.create_s3_client()

    def create_s3_client(self):
        """
        Creates an S3 client with configured credentials.

        Returns:
        - s3_client: Configured S3 client.
        """
        s3_client = boto3.client('s3', aws_access_key_id=aws_config.aws_access_key_id,
                                 aws_secret_access_key=aws_config.aws_secret_access_key,
                                 aws_session_token=aws_config.aws_session_token,
                                 region_name='us-east-1', verify=False)
        return s3_client

    def read_csv_from_s3(self, bucket, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            csv_data = response['Body'].read()
            dataframe = pd.read_csv(BytesIO(csv_data))
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except Exception as e:
            print(f"Error reading CSV data from S3: {e}")
            return None

    def read_json_from_s3(self, bucket, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            json_data = response['Body'].read()
            dataframe = pd.read_json(BytesIO(json_data), encoding='utf-8')
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except Exception as e:
            print(f"Error reading JSON data from S3: {e}")
            return None

    def read_parquet_from_s3(self, bucket, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_data = response['Body'].read()
            table = pq.read_table(BytesIO(parquet_data))
            dataframe = table.to_pandas()
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except Exception as e:
            print(f"Error reading Parquet data from S3: {e}")
            return None

    def read_data_from_s3(self, bucket, key, file_type):
        if file_type.lower() == "csv":
            return self.read_csv_from_s3(bucket, key)
        elif file_type.lower() == "json":
            return self.read_json_from_s3(bucket, key)
        elif file_type.lower() == "parquet":
            return self.read_parquet_from_s3(bucket, key)
        else:
            print("Unsupported file type. Please choose 'csv', 'json', or 'parquet'.")
            return None

    def display_dataframe_info(self, df):
        if df is not None:
            print("\n****------DataFrame successfully created------****")
            print(f"Number of Rows: {df.shape[0]}")
            print(f"Number of Columns: {df.shape[1]}")
        else:
            print("DataFrame is None. No further information to display.")

    def main(self):
        file_type = input("Enter the file type ('csv', 'json', or 'parquet'): ")
        bucket_name = input("Enter the S3 bucket name: ")
        s3_key = input(f"Enter S3 key for the {file_type.upper()} file: ")

        df = self.read_data_from_s3(bucket_name, s3_key, file_type)
        self.display_dataframe_info(df)

if __name__ == "__main__":
    data_loader = S3DataLoader()
    data_loader.main()