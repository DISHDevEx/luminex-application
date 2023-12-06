"""
This module defines the S3DataLoader class,which interacts with an S3 bucket,
reads various file types (CSV, JSON, Parquet),
and displays information about the resulting Pandas DataFrame.
"""
import sys
import os
import json

from io import BytesIO
from botocore.exceptions import EndpointConnectionError

import pandas as pd
import pandas.errors
import boto3
import urllib3
import pyarrow
import pyarrow.parquet as pq

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import aws_config

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class S3DataLoader:
    """
    S3DataLoader: Manages interaction with S3, reads CSV, JSON, or Parquet files,
    and displays Pandas DataFrame.

    Attributes:
        s3_client (boto3.client): S3 client with configured credentials.

    Methods:
        - __init__: Initialize S3DataLoader with configured S3 client.
        - create_s3_client: Create S3 client with configured credentials.
        - read_csv_from_s3: Read CSV file from S3 and return Pandas DataFrame.
        - read_json_from_s3: Read JSON file from S3 and return Pandas DataFrame.
        - read_parquet_from_s3: Read Parquet file from S3 and return Pandas DataFrame.
        - read_data_from_s3: Read data from S3 by file type and return Pandas DataFrame.
        - display_dataframe_info: Display success message and DataFrame details.
        - main: Interact with user, read S3 data, and display DataFrame info.
    """

    def __init__(self):
        """
        Initialize S3DataLoader with an S3 client using configured credentials.

        Parameters:
        - None
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

    def read_csv_from_s3(self, bucket, key):
        """
        Reads a CSV file from an S3 bucket and returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - key (str): The object key for the CSV file in the S3 bucket.

        Returns:
        - dataframe: Pandas dataframe containing the CSV data.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            csv_data = response['Body'].read()
            dataframe = pd.read_csv(BytesIO(csv_data))
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except (pandas.errors.EmptyDataError, pandas.errors.ParserError) as e:
            print(f"Error reading CSV data from S3: {e}")
            return None

    def read_json_from_s3(self, bucket, key):
        """
        Reads a JSON file from an S3 bucket and returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - key (str): The object key for the JSON file in the S3 bucket.

        Returns:
        - dataframe: Pandas dataframe containing the JSON data.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            json_data = response['Body'].read()
            dataframe = pd.read_json(BytesIO(json_data), encoding='utf-8')
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Error reading JSON data from S3: {e}")
            return None

    def read_parquet_from_s3(self, bucket, key):
        """
        Reads a Parquet file from an S3 bucket and returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - key (str): The object key for the Parquet file in the S3 bucket.

        Returns:
        - dataframe: Pandas dataframe containing the Parquet data.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_data = response['Body'].read()
            table = pq.read_table(BytesIO(parquet_data))
            dataframe = table.to_pandas()
            return dataframe
        except EndpointConnectionError as e:
            print(f"Endpoint Connection Error: {e}")
            return None
        except pyarrow.ArrowIOError as e:
            print(f"Error reading Parquet data from S3: {e}")
            return None

    def read_data_from_s3(self, bucket, key, file_type):
        """
        Reads data from an S3 bucket by file type, returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - key (str): The object key for the file in the S3 bucket.
        - file_type (str): The type of file ("csv", "json", or "parquet").

        Returns:
        - dataframe: Pandas dataframe containing the file data.
        """
        if file_type.lower() == "csv":
            return self.read_csv_from_s3(bucket, key)
        if file_type.lower() == "json":
            return self.read_json_from_s3(bucket, key)
        if file_type.lower() == "parquet":
            return self.read_parquet_from_s3(bucket, key)

        print("Unsupported file type. Choose 'csv', 'json', or 'parquet'.")
        return None

    def display_dataframe_info(self, df):
        """
        Displays success message, number of rows, number of columns.

        Parameters:
        - df: Pandas DataFrame.
        """
        if df is not None:
            print("\n****-----DataFrame successfully created-----****")
            print(f"Number of Rows: {df.shape[0]}")
            print(f"Number of Columns: {df.shape[1]}")
        else:
            print("DataFrame is None. No further information to display.")

    def main(self):
        """
        Interacts with the user, reads S3 data, and displays DataFrame info.
        """
        file_type = input("Enter the file type (csv/json/parquet):")
        bucket_name = input("Enter the S3 bucket name: ")
        s3_key = input(f"Enter S3 key for the {file_type.upper()} file: ")

        df = self.read_data_from_s3(bucket_name, s3_key, file_type)
        self.display_dataframe_info(df)


if __name__ == "__main__":

    data_loader = S3DataLoader()
    data_loader.main()
