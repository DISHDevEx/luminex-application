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
        """
        Initialize the S3DataLoader instance by creating an S3 client with configured credentials.

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
        s3_client = boto3.client('s3', aws_access_key_id=aws_config.aws_access_key_id,
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
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
            print(f"Error reading Parquet data from S3: {e}")
            return None

    def read_data_from_s3(self, bucket, key, file_type):
        """
        Reads data from an S3 bucket based on the specified file type and returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - key (str): The object key for the file in the S3 bucket.
        - file_type (str): The type of file ("csv", "json", or "parquet").

        Returns:
        - dataframe: Pandas dataframe containing the file data.
        """
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
        Main function to interact with the user, read data from S3, and display DataFrame information.
        """
        file_type = input("Enter the file type ('csv', 'json', or 'parquet'): ")
        bucket_name = input("Enter the S3 bucket name: ")
        s3_key = input(f"Enter S3 key for the {file_type.upper()} file: ")

        df = self.read_data_from_s3(bucket_name, s3_key, file_type)
        self.display_dataframe_info(df)

if __name__ == "__main__":
    data_loader = S3DataLoader()
    data_loader.main()