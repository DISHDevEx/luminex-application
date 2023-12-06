"""
This module defines the S3DataLoader class,which interacts with an S3 bucket,
reads various file types (CSV, JSON, Parquet),
and displays information about the resulting Pandas DataFrame.
"""
from s3_data_loader import S3DataLoader

import sys
import os
import boto3
import urllib3
import pandas as pd

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import aws_config

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class S3DataUploader:
    def __init__(self):
        """
        Initialize S3DataUpLoader with an S3 client using configured credentials.

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
    
    def convert_df_to_json(self, df):
        """
        Converts Pandas DataFrame to JSON.

        Parameters:
        - df: Pandas DataFrame.

        Returns:
        - json_data: JSON data.
        """
        json_data = df.to_json(orient='records')
        return json_data

    def upload_json_data_to_s3(self,json_data,bucket_name,file_name,s3_key):
        """
        Reads data from an S3 bucket by file type, returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - s3_key (str): The object key for the file in the S3 bucket.
        - file_name (str): Filename of the standardized data (json).

        """
        json_bytes = str.encode(json_data)
        print("\n****-----Data successfully standardized to json-----****")
        self.s3_client.put_object(Bucket=bucket_name, Key = f'{s3_key}/{file_name}', Body=json_bytes)
        print(f'DataFrame has been standardized to json and uploaded to S3://{bucket_name}/{s3_key}/{file_name}')
        return json_bytes

    def main(self):
        """
        Interacts with the user, reads S3 data, and converts to json and displays json data info.
        """
        # Load data from S3 as DataFrame
        file_type = input("Enter the file type (csv/json/parquet):")
        bucket_name = input("Enter the S3 bucket name: ")
        s3_key = input(f"Enter S3 key for the {file_type.upper()} file: ")
        df = data_loader.read_data_from_s3(bucket_name, s3_key, file_type)

        # #local df passed for local testing 
        # file_path = r'/Users/madhu.bandi/Downloads/transformed_sales_data.csv'
        # df = pd.read_csv(file_path)
        
        print(df.info())

        #destination
        bucket_name = input("Enter the S3 bucket name: ")
        file_name_without_extension = input("Enter the file name (without extension): ")
        # Append '.json' extension to the provided file name
        file_name = file_name_without_extension + '.json'
        s3_key = input(f"Enter S3 key for the {file_name} file:")

        json_data = self.convert_df_to_json(df)
        #upload json data to s3
        self.upload_json_data_to_s3(json_data,bucket_name,file_name,s3_key)


if __name__ == "__main__":

    data_loader = S3DataLoader()
    data_uploader = S3DataUploader()
    data_uploader.main()