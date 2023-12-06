from ..config import aws_config
from s3_loader import load_data_from_s3
import boto3
import json 
import urllib3

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

    def upload_df_to_s3(self,df,bucket_name,file_name):
        """
        Reads data from an S3 bucket by file type, returns a Pandas DataFrame.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - s3_key (str): The object key for the file in the S3 bucket.
        - file_name (str): Filename of the standardized data (json).

        """
        json_data = df.to_json(orient='records')
        json_bytes = str.encode(json_data)
        self.s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=json_bytes)
        print(f'DataFrame has been standardized and uploaded to S3://{bucket_name}/{file_name}')
        return json_bytes

    def display_json_data(self,json_data):
        """
        Displays success message, number of rows, number of columns.

        Parameters:
        - json_data: Json data.
        """
        if json_data is not None:
            print("\n****-----Data successfully standardized to json-----****")
        else:
            print("json_data is None. No further information to display.")

    def main(self):
        """
        Interacts with the user, reads S3 data, and displays DataFrame info.
        """
        bucket_name = input("Enter the S3 bucket name: ")
        file_name = input("Enter the file((json) name:")
        s3_key = input(f"Enter S3 key for the {file_name.upper()} file:")

        # Load data from S3 and convert it to a DataFrame
        df = load_data_from_s3(bucket_name, s3_key)
        json_data = self.upload_df_to_s3(df,bucket_name,file_name,s3_key)
        self.display_json_data(json_data)


if __name__ == "__main__":

    data_uploader = S3DataUploader()
    data_uploader.main()
