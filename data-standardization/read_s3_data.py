import pandas as pd
import boto3
import urllib3
import aws_config
from botocore.exceptions import EndpointConnectionError
from io import BytesIO

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_csv_from_s3(bucket, key, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, aws_region=None):
    """
    Reads a CSV file from an S3 bucket and returns a Pandas DataFrame.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - key (str): The object key for the CSV file in the S3 bucket.
    - aws_access_key_id (str): AWS Access Key ID (optional if configured via AWS CLI or environment variables).
    - aws_secret_access_key (str): AWS Secret Access Key (optional if configured via AWS CLI or environment variables).
    - aws_session_token (str): AWS Session Token (required for temporary security credentials).
    - aws_region (str): AWS region (optional if configured via AWS CLI or environment variables).

    Returns:
    - dataframe: Pandas dataframe containing the CSV data.
    """

    try:
        # Create an S3 client with configured credentials
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                          aws_session_token=aws_session_token, region_name=aws_region, verify=False)

        # Read CSV data from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = response['Body'].read()

        # Convert CSV data to DataFrame using Pandas
        dataframe = pd.read_csv(BytesIO(csv_data))

        return dataframe

    except EndpointConnectionError as e:
        print(f"Endpoint Connection Error: {e}")
        return None
    except Exception as e:
        print(f"Error reading CSV data from S3: {e}")
        return None

def display_dataframe_info(df):
    """
    Displays success message, number of rows, number of columns, and the first few rows of the DataFrame.

    Parameters:
    - df: Pandas DataFrame.
    """
    if df is not None:
        # Display success message
        print("DataFrame successfully created:")

        # Display number of rows and columns
        print(f"Number of Rows: {df.shape[0]}")
        print(f"Number of Columns: {df.shape[1]}")

        # Display the first few rows of the DataFrame
        print("\nFirst few rows:")
        print(df.head())

def main():
    # Get user input for bucket_name and s3_key
    bucket_name = input("Enter the S3 bucket name: ")
    s3_key = input("Enter the S3 key (object key for the CSV file): ")

    # Optional: Provide AWS credentials and region if not configured via AWS CLI or environment variables
    df = read_csv_from_s3(bucket_name, s3_key,
                          aws_access_key_id=aws_config.aws_access_key_id,
                          aws_secret_access_key=aws_config.aws_secret_access_key,
                          aws_session_token=aws_config.aws_session_token,
                          aws_region='us-east-1'
                        )

    # Call the function to display DataFrame information
    display_dataframe_info(df)

if __name__ == "__main__":
    main()