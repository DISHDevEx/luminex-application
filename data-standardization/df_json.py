import pandas as pd
import boto3
import urllib3
from io import StringIO
from config import aws_config

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def upload_df_to_s3(df, bucket_name, file_name):
    # Standardize DataFrame to JSON
    json_data = df.to_json(orient='records')

    # Create a StringIO object to convert the JSON data to bytes
    json_bytes = str.encode(json_data)

    # Upload to S3
    s3 = boto3.client('s3')

    # Upload to S3 bucket
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_bytes)

    print(f'DataFrame has been standardized and uploaded to S3://{bucket_name}/{file_name}')

# Example usage:
# Assume you have a DataFrame called 'my_df' and want to upload it to 'my_bucket' with the filename 'my_file.json'
upload_df_to_s3(my_df, 'my_bucket', 'my_file.json')
