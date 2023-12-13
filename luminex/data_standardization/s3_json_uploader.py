from pyspark.sql import SparkSession
import sys
import os
import urllib3
import boto3
import json

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class S3DataUploader:

    def __init__(self, bucket_name=None):
        """
        Initialize S3DataLoader with an S3 client using configured credentials.

        Parameters:
        - None
        """
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
    
    def convert_df_to_json(self, df_spark):
        """
        Converts Pandas DataFrame to JSON.

        Parameters:
        - df: Pandas DataFrame.

        Returns:
        - json_data: JSON data.
        """
        json_data = df.toJSON().collect()

        # Convert the list of strings to a JSON-formatted string
        json_data = "[" + ",".join(json_data) + "]"

        return json_data
    
    def upload_json_data_to_s3(self,json_data,bucket_name,file_name,s3_key):
        """
        writes data to S3 bucket.

        Parameters:
        - json_data: JSON data in string format.
        - bucket (str): The name of the S3 bucket.
        - s3_key (str): The object key for the file in the S3 bucket.
        - file_name (str): Filename of the standardized data (json) writing to the s3.

        """
        # Encode the string to bytes using UTF-8
        bytes_data = json_data.encode('utf-8')
        print("\n****-----Data successfully standardized to json-----****")
        self.client.put_object(Body=bytes_data, Bucket=bucket_name, Key=f'{s3_key}/{file_name}')
        print(f'DataFrame has been standardized to json and uploaded to S3://{bucket_name}/{s3_key}/{file_name}')

    def main(self,df):
        """
        Interacts with the user, reads S3 data, and converts to json and displays json data info.
        """
        # #destination
        bucket_name = input("Enter the S3 bucket name: ")
        file_name = input("Enter the file name: ")
        s3_key = input(f"Enter S3 key for the {file_name} file:")
        json_data = self.convert_df_to_json(df)
        #upload json data to s3
        self.upload_json_data_to_s3(json_data,bucket_name,file_name,s3_key)
       

if __name__ == "__main__":

    spark = SparkSession.builder.appName("luminex").getOrCreate()
    file_path = r'/Users/madhu.bandi/Downloads/transformed_sales_data.csv'
    df = spark.read.csv(file_path,header=True,inferSchema=True)
    data_uploader = S3DataUploader()
    data_uploader.main(df)