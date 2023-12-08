"""
S3 Data Uploader

This script defines a class, `S3DataUploader`, responsible for uploading a Pandas DataFrame to an Amazon S3 bucket. The script uses a separate S3 client creation function from `s3_utils` and includes functionality to convert the DataFrame to JSON before uploading it to the specified S3 bucket.

- Importing necessary modules:
  - `S3DataLoader`: A class for reading data from S3 buckets.
  - `create_s3_client`: A function for creating an S3 client using configuration details.
  - Other standard libraries: `sys`, `os`, `urllib3`, and `pandas`.

- Setting up the Python path:
  The script adds the parent directory to the Python path to import modules from the parent directory.

- Reading a local CSV file into a Pandas DataFrame:
  The script reads a CSV file from a local path into a Pandas DataFrame (`df`). This local DataFrame will be used for testing and uploading to S3.

- Class Definition: `S3DataUploader`:
  - `s3_client`: A class-level attribute storing the S3 client instance created using `create_s3_client`.
  - `convert_df_to_json`: A method to convert a Pandas DataFrame to JSON format.
  - `upload_json_data_to_s3`: A method to upload JSON data to the specified S3 bucket and key.
  - `main`: The main method that interacts with the user, reads data, converts it to JSON, and uploads it to S3.

- Execution Block:
  The script creates an instance of `S3DataUploader` and calls its `main` method, initiating the interaction with the user for S3 bucket details and uploading the local DataFrame in JSON format to the specified location in the S3 bucket.

Note: The commented-out section loading data from S3 using `S3DataLoader` is preserved for reference and can be used for fetching data from an S3 bucket instead of reading a local file.
"""
import sys
import os
import urllib3

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from s3_utils import S3Utils

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class S3DataUploader:

    def __init__(self):
        """
        Initialize S3DataUploader with an S3 client using configured credentials.

        Parameters:
        - None
        """
        self.s3_utils_instance = S3Utils()
    
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
        writes data to S3 bucket.

        Parameters:
        - json_data: JSON data.
        - bucket (str): The name of the S3 bucket.
        - s3_key (str): The object key for the file in the S3 bucket.
        - file_name (str): Filename of the standardized data (json) writing to the s3.

        """
        json_bytes = str.encode(json_data)
        s3_client = self.s3_utils_instance.create_s3_client()

        print("\n****-----Data successfully standardized to json-----****")
        s3_client.put_object(Bucket=bucket_name, Key = f'{s3_key}/{file_name}', Body=json_bytes)
        print(f'DataFrame has been standardized to json and uploaded to S3://{bucket_name}/{s3_key}/{file_name}')
        return json_bytes

    def main(self):
        """
        Interacts with the user, reads S3 data, and converts to json and displays json data info.
        """
        
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

    data_uploader = S3DataUploader()
    data_uploader.main()