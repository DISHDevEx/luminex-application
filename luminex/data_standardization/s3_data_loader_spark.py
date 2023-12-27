"""
This module defines the S3DataLoader class,which interacts with an S3 bucket,
reads various file types (CSV, JSON, Parquet) resulting PySpark DataFrame.
"""
import os
import sys
import subprocess
from pyspark.sql import SparkSession

# # Add the parent directory to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
# from config import aws_config

# get repo root level
root_path = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
).stdout.rstrip("\n")
# add repo path to use all libraries
sys.path.append(root_path)

from configs import Config

cfg = Config('../configs/config.yaml')

class S3DataLoader:
    """
    This class Manages interaction with S3, reads CSV, JSON, or Parquet files,
    and converts data to a PySpark dataframe.
    """
    def __init__(self):
        """
        Create a Spark session
        Parameters:
            self
        Returns:
            None
        """
        self.spark = SparkSession.builder\
                .appName("S3ReadExample")\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")\
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                .config("spark.hadoop.fs.s3a.access.key", cfg.get('aws/access_key_id')) \
                .config("spark.hadoop.fs.s3a.secret.key", cfg.get('aws/secret_access_key')) \
                .config("spark.hadoop.fs.s3a.session.token", cfg.get('aws/session_token'))\
                .getOrCreate()

    def read_csv_to_df(self, bucket_folder_path):
        """
        Reads CSV data from an S3 bucket folder path into a Spark DataFrame.

        Parameters:
            bucket_folder_path (str): The S3 bucket folder path containing the CSV files.

        Returns:
            df: A Spark DataFrame representing the CSV data.
        """
        df = self.spark.read.csv(bucket_folder_path, header=True, inferSchema=True)
        self.spark.stop()

        return df

    def read_json_to_df(self, bucket_folder_path):
        """
        Reads data from a JSON file into a Spark DataFrame.

        Parameters:
            bucket_folder_path (str): The file path of the JSON file.

        Returns:
            df: A Spark DataFrame representing the JSON data.
        """
        df = self.spark.read.json(bucket_folder_path, multiLine=True)
        self.spark.stop()

        return df

    def read_parquet_to_df(self, bucket_folder_path):
        """
        Reads data from a Parquet file into a Spark DataFrame.

        Parameters:
            bucket_folder_path (str): The file path of the Parquet file.

        Returns:
            df: A Spark DataFrame representing the Parquet data.
        """
        df = self.spark.read.parquet(bucket_folder_path)
        self.spark.stop()

        return df

    @staticmethod
    def process_s3_data(bucket_folder_path):
        """
        Processes data in an S3 bucket based on the file extension.

        Parameters:
            bucket_folder_path (str): The S3 bucket folder path containing the data files.

        Returns:
            None
        """
        s3_data_loader = S3DataLoader()

        # Choose the appropriate method based on the file extension
        if bucket_folder_path.lower().endswith(".json"):
            s3_data_loader.read_json_to_df(bucket_folder_path)
        if bucket_folder_path.lower().endswith(".csv"):
            s3_data_loader.read_csv_to_df(bucket_folder_path)
        if bucket_folder_path.lower().endswith(".parquet"):
            s3_data_loader.read_parquet_to_df(bucket_folder_path)

def main():
    """
    Main function to initiate the S3 data processing.
    """
    bucket_folder_path = sys.argv[1]
    S3DataLoader.process_s3_data(bucket_folder_path)

if __name__ == '__main__':
    main()
