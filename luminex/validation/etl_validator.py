import sys
import boto3

class ETLValidator:
    def __init__(self, source_path, destination_bucket):
        # Extract source and destination bucket names from the provided paths
        self.source_path = source_path
        self.destination_bucket = destination_bucket

    def validate_input(self):
        # Validate source and destination paths
        if not self.validate_s3_path(self.source_path, "Source"):
            return False

        if not self.validate_s3_path(self.destination_bucket, "Destination"):
            return False

        # Add more input validation functions here for source key etc., if needed

        # Return True if all validations pass
        return True

    def validate_s3_path(self, s3_path, path_type):
        bucket, key = self.extract_bucket_and_key(s3_path)

        if not bucket:
            print(f"{path_type} path is missing the bucket name.")
            return False

        s3 = boto3.client('s3')

        if path_type == "Destination":
            try:
                # Check if the specified S3 bucket exists
                s3.head_bucket(Bucket=bucket)
            except Exception as e:
                print(f"{path_type} bucket not found: {e}")
                return False
        else:
            # For source path, check if the object exists
            if not key:
                print(f"{path_type} path is missing the object key.")
                return False

            try:
                # Check if the specified S3 object exists
                s3.head_object(Bucket=bucket, Key=key)
            except Exception as e:
                print(f"{path_type} path not found: {e}")
                return False

        return True

    def extract_bucket_and_key(self, s3_path):
        # Remove "s3://" prefix and split into bucket and key
        s3_path = s3_path.replace("s3://", "")
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    def run_validation(self):
        # Run the input validation
        if self.validate_input():
            print("Source and Destination ETL Validation passed.")
        else:
            print("Source and Destination Validation failed. Check the error messages for details.")