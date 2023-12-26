import boto3
import json

class InputValidator:
    def __init__(self, cfg):
        # Read configuration from the provided config file
        self.config = cfg
        # Extract source and destination bucket names from the configuration file
        self.source_bucket = self.config.get('validation/source_bucket')
        self.destination_bucket = self.config.get('validation/destination_bucket')

    def validate_input(self):
        # Validate source and destination buckets
        if not self.validate_s3_bucket(self.source_bucket, "Source"):
            return False

        if not self.validate_s3_bucket(self.destination_bucket, "Destination"):
            return False

        # Add more input validation functions here for source key etc., if needed

        # Return True if all validations pass
        return True

    def validate_s3_bucket(self, bucket_name, bucket_type):
        # Initialize the S3 client
        s3 = boto3.client('s3')

        try:
            # Check if the specified S3 bucket exists
            s3.head_bucket(Bucket=bucket_name)
        except Exception as e:
            # Print an error message if the bucket is not found
            print(f"{bucket_type} bucket not found: {e}")
            return False

        # Return True if the bucket is found
        return True

    def run_validation(self):
        # Run the input validation
        if self.validate_input():
            print("Source and Destination Validation passed.")
        else:
            print("Source and Destination Validation failed. Check the error messages for details.")
