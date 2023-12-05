import os
import sys
import boto3

def stack_exists(stack_name):
    cf_client = boto3.client('cloudformation')

    try:
        cf_client.describe_stacks(StackName=stack_name)
        return True  # Stack exists
    except cf_client.exceptions.ClientError as e:
        if 'does not exist' in str(e):
            return True  # Stack does not exist
        else:
            raise  # Some other error

if __name__ == "__main__":
    # Check if the stack name is provided as input from GitHub Actions
    stack_name = "luminex-cloudformation-stack"

    if not stack_name:
        print("Error: input_stack_name not set.")
        sys.exit(1)

    # AWS CloudFormation Stack Existence Check
    if stack_exists(stack_name):
        print(f"Stack '{stack_name}' exists.")
        sys.exit(0)  # Exit with success status
    else:
        print(f"Stack '{stack_name}' does not exist.")
        sys.exit(1)  # Exit with failure status


