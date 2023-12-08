
import sys
import boto3
import os

def stack_exists(input_stack_name):
    """
    Checks the existence of an AWS CloudFormation stack.

    Parameters:
    - input_stack_name (str): The name of the CloudFormation stack to check.

    Returns:
    - bool: True if the stack exists, False if it does not.

    Raises:
    - botocore.exceptions.ClientError: If an unexpected error occurs during the AWS API call.
    """
    cf_client = boto3.client('cloudformation')

    try:
        cf_client.describe_stacks(StackName=input_stack_name)
        return True  # Stack exists
    except cf_client.exceptions.ClientError as e:
        if 'does not exist' in str(e):
            return False  # Stack does not exist
        else:
            raise Exception("Value error")

if __name__ == "__main__":

    input_stack_name = os.getenv('stack-name')
    print(input_stack_name)

    # AWS CloudFormation Stack Existence Check
    if stack_exists(input_stack_name):
        print(f"Stack '{input_stack_name}' exists.")
        sys.exit(1)  # Exit with success status
    else:
        print(f"Stack '{input_stack_name}' does not exist.")
        sys.exit(0)  # Exit with failure status


