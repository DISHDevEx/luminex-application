
import sys
import boto3
import os

def stack_exists(input_stack_name):
    cf_client = boto3.client('cloudformation')

    try:
        cf_client.describe_stacks(StackName=input_stack_name)
        return True  # Stack exists
    except cf_client.exceptions.ClientError as e:
        if 'does not exist' in str(e):
            return True  # Stack does not exist
        else:
            raise  # Some other error

if __name__ == "__main__":
    # Check if the stack name is provided as a command-line argument


    input_stack_name = os.getenv(stack_name)
    print(input_stack_name)

    # AWS CloudFormation Stack Existence Check
    if stack_exists(input_stack_name):
        print(f"Stack '{input_stack_name}' exists.")
        sys.exit(0)  # Exit with success status
    else:
        print(f"Stack '{input_stack_name}' does not exist.")
        sys.exit(1)  # Exit with failure status


