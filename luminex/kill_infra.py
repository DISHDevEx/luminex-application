"""
This module defines a StackManager class for managing AWS CloudFormation stacks.
It includes methods for checking the existence of a stack, deleting a stack, and
running the stack deletion process.
"""
import sys
import time
import boto3
from botocore.exceptions import ClientError

class StackManager:
    """
    A class for managing AWS CloudFormation stacks.
    """
    def __init__(self):
        """
        Initializes the StackManager with an AWS CloudFormation client.
        """
        self.cloudformation = boto3.client('cloudformation')

    def stack_exists(self, input_stack_name):
        """
        Check if a CloudFormation stack exists.

        Args:
            stack_name (str): The name of the stack.

        Returns:
            bool: True if the stack exists, False otherwise.
        """
        try:
            response = self.cloudformation.describe_stacks(StackName=input_stack_name)
            exists = len(response['Stacks']) > 0
            if exists:
                print(f"Stack '{input_stack_name}' exists.")
            return exists

        except self.cloudformation.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']

            if error_code == 'ValidationError' and 'does not exist' in error_message:
                print(f"Stack '{input_stack_name}' does not exist.")
                return False

            print(f"Error: {str(e)}")
            return False

    def delete_stack(self, input_stack_name):
        """
        Delete a CloudFormation stack.

        Args:
            stack_name (str): The name of the stack.
        """
        try:
            self.cloudformation.delete_stack(StackName=input_stack_name)
            print(f"Stack deletion initiated. Stack Name: {input_stack_name}")

            waiter = self.cloudformation.get_waiter('stack_delete_complete')
            waiter.wait(StackName=input_stack_name)

            time.sleep(5)

            if not self.stack_exists(input_stack_name):
                print(f"Stack '{input_stack_name}' deleted successfully.")
            else:
                print(f"Stack deletion failed. Stack '{input_stack_name}' still exists.")

        except ClientError as e:
            print(f"Error: {str(e)}")

    def run(self):
        """
        Run the stack deletion process.
        """
        if len(sys.argv) != 2:
            print("Usage: python delete_stack.py <stack_name>")
            sys.exit(1)
        stack_name = sys.argv[1]

        if self.stack_exists(stack_name):
            self.delete_stack(stack_name)

if __name__ == "__main__":

    stack_manager = StackManager()
    stack_manager.run()
