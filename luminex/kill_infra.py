"""
This module defines a StackManager class for managing AWS CloudFormation stacks.
It includes methods for checking the existence of a stack, deleting a stack, and
running the stack deletion process.
"""
import sys
import time

import boto3
import botocore.exceptions

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
                print(f"Stack '{input_stack_name}' exists...")
            return exists

        except self.cloudformation.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']

            if error_code == 'ValidationError' and 'does not exist' in error_message:
                print(f"Stack '{input_stack_name}' does not exist...")
                return False

            print(f"Error: {str(e)}")
            return False

    def delete_stack(self, input_stack_name):
        """
        Delete a CloudFormation stack.

        Args:
            input_stack_name (str): The name of the stack.
        """
        try:
            # Check if EMR cluster is running
            emr_cluster_id = self.get_emr_cluster_id(input_stack_name)
            emr_cluster_id = emr_cluster_id.strip()

            if emr_cluster_id:
                emr_cluster_status = self.get_emr_cluster_status(emr_cluster_id)
                print(f"EMR cluster with ID '{emr_cluster_id}' is in {emr_cluster_status} state...")

                if emr_cluster_status in ['RUNNING']:
                    user_input = input(
                        "The cluster is still running. "
                        "Do you still want to delete the stack? (yes/no): "
                    ).lower()
                    if user_input != 'yes':
                        print("Stack deletion aborted...")
                        return

            # Delete the CloudFormation stack
            self.cloudformation.delete_stack(StackName=input_stack_name)
            print(f"Stack deletion initiated. Stack Name: {input_stack_name}...")

            waiter = self.cloudformation.get_waiter('stack_delete_complete')
            waiter.wait(StackName=input_stack_name)

            time.sleep(5)

            # Check if the stack deletion is successful
            if self.is_stack_deleted(input_stack_name):
                print(f"Stack {input_stack_name} deleted successfully...")
            else:
                print(f"Failed to delete stack {input_stack_name}. "
                    "Check the CloudFormation console for more details."
)
        except botocore.exceptions.WaiterError as e:
            print(f"Error waiting for stack deletion: {e}")

        except botocore.exceptions.ClientError as e:
            print(f"Error deleting stack {input_stack_name}: {e}")

    def get_emr_cluster_status(self, emr_cluster_id):
        """
        Get the status of an EMR cluster.

        Args:
            emr_cluster_id (str): The ID of the EMR cluster.

        Returns:
            str: EMR cluster status.
        """
        try:
            emr = boto3.client('emr')
            response = emr.describe_cluster(ClusterId=emr_cluster_id)
            if ('Cluster' in response and
                'Status' in response['Cluster'] and
                'State' in response['Cluster']['Status']):
                cluster_status = response['Cluster']['Status']['State']
                return cluster_status
        except botocore.exceptions.ClientError as e:
            print(f"Error getting EMR cluster status: {e}")

        return 'unknown'


    def get_emr_cluster_id(self, stack_name):
        """
        Get the EMR cluster ID from the CloudFormation stack outputs.

        Args:
            stack_name (str): The name of the stack.

        Returns:
            str: EMR cluster ID if found, otherwise None.
        """
        try:
            response = self.cloudformation.describe_stacks(StackName=stack_name)
            outputs = response['Stacks'][0].get('Outputs', [])

            for output in outputs:
                if output['OutputKey'] == 'EMRClusterIdOutput':
                    return output['OutputValue']

        except self.cloudformation.exceptions.ClientError as e:
            print(f"Error getting EMR cluster ID: {e}")

        return None

    def is_stack_deleted(self, stack_name):
        """
        Check if a CloudFormation stack is deleted.

        Args:
            stack_name (str): The name of the stack.

        Returns:
            bool: True if the stack is deleted, False otherwise.
        """
        deleted_stacks = self.get_deleted_stacks()
        return stack_name in deleted_stacks

    def get_deleted_stacks(self):
        """
        Get the list of deleted CloudFormation stacks.

        Returns:
            list: List of deleted stack names.
        """
        deleted_stacks = []

        try:
            response = self.cloudformation.list_stacks(StackStatusFilter=['DELETE_COMPLETE'])
            deleted_stacks = [stack['StackName'] for stack in response.get('StackSummaries', [])]

        except botocore.exceptions.ClientError as e:
            print(f"Error retrieving deleted stacks: {e}")

        return deleted_stacks

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
