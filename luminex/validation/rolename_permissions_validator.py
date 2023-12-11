import json
import boto3
from botocore.exceptions import ClientError
import re

class IAMRoleValidator:
    def __init__(self, config_path):
        # Initialize IAMRoleValidator class with the path to the configuration file
        self.config = self.load_config(config_path)  # Load configuration from the specified file
        self.iam_client = boto3.client('iam')  # Initialize IAM client using boto3

    def load_config(self, config_path):
        # Load configuration from the specified JSON file
        with open(config_path, 'r') as config_file:
            return json.load(config_file)

    def is_valid_role_name_format(self, role_name):
        # Check if the role name adheres to the specified format
        # The role name should start with "StackSet" or "EMR" and end with "Role"
        pattern = re.compile(r'^(StackSet|EMR)[a-zA-Z0-9_]+Role$')
        return bool(re.match(pattern, role_name))

    def validate_roles(self):
        # Validate IAM roles based on the specified permissions in the configuration
        permissions_config = self.config.get('permissions', {})

        for role_name, required_permissions in permissions_config.items():
            if self.iam_role_exists(role_name):
                print(f"Role '{role_name}' exists.")

                # Check if the role name follows the specified format
                if self.is_valid_role_name_format(role_name):
                    print(f"Role '{role_name}' follows the specified naming format.")
                else:
                    print(f"Role '{role_name}' does not follow the specified naming format.")

                # List policies attached to the IAM role and collect permissions
                role_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
                role_permissions = [item.get('PolicyName', '') for item in role_policies.get('AttachedPolicies', [])]

                # Compare required permissions with actual permissions of the IAM role
                for required_permission in required_permissions:
                    if required_permission not in role_permissions:
                        print(f"Role '{role_name}' is missing required permission: {required_permission}")
                    else:
                        print(f"Role '{role_name}' has required permission: {required_permission}")
            else:
                print(f"Role '{role_name}' does not exist. Skipping validation.")

    def iam_role_exists(self, role_name):
        # Check if the IAM role exists
        try:
            self.iam_client.get_role(RoleName=role_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                return False
            else:
                raise

if __name__ == "__main__":
    # Create an instance of IAMRoleValidator and run the permissions validator
    permissions_validator = IAMRoleValidator('config.json')
    permissions_validator.validate_roles()
