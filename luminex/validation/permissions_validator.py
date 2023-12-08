import json
import yaml
import boto3
from botocore.exceptions import ClientError

class IAMRoleValidator:
    def __init__(self, config_path):
        # Initialize the IAMRoleValidator class with the path to the configuration file
        self.config = self.load_config(config_path)  # Load the configuration from the specified file
        self.iam_client = boto3.client('iam')  # Initialize the IAM client using boto3

    def load_config(self, config_path): 
        # Load the configuration from the specified JSON file
        with open(config_path, 'r') as config_file:
            return json.load(config_file)

    def validate_roles(self):
        # Validate IAM roles based on the specified permissions in the configuration
        permissions_config = self.config.get('permissions', {})

        for role_name, required_permissions in permissions_config.items():
            # Check if the IAM role exists
            # print(role_name)
            # print(required_permissions)
            if self.iam_role_exists(role_name):
                print(f"Role '{role_name}' exists.")

                # List policies attached to the IAM role and collect the permissions
                role_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
                role_permissions = []
                for item in role_policies.get('AttachedPolicies', []):
                    policy_name = item.get('PolicyName', '')
                    role_permissions.append(policy_name)

                # Compare required permissions with the actual permissions of the IAM role
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
