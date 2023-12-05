import json
import yaml
import boto3

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
        # Get the CloudFormation template path from the configuration
        cft_path = self.config.get('cft_path', '')

        # Load the CloudFormation template from the specified YAML file
        with open(cft_path, 'r') as cft_file:
            cft_content = cft_file.read()

        # Replace specific CloudFormation intrinsic function tags with placeholder values
        placeholders = {
            '!Equals': '___REPLACE_EQUALS___',
            '!Not': '___REPLACE_NOT___',
            '!Sub': '___REPLACE_SUB___',
            '!Ref': '___REPLACE_REF___',
            '!If': '___REPLACE_IF___'
        }
        for tag, placeholder in placeholders.items():
            cft_content = cft_content.replace(tag, placeholder)

        # Load the modified YAML content using PyYAML
        cft_template = yaml.safe_load(cft_content)

        # Restore the original content by replacing placeholders with the original tags
        cft_template = self.restore_placeholders(cft_template, placeholders)

        # Validate IAM roles based on the specified permissions in the configuration
        for resource_name, resource_details in cft_template.get('Resources', {}).items():
            if resource_details.get('Type') == 'AWS::IAM::Role':
                self.validate_iam_role(resource_name, resource_details)

    def restore_placeholders(self, obj, placeholders):
        # Recursively restore placeholders with their original CloudFormation intrinsic function tags
        if isinstance(obj, dict):
            return {k: self.restore_placeholders(v, placeholders) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.restore_placeholders(item, placeholders) for item in obj]
        elif isinstance(obj, str):
            # Restore placeholder values
            for tag, placeholder in placeholders.items():
                obj = obj.replace(placeholder, tag)
            return obj
        else:
            return obj

    def validate_iam_role(self, role_name, role_details):
        # Validate IAM roles based on required permissions specified in the configuration
        permissions_config = self.config.get('permissions', {})
        required_permissions = permissions_config.get(role_name, [])

        if not required_permissions:
            print(f"Skipping validation for role '{role_name}' as no permissions are specified.")
            return

        role_arn = role_details.get('Properties', {}).get('RoleName')
        if not role_arn:
            print(f"Role '{role_name}' does not have a valid RoleName property.")
            return

        # List policies attached to the IAM role and collect the permissions
        role_policies = self.iam_client.list_role_policies(RoleName=role_name)
        role_permissions = []
        for policy_name in role_policies.get('PolicyNames', []):
            policy_document = self.iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_permissions = json.loads(policy_document['PolicyDocument']).get('Statement', [])
            role_permissions.extend(policy_permissions)

        # Compare required permissions with the actual permissions of the IAM role
        for required_permission in required_permissions:
            if required_permission not in role_permissions:
                print(f"Role '{role_name}' is missing required permission: {required_permission}")
            else:
                print(f"Role '{role_name}' has required permission: {required_permission}")

if __name__ == "__main__":
    # Create an instance of IAMRoleValidator and run the permissions validator
    permissions_validator = IAMRoleValidator('config.json')
    permissions_validator.validate_roles()
