import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Disable SSL/TLS-related warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class ETLFileValidator:
    def __init__(self, config_file):
        

        """
        Initialize the ETLFileValidator.

        Parameters:
        - config_file (str): Path to the JSON configuration file containing the following keys:
            - organization (str): GitHub organization or username.
            - repo_name (str): Name of the GitHub repository.
            - file_path (str): Path to the file within the GitHub repository.
            - access_token (str): GitHub access token for authentication.
        """

        with open(config_file, 'r') as f:
            config = json.load(f)
            self.organization = config.get('organization')
            self.repo_name = config.get('repo_name')
            self.file_path = config.get('file_path')
            self.access_token = config.get('access_token')

    def validate_logic_file(self):

        """
        Validate the existence of a file within a GitHub repository.

        Returns:
        - response (Response): The response object from the GitHub API request.
        
        """
        api_url = f'https://api.github.com/repos/{self.organization}/{self.repo_name}/contents/{self.file_path}'
        headers = {'Authorization': f'token {self.access_token}'}
        response = requests.get(api_url, headers=headers, verify=False)

        if response.status_code == 200:
            print(f'The file {self.file_path} exists in Repo {self.repo_name}.')
        elif response.status_code == 404:
            print(f'The file {self.file_path} does not exist in Repo {self.repo_name}.')
        else:
            print(f'Error checking file existence: {response.status_code} - {response.text}')

        return response

if __name__ == "__main__":
    # Create an instance of the ETLFileValidator class
    etl_validator = ETLFileValidator('config.json')
    
    # Run the ETL logic file validation
    etl_validator.validate_logic_file()
