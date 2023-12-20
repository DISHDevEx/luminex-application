import json
import requests
import urllib3
from urllib.parse import quote

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ETLFileValidator:
    def __init__(self, config_file):
        """
        Initialize the ETLFileValidator.

        Parameters:
            - config_file (str): Path to the JSON configuration file containing the following keys:
              - organization (str): GitHub organization or username.
              - repo_name (str): Name of the GitHub repository.
              - access_token (str): GitHub access token for authentication.
              - files_to_validate (list): List of file names within the GitHub repository.
        """
        with open(config_file, 'r') as f:
            config = json.load(f)
            self.organization = config.get('organization')
            self.repo_name = config.get('repo_name')
            self.access_token = config.get('access_token')
            self.files_to_validate = config.get('files_to_validate', [])

    def validate_file(self, file_name):
        """
        Validate the existence of a file within a GitHub repository.

        Parameters:
            - file_name (str): Name of the file within the GitHub repository.

        Returns:
            - response (Response): The response object from the GitHub API request.
        """
        # Try with the full path
        # Add prefix to the file name
        full_file_name = f'data-source/transformations/{file_name}'
        # Try with the full path
        encoded_file_path = quote(full_file_name)
        api_url = f'https://api.github.com/repos/{self.organization}/{self.repo_name}/contents/{encoded_file_path}'
        headers = {'Authorization': f'token {self.access_token}'}
        response = requests.get(api_url, headers=headers, verify=False)

        if response.status_code == 200:
            print(f'The file {file_name} exists in Repo {self.repo_name}.')
            return response

        # Retry with just the file name in the root directory
        api_url = f'https://api.github.com/repos/{self.organization}/{self.repo_name}/data-source/transformations/{quote(file_name)}'
        response = requests.get(api_url, headers=headers, verify=False)

        if response.status_code == 200:
            print(f'The file {file_name} exists in Repo {self.repo_name}.')
        elif response.status_code == 404:
            print(f'The file {file_name} does not exist in Repo {self.repo_name}.')
        else:
            print(f'Error checking file existence for {file_name}: {response.status_code} - {response.text}')

        return response

    def validate_files(self):
        """
        Validate the existence of multiple files within a GitHub repository.

        Returns:
            - responses (list): List of response objects from the GitHub API requests.
        """
        responses = []
        for file_name in self.files_to_validate:
            response = self.validate_file(file_name)
            responses.append(response)

        return responses

