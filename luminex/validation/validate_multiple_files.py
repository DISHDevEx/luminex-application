import json
import requests
import urllib3
from urllib.parse import quote

# Suppress only the InsecureRequestWarning from urllib3 needed in this case
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ETLFileValidator:
    def __init__(self, cfg, pat, transformation_names):
        """
        Initialize the ETLFileValidator.

        Parameters:
            - cfg (dict): Configuration dictionary containing necessary information.
            - pat (str): GitHub personal access token for authentication.
            - transformation_names (list): List of transformation file names.
        """
        self.repo_name = cfg.get('etl/transformation_folder_path')
        self.access_token = pat
        self.files_to_validate = [string + ".py" for string in transformation_names]

    def validate_file(self, file_name):
        """
        Validate the existence of a file within a GitHub repository.

        Parameters:
            - file_name (str): Name of the file within the GitHub repository.

        Returns:
            - response (Response): The response object from the GitHub API request.
        """
        full_file_name = f'data-source/transformations/{file_name}'
        encoded_file_path = quote(full_file_name)
        api_url = f'https://api.github.com/repos/{self.repo_name}/contents/{encoded_file_path}'
        headers = {'Authorization': f'token {self.access_token}'}
        response = requests.get(api_url, headers=headers, verify=False)

        return response

    def validate_files(self):
        """
        Validate the existence of multiple files within a GitHub repository.

        Returns:
            - missing_files (list): List of transformation files that do not exist.
        """
        missing_files = []

        for file_name in self.files_to_validate:
            response = self.validate_file(file_name)

            if response.status_code != 200:
                missing_files.append(file_name)

        if not missing_files:
            print(f'All given transformation files exist in Repo {self.repo_name}.')
        else:
            print(f'The following transformation files do not exist in Repo {self.repo_name}: {", ".join(missing_files)}.')

        return missing_files

