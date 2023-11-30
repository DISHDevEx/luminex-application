import requests
import urllib3

# Disable SSL/TLS-related warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

organization = 'DISHDevEx'
repo_name = 'luminex-transformation'
file_path = 'data-source/transformations/main.py'
access_token = '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'

def validate_etl_logic(organization,repo_name,file_path,access_token):

    api_url = f'https://api.github.com/repos/{organization}/{repo_name}/contents/{file_path}'
    headers = {'Authorization': f'token {access_token}'}
    response = requests.get(api_url, headers=headers, verify=False)

    if response.status_code == 200:
        print(f'The file {file_path} exists in Repo{repo_name}.')
    elif response.status_code == 404:
        print(f'The file {file_path} does not exist in Repo {repo_name}.')
    else:
        print(f'Error checking file existence: {response.status_code} - {response.text}')
    return response

response = validate_etl_logic(organization,repo_name,file_path,access_token)
