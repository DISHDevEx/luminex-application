import requests
import json
import os

def read_config(file_path='../config/infra_config.json'):
    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

def trigger_workflow(organization, repository, workflow_name, event_type, token, inputs=None):
    url = f'https://api.github.com/repos/{organization}/{repository}/dispatches'
    print(url)
    headers = {
        'Accept': 'application/vnd.github.everest-preview+json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    payload = {
        'event_type': event_type,
        'client_payload': {
            'workflow': workflow_name,
            'inputs': inputs or {},
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    print(payload)

    print(f'Response status code: {response.status_code}')
    print(f'Response content: {response.text}')

    if response.status_code == 204:
        print(f'Success! Triggered workflow "{workflow_name}" in repository "{organization}/{repository}".')
    else:
        print(f'Failed to trigger workflow.')

    return response.status_code

def run_infra(pat, stack_name, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN ):
    config = read_config('../config/infra_config.json')
    organization_name = config.get('GITHUB_ORGANIZATION', 'your-organization')
    repository_name = config.get('GITHUB_REPOSITORY', 'your-username/your-repo')
    workflow_name = config.get('GITHUB_WORKFLOW', 'name-of-your-workflow')
    event_type = config.get('GITHUB_EVENT_TYPE', 'type-of-the-github-workflow-event')
    personal_access_token = pat


    workflow_inputs = {
        'stack-name': stack_name,
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_SESSION_TOKEN': AWS_SESSION_TOKEN
    }

    trigger_workflow(organization_name, repository_name, workflow_name, event_type, personal_access_token, workflow_inputs)

