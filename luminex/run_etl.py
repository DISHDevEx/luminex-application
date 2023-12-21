import os
import json
from github import Github
from github.GithubException import UnknownObjectException
import boto3
import time
# pip install PyGithub


def read_config(file_path='../config/etl_config.json'):

    """
    Returns the static parameters to run_infra from the config file.

            Parameters:
                    file_path (str): The path of the config file

            Returns:
                    config_data (dict): Represents the data in the config file
    """

    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data


# def clone_private_repo(repo_url, local_repo_path, token):
#     """
#     Clones the files needed to run the transformation, to local.
#
#             Parameters:
#                     repo_url (str): The path of the config file
#                     local_repo_path ( str): The local path where the repo can be cloned to
#                     token (str): GitHub token to get access to the Repo
#
#             Returns:
#                     local_repo_path (str): The local path where the repo has been cloned to
#     """
#     try:
#         g = Github(token, verify=False)
#         repo = g.get_repo(repo_url)
#
#         # Clone the private repository to a local directory
#         local_repo_path = local_repo_path
#         os.system(f"git clone {repo.clone_url} {local_repo_path}")
#
#         return local_repo_path
#     except UnknownObjectException as e:
#         print(f"Error: {e}")
#         raise

def clone_private_repo(repo_url, local_repo_path, token):
    """
    Clones the files needed to run the transformation, to local.

            Parameters:
                    repo_url (str): The path of the config file
                    local_repo_path (str): The local path where the repo can be cloned to
                    token (str): GitHub token to get access to the Repo

            Returns:
                    local_repo_path (str): The local path where the repo has been cloned to
    """
    try:
        g = Github(token, verify=False)
        repo = g.get_repo(repo_url)

        # Clone the private repository using SSH
        ssh_url = repo.ssh_url
        os.system(f"git clone {ssh_url} {local_repo_path}")

        return local_repo_path
    except UnknownObjectException as e:
        print(f"Error: {e}")
        raise


def submit_spark_job(aws_access_key_id, aws_secret_access_key, aws_session_token, region_name, emr_cluster_id,
                     step_name, script_s3_path, s3_input_path, s3_output_path):
    """
    Creates the EMR spark jobs(steps).

            Parameters:
                    aws_access_key_id (str): AWS Temp Credentials: Access Key ID
                    aws_secret_access_key (str): AWS Temp Credentials: Secret Access Key
                    aws_session_token (str): AWS Temp Credentials: Session Token
                    region_name (str): The aws region from where the EMR steps needs to be created
                    emr_cluster_id ( str): The emr cluster id to which the spark jobs should be added
                    step_name (str): The name for each step
                    script_s3_path (str): The s3 path to the transformation script
                    s3_input_path ( str): The s3 path to the input dataset
                    s3_output_path (str): The s3 path to store the transformed output
    """
    emr_client = boto3.client('emr', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                              aws_session_token=aws_session_token, region_name=region_name)
    response = emr_client.add_job_flow_steps(
        JobFlowId=emr_cluster_id,
        Steps=[
            {
                'Name': step_name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        script_s3_path,
                        '--input', s3_input_path,
                        '--output', s3_output_path
                    ]
                }
            }
        ]
    )

    step_id = response['StepIds'][0]

    while True:
        step_status = emr_client.describe_step(ClusterId=emr_cluster_id, StepId=step_id)['Step']['Status']['State']
        print(f'Step {step_id} status: {step_status}')

        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(20)

    print(f'Transformation executed, refer to {s3_output_path} for the transformed output.')

    return {'StepId': step_id, 'Status': step_status}


def run_etl(emr_cluster_id, pat, num_transformations, transformation_names):
    """
    Main function that triggers required functions in the required order to run the transformation on the EMR Cluster.

            Parameters:
                    emr_cluster_id (str): The emr cluster id to which the spark jobs should be added
                    pat ( str): GitHub token to get access to the Repo
                    num_transformations (int): No of transformations that needs to be performed on the dataset
                    transformation_names (list): The list of transformations
                    ENV: aws_access_key_id (str): AWS Temp Credentials: Access Key ID
                    ENV: aws_secret_access_key (str): AWS Temp Credentials: Secret Access Key
                    ENV: aws_session_token (str): AWS Temp Credentials: Session Token
    """
    # Access environment variables
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

    if not aws_access_key_id or not aws_secret_access_key or not aws_session_token:
        print("Please set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN environment variables.")
        return

    local_repo_path = None
    emr_cluster_id = emr_cluster_id
    config = read_config('../config/etl_config.json')
    github_token = pat
    region_name = config.get('aws_region', 'aws-region')
    if num_transformations == len(transformation_names):
        try:
            github_repo_url = config.get('transformation_folder_path')

            # Cloning the GitHub repository
            local_repo_path = clone_private_repo(github_repo_url, "local_transformation_repo", github_token)

            s3_input_bucket_name = config.get('s3_input_bucket_name', 'name-of-s3-bucket')

            # Initializing the S3 client
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                              aws_session_token=aws_session_token, region_name=region_name, verify=False)

            # Upload files from the transformation folder based on user input
            transformation_folder = os.path.join(local_repo_path, 'data-source/transformations')
            for name in transformation_names:
                for root, dirs, files in os.walk(transformation_folder):
                    for file in files:
                        if file.startswith(name):
                            file_path = os.path.join(root, file)
                            s3_object_key = f'scripts/transformation/{file}'
                            s3.upload_file(file_path, s3_input_bucket_name, s3_object_key)
                            print(f"Uploaded {file_path} to S3: s3://{s3_input_bucket_name}/{s3_object_key}")

            s3_bucket_input_path = config.get('s3_bucket_input_path', 'path-to-s3-bucket')
            s3_bucket_temp_output_path = config.get('s3_bucket_temp_output_path', 'path-to-temporary-s3-bucket')
            transformation_folder = config.get('transformation_folder', 'path-to-s3-transformation_folder')
            s3_bucket_final_output_path = config.get('s3_bucket_final_output_path', 'path-to-temporary-s3-bucket')
            input_folder = config.get('input_folder')
            boat = config.get('boat', 'name-for-temp-files-to-be-stored')

            # Step 1: Run transformations
            for i, transformation_script_name in enumerate(transformation_names, start=1):
                print(f'Executing {i}/{num_transformations} Transformations...')
                transformation_output_path = s3_bucket_temp_output_path + boat + \
                                             transformation_script_name + '_output/' \
                    if i != num_transformations else s3_bucket_final_output_path + \
                                                     transformation_script_name + '_output/'
                transformation_script = transformation_script_name + '.py'
                transformation_step_name = f'Luminex_' + transformation_script_name
                submit_spark_job(aws_access_key_id, aws_secret_access_key, aws_session_token,
                                 region_name, emr_cluster_id, transformation_step_name, s3_bucket_input_path +
                                 transformation_folder + transformation_script, input_folder,
                                 transformation_output_path)
                input_folder = transformation_output_path

        except Exception as e:
            print(f"Error: {e}")

        finally:
            if local_repo_path and os.path.exists(local_repo_path):
                os.system(f"rm -rf {local_repo_path}")

    else:
        print("Invalid transformation number:name combination")
