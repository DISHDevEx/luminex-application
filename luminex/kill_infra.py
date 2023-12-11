"""
Terminates an Amazon EMR cluster based on user-specified time.
Sleeps until specified time before terminating EMR cluster.
"""
from datetime import datetime
import time
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def terminate_emr_cluster(cluster_id):
    """
    Terminate an EMR cluster.

    Parameters:
        cluster_id (str): The ID of the EMR cluster to terminate.
    """
    emr = boto3.client('emr')

    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        print(f"EMR cluster {cluster_id} terminated successfully.")
    except BotoCoreError as e:
        print(f"BotoCoreError terminating EMR cluster {cluster_id}: {str(e)}")
    except ClientError as e:
        print(f"ClientError terminating EMR cluster {cluster_id}: {str(e)}")

def wait_and_terminate(cluster_id, termination_time):
    """
    Sleeps until specified time before terminating EMR cluster.

    Parameters:
        cluster_id (str): The ID of the EMR cluster to terminate.
        termination_time (datetime): The time to terminate the EMR cluster.
    """
    try:
        current_time = datetime.now()
        time_difference = termination_time - current_time

        if time_difference.total_seconds() <= 0:
            raise ValueError("Termination time should be in the future.")

        print(f"EMR cluster {cluster_id} will be terminated at {termination_time}.")
        print(f"Waiting for {time_difference} seconds before termination...")

        time.sleep(time_difference.total_seconds())
        terminate_emr_cluster(cluster_id)

    except ValueError as ve:
        print(f"ValueError: {str(ve)}")
    except ConnectionError as ce:
        print(f"ConnectionError: {str(ce)}")

def get_user_input():
    """
    Get user input for the EMR cluster ID and termination time.

    Returns:
        tuple: EMR cluster ID and termination time.
    """
    try:
        cluster_id = input("Enter the EMR cluster ID: ")
        termination_time_str = input(
            "Enter the termination time (YYYY-MM-DD HH:MM:SS, "
            "press Enter to terminate immediately): ")

        if not termination_time_str:
            return cluster_id, None

        termination_time = datetime.strptime(termination_time_str, "%Y-%m-%d %H:%M:%S")
        return cluster_id, termination_time
    except ValueError:
        print("Invalid date/time format. Please use the format 'YYYY-MM-DD HH:MM:SS'.")
        return None

def run():
    """
    Execute the main functionality of the script.
    """
    cluster_id, termination_time = get_user_input()

    if cluster_id:
        if termination_time:
            wait_and_terminate(cluster_id, termination_time)
        else:
            terminate_emr_cluster(cluster_id)

if __name__ == "__main__":
    run()
