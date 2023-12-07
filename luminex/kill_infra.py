import boto3
from datetime import datetime

def terminate_emr_cluster(emr_client, cluster_id):
    try:
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        print(f"Cluster {cluster_id} terminated successfully.")
    except Exception as e:
        print(f"Error terminating cluster {cluster_id}: {str(e)}")

def get_user_input():
    cluster_id = input("Enter the EMR cluster ID to terminate: ")
    termination_time_str = input("Enter the termination time (in format YYYY-MM-DD HH:MM:SS): ")

    try:
        termination_time = datetime.strptime(termination_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print("Invalid date format. Please use the format YYYY-MM-DD HH:MM:SS.")
        return None, None

    return cluster_id, termination_time

if __name__ == "__main__":
    # Assuming you have AWS credentials properly configured
    emr_client = boto3.client("emr", region_name="your-region")

    cluster_id, termination_time = get_user_input()

    if cluster_id and termination_time:
        current_time = datetime.now()
        time_difference = termination_time - current_time

        if time_difference.total_seconds() > 0:
            print(f"Cluster termination scheduled at {termination_time}.")
            print(f"Waiting for {time_difference.total_seconds()} seconds...")
            emr_client.get_waiter('cluster_terminated').wait(
                ClusterId=cluster_id,
                WaiterConfig={'Delay': 30, 'MaxAttempts': int(time_difference.total_seconds() / 30)}
            )
            terminate_emr_cluster(emr_client, cluster_id)
        else:
            print("Invalid termination time. It should be a future time.")
