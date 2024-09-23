import boto3
import re

def delete_log_group(log_client, log_group_name):
    try:
        log_client.delete_log_group(logGroupName=log_group_name)
        print(f"Deleted log group: {log_group_name}")
    except log_client.exceptions.ResourceNotFoundException:
        print(f"Log group {log_group_name} does not exist.")
    except ClientError as e:
        print(f"Unexpected error: {e}")
        raise e

def parse_log_file(file_path):
    msg_stats = {}

    with open(file_path, 'r') as file:
        max_start_time = None
        min_start_time = None
        max_end_time = None
        for line in file:
            if not "custom metrics" in line:
                continue

            # Extract Start Time, Execution time, Message ID
            msg_id_match = re.search(r'Message ID: (\d+)', line)
            start_time_match = re.search(r'Start Time: ([\d.]+)', line)
            end_time_match = re.search(r'End Time: ([\d.]+)', line)
            execution_time_match = re.search(r'Execution time: ([\d.]+) microseconds', line)

            if not (msg_id_match and start_time_match and end_time_match and execution_time_match):
                continue

            msg_id = msg_id_match.group(1)
            start_time = float(start_time_match.group(1))
            end_time = float(end_time_match.group(1))
            execution_time = float(execution_time_match.group(1))

            if max_start_time is None or start_time > max_start_time:
                max_start_time = start_time
                msg_stats['max_start_time'] = max_start_time
            if min_start_time is None or start_time < min_start_time:
                min_start_time = start_time
                msg_stats['min_start_time'] = min_start_time
            if max_end_time is None or end_time > max_end_time:
                max_end_time = end_time
                msg_stats['max_end_time'] = max_end_time
                
            # Check if the replica number matches the expected value
            if msg_id not in msg_stats:
                msg_stats[msg_id] = {
                    'start_time': start_time,
                    'end_time': end_time,
                    'execution_time': execution_time
                }
            else:
                if msg_stats[msg_id]['start_time'] > start_time:
                    msg_stats[msg_id]['start_time'] = start_time
                if msg_stats[msg_id]['end_time'] < end_time:
                    msg_stats[msg_id]['end_time'] = end_time
                if msg_stats[msg_id]['execution_time'] > execution_time:
                    msg_stats[msg_id]['execution_time'] = execution_time

    return msg_stats

