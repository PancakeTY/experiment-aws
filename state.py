import boto3
import json
import time
import re
import subprocess
from botocore.exceptions import ClientError

# Initialize a session using Amazon Kinesis
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
stream_name = 'stateful_example'
log_group_name = "/aws/lambda/stateful_example"
file_path = "temp_stateful.txt"
output_file = "stateful_test_output.txt"

# Function to put records to the Kinesis stream
def put_to_stream_batch(stream_name, records):
    try:
        response = kinesis_client.put_records(
            StreamName=stream_name,
            Records=records
        )
        return response
    except Exception as e:
        print(f"Error putting records to stream: {e}")
        return None

def delete_log_group(log_client, log_group_name):
    try:
        log_client.delete_log_group(logGroupName=log_group_name)
        print(f"Deleted log group: {log_group_name}")
    except log_client.exceptions.ResourceNotFoundException:
        print(f"Log group {log_group_name} does not exist.")
    except ClientError as e:
        print(f"Unexpected error: {e}")
        raise e

def parse_log_file(file_path, expected_memory_size):
    report_durations = []
    execution_times = []
    start_times = []
    message_ids = set()

    with open(file_path, 'r') as file:
        for line in file:
            if "REPORT RequestId" in line:
                # Extract Duration and Memory Size for REPORT messages
                duration_match = re.search(r'Duration: ([\d.]+) ms', line)
                memory_size_match = re.search(r'Memory Size: (\d+) MB', line)

                if duration_match and memory_size_match:
                    duration = float(duration_match.group(1))
                    memory_size = int(memory_size_match.group(1))

                    # Check if memory size matches the expected value
                    if memory_size == expected_memory_size:
                        report_durations.append(duration)

            elif "Stateful_example metrics" in line:
                # Extract Start Time, Execution time, Message ID, and Memory Size
                start_time_match = re.search(r'Start Time: ([\d.]+)', line)
                execution_time_match = re.search(r'Execution time: ([\d.]+) microseconds', line)
                msg_id_match = re.search(r'Message ID: (\d+)', line)
                memory_size_match = re.search(r'Memory Size: (\d+)', line)

                if start_time_match and execution_time_match and msg_id_match and memory_size_match:
                    start_time = float(start_time_match.group(1))
                    execution_time = float(execution_time_match.group(1))
                    msg_id = msg_id_match.group(1)
                    memory_size = int(memory_size_match.group(1))

                    # Check if memory size matches the expected value
                    if memory_size == expected_memory_size:
                        start_times.append(start_time)
                        execution_times.append(execution_time)
                        message_ids.add(msg_id)
                        print(msg_id)

    # Calculate statistics
    average_duration = sum(report_durations) / len(report_durations) if report_durations else 0
    min_start_time = min(start_times) if start_times else None
    max_start_time = max(start_times) if start_times else None
    unique_message_id_count = len(message_ids)
    average_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0

    return {
        'average_duration': average_duration,
        'min_start_time': min_start_time,
        'max_start_time': max_start_time,
        'unique_message_id_count': unique_message_id_count,
        'average_execution_time': average_execution_time
    }
# Main function to write data
def main():
    memory_size = 128
    stats = parse_log_file(file_path, memory_size)
    # Print the statistics
    print(f"Memory Size: {memory_size} MB")
    print(f"Average Duration: {stats['average_duration']:.2f} ms")
    print(f"Minimum Start Time: {stats['min_start_time']}")
    print(f"Maximum Start Time: {stats['max_start_time']}")
    print(f"Unique Message ID Count: {stats['unique_message_id_count']}")
    print(f"Average Execution Time: {stats['average_execution_time']:.2f} microseconds")


if __name__ == "__main__":
    main()