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
# Function to put records to the Kinesis stream with retry logic
def put_to_stream_batch(stream_name, records, max_retries=5):
    attempt = 0
    
    while attempt < max_retries:
        try:
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=records
            )            
            # Check if there are any failed records
            if response['FailedRecordCount'] > 0:
                print(f"Attempt {attempt + 1}: {response['FailedRecordCount']} records failed.")
                time.sleep(1)  # Exponential backoff
                attempt += 1
                # Retry only the failed records
                failed_records = [
                    records[i] for i in range(len(records))
                    if 'ErrorCode' in response['Records'][i]
                ]
                records = failed_records  # Retry only the failed records
            else:
                return response  # Success

        except Exception as e:
            print(f"Error putting records to stream: {e}")
            time.sleep(1)  # Exponential backoff
            attempt += 1
    
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

    log_client = boto3.client('logs', region_name='ap-southeast-2')
    # Initialize a session using Amazon Lambda
    lambda_client = boto3.client('lambda', region_name='ap-southeast-2')
    
    memory_sizes = [128, 256, 512, 832, 1769, 3009, 5308, 7707, 8846, 10240]

    for memory_size in memory_sizes:
        # Example start time (Unix epoch time in seconds)
        start_time = time.time()  # or any specific start time

        # Convert start time to milliseconds
        start_time_ms = int(start_time * 1000)

        # Delete the log group
        delete_log_group(log_client, log_group_name)

        # Update the function configuration
        lambda_client.update_function_configuration(
            FunctionName='stateful_example',
            MemorySize=memory_size
        )
        time.sleep(2)

        inputs = []
        total_records = 5000
        for msg_id in range(1, total_records + 1):
            data = {
                'msg_id': msg_id,
                'memory_size': memory_size
            }

            # Use msg_id as partition key
            partition_key = "1"

            # Prepare record
            input = { 
                'Data': json.dumps(data),
                'PartitionKey': partition_key
            }
            inputs.append(input)

            # Check if the batch size limit is reached
            if len(inputs) >= 500:
                put_to_stream_batch(stream_name, inputs)
                inputs = []  # Clear the list for the next batch
        if inputs:
            put_to_stream_batch(stream_name, inputs)

        while True:
        # Define the AWS CLI command for the log group
            command = [
                "aws", "logs", "filter-log-events",
                "--log-group-name", log_group_name,
                "--start-time", str(start_time_ms),
                "--output", "json"
            ]
            time.sleep(10)
            # Run the command and write the output to the specified file
            with open(file_path, "w") as file:
                subprocess.run(command, stdout=file)

            stats = parse_log_file(file_path, memory_size)

            # Print the statistics
            print(f"Memory Size: {memory_size} MB")
            print(f"Average Duration: {stats['average_duration']:.2f} ms")
            print(f"Minimum Start Time: {stats['min_start_time']}")
            print(f"Maximum Start Time: {stats['max_start_time']}")
            print(f"Unique Message ID Count: {stats['unique_message_id_count']}")
            print(f"Average Execution Time: {stats['average_execution_time']:.2f} microseconds")

            if not stats:
                continue
            if stats['unique_message_id_count'] == total_records:
                break
            if stats['max_start_time'] is not None and time.time() - stats['max_start_time'] > 60 and stats['unique_message_id_count'] > total_records * 9 / 10:
                print("Idle for 60 seconds.")
                break

         # Write the statistics to a file
        with open(output_file, "a") as out_file:
            out_file.write(f"Memory Size: {memory_size} MB\n")
            out_file.write(f"Average Duration: {stats['average_duration']:.2f} ms\n")
            out_file.write(f"Minimum Start Time: {stats['min_start_time']}\n")
            out_file.write(f"Maximum Start Time: {stats['max_start_time']}\n")
            out_file.write(f"Unique Message ID Count: {stats['unique_message_id_count']}\n")
            out_file.write(f"Average Execution Time: {stats['average_execution_time']:.2f} microseconds\n")
            out_file.write("\n")

if __name__ == "__main__":
    main()