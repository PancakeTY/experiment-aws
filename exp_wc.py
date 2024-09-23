from util.thread import AtomicInteger, batch_producer, batch_consumer
from util.cloudwatch import delete_log_group, parse_log_file

import boto3
import json
import time
import subprocess
from botocore.exceptions import ClientError
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor  
import threading
import numpy as np
import re

# Initialize a session using Amazon Kinesis
function_name = 'wordcount_split_redis_kinesis'
file_name = 'wc'
stream_name = function_name
# log group name in cloudwatch
log_group_name = f"/aws/lambda/wordcount_count_redis_kinesis"
# tmp copy file path
file_path = f"logs/{file_name}_log.txt"
# output file path
output_file = f"results/{file_name}_output.txt"
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
DURATION = 10
INPUT_BATCHSIZE = 300
NUM_INPUT_THREADS = 10
INPUT_MAP = {"sentence": 0}

log_client = boto3.client('logs', region_name='ap-southeast-2')
lambda_client = boto3.client('lambda', region_name='ap-southeast-2')
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')

def read_sentences_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            text = file.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

    # Remove all non-alphabetic characters and convert to lowercase
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', text).lower()
    
    # Split text into words
    words = cleaned_text.split()
    print(f"Total words extracted: {len(words)}")

    # Group words into sentences of 10 words each
    sentences = [[' '.join(words[i:i+10])] for i in range(0, len(words), 10)]
    print(f"Total sentences created: {len(sentences)}")

    return sentences

def run(input_rate = None, replica = None):
    # Convert start time to milliseconds to match cloudwatch log
    log_start_time_ms = int(time.time() * 1000)
    delete_log_group(log_client, log_group_name)

    # Update the function configuration
    
    if replica is not None :
        response = lambda_client.put_function_concurrency(
            FunctionName='wordcount_split_redis_kinesis',
            ReservedConcurrentExecutions=replica
        )
        response = lambda_client.put_function_concurrency(
            FunctionName='wordcount_count_redis_kinesis',
            ReservedConcurrentExecutions=replica
        )
        print("Reserved concurrency set:", response, "\n")

    time.sleep(2)

    records = read_sentences_from_file('data/books.txt')
    atomic_count = AtomicInteger(1)
    batch_queue = Queue()

    # Launch multiple threads
    start_time = time.time()
    end_time = start_time + DURATION

    input_threads = []

    with ThreadPoolExecutor() as executor:
        # Submit the batch_producer function to the executor
        future = executor.submit(
            batch_producer,
            records,
            atomic_count,
            INPUT_BATCHSIZE,
            INPUT_MAP,
            batch_queue,
            end_time,
            input_rate,
            NUM_INPUT_THREADS,
        )

        # Start consumer threads
        for _ in range(NUM_INPUT_THREADS):
            thread = threading.Thread(
                target=batch_consumer,
                args=(
                    kinesis_client,
                    batch_queue,
                    stream_name,
                )
            )
            input_threads.append(thread)
            thread.start()

    total_records = future.result()
    for thread in input_threads:
        thread.join()

    print(f"start fecthing logs")
    while True:
    # Define the AWS CLI command for the log group
        command = [
            "aws", "logs", "filter-log-events",
            "--filter-pattern", "custom metrics",
            "--log-group-name", log_group_name,
            "--start-time", str(log_start_time_ms),
            "--output", "json"
        ]
        time.sleep(10)
        # Run the command and write the output to the specified file
        with open(file_path, "w") as file:
            subprocess.run(command, stdout=file)

        stats = parse_log_file(file_path)

        print(f"the stats record length is {len(stats)}")

        if not stats:
            continue
        if len(stats) >= total_records:
            break
        if stats['max_start_time'] is not None and time.time() - stats['max_start_time'] > 60 and len(stats) > total_records * 9 / 10:
            print("Idle for 60 seconds.")
            break

    execution_times = [msg['execution_time'] for msg_id, msg in stats.items() if isinstance(msg, dict)]
    if not execution_times:
        print("No execution times found.")
        
    # Calculate statistics using numpy
    execution_times = np.array(execution_times)
    median_execution_time = np.median(execution_times)
    percentile_95_execution_time = np.percentile(execution_times, 95)
    percentile_99_execution_time = np.percentile(execution_times, 99)

    # Add these stats to the output
    stats['median_execution_time'] = median_execution_time
    stats['95_percentile_execution_time'] = percentile_95_execution_time
    stats['99_percentile_execution_time'] = percentile_99_execution_time
    stats['duration'] = stats['max_end_time'] - stats['min_start_time']

    # Print the statistics
    print(f"Duration: {stats['duration']:.2f} seconds")
    print(f"Median Execution Time: {median_execution_time:.2f} microseconds")
    print(f"95th Percentile Execution Time: {percentile_95_execution_time:.2f} microseconds")
    print(f"99th Percentile Execution Time: {percentile_99_execution_time:.2f} microseconds")

    # Write the statistics to a file
    with open(output_file, "a") as out_file:
        # Add the new statistics for execution times
        out_file.write(f"Input Rate: {input_rate} records per second\n")
        out_file.write(f"Replica: {replica}\n")
        out_file.write(f"Total Records: {total_records}\n")
        out_file.write(f"Duration: {stats['duration']:.2f} seconds\n")
        out_file.write(f"Median Execution Time: {stats['median_execution_time']:.2f} microseconds\n")
        out_file.write(f"95th Percentile Execution Time: {stats['95_percentile_execution_time']:.2f} microseconds\n")
        out_file.write(f"99th Percentile Execution Time: {stats['99_percentile_execution_time']:.2f} microseconds\n")
        out_file.write("\n")

# Main function to write data
def main():
    
    replica = 10
    input_rates = [300, 600, 900, 1200]
    
    # for input_rate in input_rates:
    #     run(input_rate = input_rate, replica = 50)
        
    function_replicas = [25, 50, 100, 200]
    for replica in function_replicas:
        run(input_rate = 300, replica = replica)


if __name__ == "__main__":
    main()