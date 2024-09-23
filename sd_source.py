import boto3
import json
import time
import re

# Initialize a session using Amazon Kinesis
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
stream_name = 'sd_moving_avg'

# Function to put records to the Kinesis stream
def put_to_stream_batch(stream_name, records):
    try:
        response = kinesis_client.put_records(
            StreamName=stream_name,
            Records=records
        )
        print(response)
        return response
    except Exception as e:
        print(f"Error putting records to stream: {e}")
        return None
    
def read_data_from_file(file_path):
    data_vectors = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors

# Main function to write data
def main():
    file_path = 'data/data_sensor.txt'
    # total_count = int(input("Enter the total count of requests to Kinesis: "))

    records = read_data_from_file(file_path)
    
    if len(records) == 0:
        print("No records containing only alphabetic characters were found.")
        return

    end = 500
    inputs = []
    for msg_id, record in enumerate(records, start=1):
        data = {
            'sensor_id': record[3],
            'temperature': record[4],
            'msg_id': msg_id
        }
        
        # Use msg_id as partition key
        partition_key = str(msg_id)

        # Prepare record
        input = {
            'Data': json.dumps(data),
            'PartitionKey': partition_key
        }
        inputs.append(input)

        # Send in batches of 10
        if len(inputs) == 10:
            response = put_to_stream_batch(stream_name, inputs)
            if response:
                print(response)
            inputs = []
        
        if msg_id >= end:
            break

if __name__ == "__main__":
    main()