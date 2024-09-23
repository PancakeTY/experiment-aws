import boto3
import json
import time
import csv

# Initialize a session using Amazon Kinesis
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
stream_name = 'mo_score'

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
    records = []
    with open(file_path, 'r') as file:  # Open the file
        reader = csv.reader(file)
        for data in reader:
            records.append(data)
    return records

# Main function to write data
def main():
    file_path = 'data/machine_usage.csv'
    # total_count = int(input("Enter the total count of requests to Kinesis: "))

    records = read_data_from_file(file_path)
    
    if len(records) == 0:
        print("No records containing only alphabetic characters were found.")
        return

    end = 100
    inputs = []
    for msg_id, record in enumerate(records, start=1):
        data = {
            "machine_id": record[0].split('_')[1], 
            "cpu": record[2], 
            "mem": record[3], 
            "timestamp": record[1],
            "msg_id": msg_id
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