import boto3
import time

def put_to_stream_batch(kinesis_client, stream_name, records, max_retries=5):
    attempt = 0
    
    print(f"Putting {len(records)} records to stream {stream_name}")
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