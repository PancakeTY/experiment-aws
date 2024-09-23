from util.kinesis import put_to_stream_batch

import threading
import time
from queue import Queue, Empty
import json

class AtomicInteger:
    def __init__(self, initial=1):
        self.value = initial
        self._lock = threading.Lock()

    def get_and_increment(self, increment_value=1):
        with self._lock:
            current_value = self.value
            self.value += increment_value
            return current_value

def generate_input_data(records, start, size, input_map):
    # Use input_map to access the specific indices for each attribute
    if records is None or input_map is None: 
        return_msg = [{'msg_id': msg_id} for msg_id in range(start, start + size)]  
    else:  
        return_msg = [{'msg_id': msg_id, **{key: record[input_map[key]] for key in input_map}}  
                      for msg_id, record in enumerate(records[start:start + size], start)]  
    return return_msg

def batch_producer(records, atomic_count, input_batchsize, input_map, batch_queue, end_time, rate, num_consumers):
    batch_interval = input_batchsize / rate  # Interval between batches in seconds
    next_batch_time = time.time()
    total_items_produced = 0  # Initialize a counter for total items produced

    print(f"Batch producer started. Producing {input_batchsize} items every {batch_interval} seconds.")
    while time.time() < end_time:
        now = time.time()
        sleep_time = next_batch_time - now
        if sleep_time > 0:
            time.sleep(sleep_time)
            now = time.time()  # Update now after sleeping

        # Produce batches until we're back on schedule
        while now >= next_batch_time:
            input_index = atomic_count.get_and_increment(input_batchsize)
            if records is not None and input_index + input_batchsize >= len(records):
                break
            input_data_list = generate_input_data(records, input_index, input_batchsize, input_map)
            # print(f"Produced batch : {input_data_list}")
            # Put batch data into queue
            batch_queue.put((input_index, input_data_list))
            total_items_produced += input_batchsize
            # Update next_batch_time for the next batch
            next_batch_time += batch_interval
            # Update now to reflect the current time after processing
            now = time.time()

    # After production is done, signal consumers to exit
    for _ in range(num_consumers):
        batch_queue.put(None)
    print(f"Batch producer finished. Total items produced: {total_items_produced}")
    return total_items_produced


def batch_consumer(kinesis_client, batch_queue, stream_name, partition_by = None):
    while True:
        batch_data = batch_queue.get()  # Blocking call, waits indefinitely
        if batch_data is None:
            # Sentinel value received, exit the thread
            batch_queue.task_done()
            break  # Use break instead of return for clarity
        # In here, chained id equals to msg id
        input_index, input_data_list = batch_data

        inputs = [] 
        for i in range(len(input_data_list)):
            input_msg = input_data_list[i]

            # print(f"Consumed batch : {input_msg}")

            # Use msg_id as partition key
            if partition_by is not None:
                partition_key = str(input_msg[partition_by])
            else:
                partition_key = str(input_msg['msg_id'])

            # Prepare record
            input = { 
                'Data': json.dumps(input_msg),
                'PartitionKey': partition_key
            }
            inputs.append(input)

        if inputs:
            put_to_stream_batch(kinesis_client, stream_name, inputs)

        batch_queue.task_done()
