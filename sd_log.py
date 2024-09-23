import re
import json
from collections import defaultdict
from datetime import datetime
import subprocess

log_group_names = [
    "/aws/lambda/sd_moving_avg",
    "/aws/lambda/sd_spike_detect"
]

file_paths = ['temp_sd_moving_avg.txt', 'temp_sd_spike_detect.txt']

# Loop over the log group names and file paths
for log_group, file_path in zip(log_group_names, file_paths):
    # Define the AWS CLI command for each log group
    command = [
        "aws", "logs", "filter-log-events",
        "--log-group-name", log_group,
        "--output", "json"
    ]
    
    # Run the command and write the output to the corresponding file
    with open(file_path, "w") as file:
        subprocess.run(command, stdout=file)

combined_log_data = []
for file_path in file_paths:
    with open(file_path, 'r') as file:
        combined_log_data.extend(json.load(file)["events"])

# Dictionary to hold timestamps of each msg_id
msg_id_timestamps = defaultdict(list)

# Regular expression to extract msg_id from message
msg_id_pattern = re.compile(r"The input msg_id is: '(\d+)'")

# Process each log entry
for log in combined_log_data:
    match = msg_id_pattern.search(log["message"])
    if match:
        msg_id = match.group(1)
        timestamp = log["timestamp"]
        msg_id_timestamps[msg_id].append(timestamp)

# Calculate the time difference for each msg_id
time_differences = {}
for msg_id, timestamps in msg_id_timestamps.items():
    first_time = min(timestamps)
    last_time = max(timestamps)
    time_difference = last_time - first_time
    time_differences[msg_id] = time_difference

# Display the results
for msg_id, time_diff in time_differences.items():
    print(f"msg_id: {msg_id}, Time difference: {time_diff} milliseconds")
