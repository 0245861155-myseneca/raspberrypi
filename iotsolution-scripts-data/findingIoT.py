#!/usr/bin/env python3

import os

# Get the current directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the path to the IoTData.txt file
iot_data_file_path = os.path.join(current_directory, 'IoTData.txt')

# Read and print the contents of the IoTData.txt file
try:
    with open(iot_data_file_path, 'r') as iot_data_file:
        iot_data = iot_data_file.read()
        print(iot_data)
except FileNotFoundError:
    print("IoTData.txt file not found in the current directory.")
