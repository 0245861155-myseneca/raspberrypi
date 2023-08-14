import maadstml
import json
import csv
import time
import os

# Set your base directory
basedir = "/your/base/directory"

# Set the VIPER token
def get_viper_token():
    with open(os.path.join(basedir, "admin.tok"), "r") as f:
        return f.read()

VIPERTOKEN = get_viper_token()

# Set up Kafka topic
def setup_kafka_topic(topic_name):
    # Set your company and personal information
    company_name = "OTICS"
    my_name = "Sebastian"
    my_email = "Sebastian.Maurice"
    my_location = "Toronto"

    # Other Kafka topic parameters
    replication = 1
    num_partitions = 1
    enable_tls = 1
    broker_host = ""
    broker_port = -999
    microservice_id = ""

    # Create the Kafka topic
    result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, topic_name, company_name,
                                      my_name, my_email, my_location, "", enable_tls,
                                      broker_host, broker_port, num_partitions, replication,
                                      microservice_id)
    topic_id = json.loads(result)[0]["ProducerId"]

    return topic_id

# Read CSV data for latitude, longitude, and identifier
def read_csv_lat_long(filename):
    lookup_dict = {}

    with open(filename, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            lookup_dict[(row["dsn"], row["lat"].lower(),
                         row["long"].lower(), row["identifier"])] = row

    return lookup_dict

# Get latitude, longitude, and identifier from lookup dictionary
def get_lat_long_identifier(reader, search):
    # Replace this with your logic to retrieve lat, long, and identifier
    random_entry = random.choice(list(reader))
    return random_entry["lat"], random_entry["long"], random_entry["identifier"]

# Publish data to Kafka topic
def publish_to_kafka(value, topic_id, maintopic, substream):
    delay = 7000
    enable_tls = 1

    try:
        maadstml.viperproducetotopic(VIPERTOKEN, VIPERHOST, VIPERPORT, maintopic, topic_id,
                                     enable_tls, delay, "", "", "", 0, value, substream, -999, "")
    except Exception as e:
        print("ERROR:", e)

def main():
    input_file = os.path.join(basedir, "IotSolution/IoTData.txt")
    maintopic = "iot-mainstream"

    # Set VIPERHOST and VIPERPORT as needed
    VIPERHOST = ""
    VIPERPORT = ""

    try:
        # Setup Kafka topic
        topic_id = setup_kafka_topic(maintopic)

        # Read CSV data for latitude, longitude, and identifier
        reader = read_csv_lat_long(os.path.join(basedir, "IotSolution/dsntmlidmain.csv"))

        # Read and publish data from the input file
        with open(input_file, "r") as file1:
            while True:
                line = file1.readline()
                if not line:
                    file1.seek(0)
                else:
                    line = line.replace(";", " ")
                    jsonline = json.loads(line)
                    lat, long, ident = get_lat_long_identifier(reader, jsonline["metadata"]["dsn"])
                    line = f'{line[:-2]},"lat":{lat},"long":{long},"identifier":"{ident}"}}'
                    publish_to_kafka(line.strip(), topic_id, maintopic, "")

                time.sleep(0.2)

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
