# Developed by: Sebastian Maurice, PhD
# Toronto, Ontario Canada
# OTICS Advanced Analytics

#######################################################################################################################################
#  This file will create the mapping for DSN id to TML id
#########################################################################################################################################

# TML python library
import maadstml
import json
import csv
import os
import time

# Set Global variables for VIPER and HPDE
basedir = os.environ['userbasedir']
viperconfigfile = basedir + "/Viper-produce/viper.env"

# Set Global Host/Port for VIPER
VIPERHOST = '127.0.0.1'
VIPERPORT = '5000'
HTTPADDR = 'https://'

# Store VIPER TOKEN
def getparams():
    global VIPERHOST, VIPERPORT, HTTPADDR
    with open("/Viper-produce/admin.tok", "r") as f:
        VIPERTOKEN = f.read()

    if VIPERHOST == "":
        with open('/Viper-produce/viper.txt', 'r') as f:
            output = f.read()
            VIPERHOST = HTTPADDR + output.split(",")[0]
            VIPERPORT = output.split(",")[1]

    return VIPERTOKEN

VIPERTOKEN = getparams()
if VIPERHOST == "":
    print("ERROR: Cannot read viper.txt: VIPERHOST is empty or HPDEHOST is empty")

def setupkafkatopic(topicname):
    # Set personal data
    companyname = "OTICS"
    myname = "Sebastian"
    myemail = "Sebastian.Maurice"
    mylocation = "Toronto"

    # Replication factor for Kafka redundancy
    replication = 1
    # Number of partitions for joined topic
    numpartitions = 1
    # Enable SSL/TLS communication with Kafka
    enabletls = 1
    # If brokerhost is empty then this function will use the brokerhost address in your
    # VIPER.ENV in the field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
    brokerhost = ''
    # If this is -999 then this function uses the port address for Kafka in VIPER.ENV in the
    # field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
    brokerport = -999
    # If you are using a reverse proxy to reach VIPER then you can put it here - otherwise if
    # empty then no reverse proxy is being used
    microserviceid = ''

    #############################################################################################################
    # CREATE TOPIC TO STORE TRAINED PARAMS FROM ALGORITHM
    producetotopic = topicname

    description = "Topic to store the trained machine learning parameters"
    result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, producetotopic, companyname,
                                      myname, myemail, mylocation, description, enabletls,
                                      brokerhost, brokerport, numpartitions, replication,
                                      microserviceid='')
    # Load the JSON array in variable y
    print("Result=", result)
    try:
        y = json.loads(result, strict='False')
    except Exception as e:
        y = json.loads(result)

    for p in y:  # Loop through the JSON and grab the topic and producerids
        pid = p['ProducerId']
        tn = p['Topic']

    return tn, pid

def csvlatlong(filename):
    csvfile = open(filename, 'r')
    fieldnames = ("dsn", "oem", "identifier", "index", "lat", "long")
    lookup_dict = {}
    reader = csv.DictReader(csvfile, fieldnames)
    for row in reader:
        lookup_dict[(row['dsn'], row['lat'].lower(), row['long'].lower(), row['identifier'])] = row
    return lookup_dict

def getlatlong2(reader):
    random_lines = random.choice(list(reader))
    return random_lines['lat'], random_lines['long'], random_lines['identifier']

def producetokafka(data, producerid, maintopic):
    topicid = -999
    delay = 7000
    enabletls = 1

    try:
        result = maadstml.viperproducetotopic(VIPERTOKEN, VIPERHOST, VIPERPORT, maintopic, producerid,
                                             enabletls, delay, '', '', '', 0, data, "")
        print(result)
    except Exception as e:
        print("ERROR:", e)

inputfile = basedir + '/IotSolution/IoTData.txt'
maintopic = 'iot-mainstream'

# Setup Kafka topic
producerid = ''
try:
    topic, producerid = setupkafkatopic(maintopic)
except Exception as e:
    pass

reader = csvlatlong(basedir + '/IotSolution/dsntmlidmain.csv')
file1 = open(inputfile, 'r')

while True:
    line = file1.readline()
    line = line.replace(";", " ")

    try:
        jsonline = json.loads(line)
        lat, long, ident = getlatlong2(reader)
        
        data = {
            "Company": jsonline['Company'],
            "Global_Rank": jsonline['Global Rank'],
            "Sales_(Billion_$)": jsonline['Sales (Billion $)'],
            "Profits_(Billion_$)": jsonline['Profits (Billion $)'],
            "Assets_(Billion_$)": jsonline['Assets (Billion $)'],
            "Market_Value_(Billion_$)": jsonline['Market Value (Billion $)'],
            "Country": jsonline['Country'],
            "Continent": jsonline['Continent'],
            "Latitude": lat,
            "Longitude": long
        }
        
        line_to_produce = json.dumps(data)
        
        producetokafka(line_to_produce, producerid, maintopic)
        time.sleep(0.2)
    except Exception as e:
        pass

file1.close()
