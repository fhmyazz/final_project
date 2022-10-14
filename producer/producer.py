#!python3

from ensurepip import bootstrap
import json
import time

import pandas as pd

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":

    #read data
    file = pd.read_csv('bigdata_log.csv').to_dict(orient='records')

    #connect to kafka
    producer = KafkaProducer(bootstrap_servers = ['localhost'],
                            value_serializer = json_serializer)

    #push data with topic final-project
    while True:
        for data in file:
            print(data)
            producer.send("final-project", data)
            # time.sleep(1)