#!python3

import os
import json
import pandas

from kafka import KafkaConsumer
from sqlalchemy import create_engine

if __name__ == "__main__":
    print("starting the consumer")

    #connect database
    try:
        engine = create_engine('postgresql://postgres:admin@172.31.16.43:5432/final_project_dwh')
        print(f"[INFO] Successfully Connect Database .....")
    except:
        print(f"[INFO] Error Connect Database .....")

    #connect kafka server
    try:
        consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')
        print(f"[INFO] Successfully Connect Kafka Server .....")
    except:
        print(f"[INFO] Error Connect Kafka Server .....")

    #read message from topic kafka server
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")

        df = pandas.DataFrame(data, index=[0])
        df.to_sql('search_log', con=engine, if_exists='append', index=False)