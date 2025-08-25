import pandas as pd
import time
import json
from kafka_project.KafkaUtil.producer import Producer

def fetch_transactions():
    df = pd.read_csv("dataset/credit_card_transactions.csv").iloc[20:25]  # Load first 100 rows for testing
    return df

def send_transactions_to_kafka(producer, df):
    if df.empty:
        print("No transactions to send.")
        return
    
    # Send each row at intervals (simulate real-time)
    for _, row in df.iterrows():
        record = row.to_dict()
        print(f"Sending transaction to Kafka...\n {record}")
        producer.send_message(
            key=str(record["id"]),
            data_json=json.dumps(record)
        )
        time.sleep(1)  # <-- Adjust interval (2 sec here)
    return