import json
from kafka_project.KafkaUtil.consumer import Consumer
from kafka_project.KafkaUtil.producer import Producer

# Consumes transactions_raw, validates and produces to transactions_validated
def validate_transaction(consumer: Consumer, producer: Producer):
    record_list = consumer.consume_messages(["transactions_raw"])
    for record in record_list:
        # print(record)
        record = json.loads(record)
        print(f"Consuming transaction: {record}")
        # Simple validation checks
        valid = True
        if record["amt"] <= 0:
            valid = False
        # upload to transactions_validated producer if valid
        if valid:
            record["status"] = "VALIDATED"
            producer.send_message(
                key=str(record["id"]),
                data_json=json.dumps(record)
            )
            print(f"Transaction validated...\n{record}")
        else:
            print(f"Invalid transaction: {record}")