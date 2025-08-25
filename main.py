from flask import Flask, jsonify

# import the Kafka producer utility
from kafka_project.KafkaUtil.producer import Producer
from kafka_project.KafkaUtil.consumer import Consumer

# import transaction producer function
import kafka_project.Services.TransactionsRaw as transaction_raw_service
import kafka_project.Services.TransactionsVal as transaction_val_service
import kafka_project.Services.TransactionsDB as db_service

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello this is flask'

@app.route('/send_transactions_raw')
def send_transactions_raw():
    # Create a Kafka producer instance
    topic = "transactions_raw"
    producer = Producer('localhost', 9092).create(topic=topic)  # Create a Kafka producer instance using the utility function
    df = transaction_raw_service.fetch_transactions()
    transaction_raw_service.send_transactions_to_kafka(producer, df)
    return jsonify(df.head().to_dict(orient='records'))  # Return the first 10 rows as a JSON response

@app.route('/validate_transactions_raw')
def consume_transactions_raw():
    consumer = Consumer('localhost', 9092).create("transactions_raw")
    producer = Producer('localhost', 9092).create("transactions_val")  # Create a Kafka producer instance using the utility function
    transaction_val_service.validate_transaction(consumer, producer)
    return "transactions_raw validated"  # Return the first 10 rows as a JSON response

@app.route('/upload_transactions_to_db')
def upload_transactions_to_db():
    consumer = Consumer('localhost', 9092).create("transactions_val")
    db_service.upload_to_db(consumer)
    return "transactions_val consumed"

@app.route('/fetch_transactions_from_db')
def fetch_transactions_from_db():
    records = db_service.fetch_from_db()
    return records

if(__name__) == '__main__':
    app.run(debug=True)