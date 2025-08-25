from kafka import KafkaProducer
import json

class Producer:
    def __init__(self, ipAddress='localhost', port=9092, topic=None, producer=None):
        self.ipAddress = ipAddress
        self.port = port
        self.topic = topic
        self.producer = producer

    # Create and return a Kafka producer instance
    def create(self, topic):
        producer = KafkaProducer(
            bootstrap_servers=f'{self.ipAddress}:{self.port}',
            key_serializer=lambda key: json.dumps(key).encode('utf-8'),
            value_serializer=lambda value: json.dumps(value).encode('utf-8')
        )
        return Producer(self.ipAddress, self.port, topic, producer)

    # Callback to confirm delivery
    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    # Send a message to Kafka producer
    def send_message(self, key, data_json):
        if not self.producer:
            raise Exception("Producer not initialized. Please create a producer instance first.")
        if not self.topic:
            raise Exception("Topic not specified. Please set the topic before sending messages.")
        future = self.producer.send(self.topic, key=key, value=data_json)
        try:
            record_metadata = future.get(timeout=10)  # Wait for send to complete
            return f"Message sent successfully to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}"
        except AttributeError:
            return "Producer not initialized. Please create a producer instance first."
        except Exception as excp:
            return f"Message delivery failed: {excp}"