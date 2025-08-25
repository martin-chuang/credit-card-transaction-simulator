from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, ipAddress='localhost', port=9092, consumer=None):
        self.ipAddress = ipAddress
        self.port = port
        self.consumer = consumer

    # Create and return a Kafka consumer instance
    def create(self, topic):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=f'{self.ipAddress}:{self.port}',
            key_deserializer=lambda key: json.loads(key.decode('utf-8')),
            value_deserializer=lambda value: json.loads(value.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=3000 # Exit after 3s with no messages
        )
        return Consumer(self.ipAddress, self.port, consumer)
    
    # Subscribe to topics and consume messages
    # def subscribe(self, topics):
    #     if not self.consumer:
    #         raise Exception("Consumer not initialized. Please create a consumer instance first.")
        
    #     self.consumer.subscribe(topics)
    #     print(f"Subscribed to topics: {topics}")
    
    def consume_messages(self, topics=None):
        """Consume messages from the subscribed topics."""
        if not self.consumer:
            raise Exception("Consumer not initialized. Please create a consumer instance first.")
        if not topics:
            raise Exception("Topic not specified. Please set the topic before consuming messages.")
        self.consumer.subscribe(topics)
        record_list = []
        for record in self.consumer:
            record_list.append(record.value)
        # self.consumer.unsubscribe()
        print(f"Number of records consumed: {len(record_list)}")
        return record_list
    
    # Unsubscribe from topics
    def unsubscribe(self):
        if not self.consumer:
            raise Exception("Consumer not initialized. Please create a consumer instance first.")
        
        self.consumer.unsubscribe()
        print("Unsubscribed from all topics.")