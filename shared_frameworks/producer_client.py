from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
from utils.config_manager import ConfigManager
import json
import time
import hashlib

config = ConfigManager.load_config()

class KafkaProducerClient:
    def __init__(self, topic, retries=5, retry_delay=2, batch_size=20):
        self.producer = AvroProducer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'schema.registry.url': config['kafka']['schema_registry_url'],
            'socket.timeout.ms': 10000,
        })
        self.topic = topic
        self.retries = retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        self.message_queue = []

        schema_path = f'schemas/{self.topic}.avsc'
        with open(schema_path, 'r') as schema_file:
            self.value_schema = avro.loads(schema_file.read())

    def hash_message_content(self, message):
        message_str = json.dumps(message)
        hashed_value = hashlib.md5(message_str.encode('utf-8')).hexdigest()
        return hashed_value

    def send_message(self, message):
        key = self.hash_message_content(message).encode('utf-8')
        self.message_queue.append((key, message))
        if len(self.message_queue) >= self.batch_size:
            self.flush_messages()

    def flush_messages(self):
        for key, message in self.message_queue:
            attempt = 0
            while attempt < self.retries:
                try:
                    print(f"Sending message {key}: {message}")
                    self.producer.produce(
                        topic=self.topic, 
                        value=message, 
                        key=key,
                        value_schema=self.value_schema)
                    break
                except Exception as e:
                    print(f"Failed to send message: {message} on attempt {attempt + 1}. Error: {e}")
                    attempt += 1
                    time.sleep(self.retry_delay)
                    if attempt == self.retries:
                        print("Exceeded maximum retries. Message not sent.")
        self.producer.flush()
        self.message_queue = []
        print(f"Messages sent successfully to topic: {self.topic}")

    def close(self):
        self.flush_messages()
        self.producer.flush()
