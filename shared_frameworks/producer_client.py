from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
from utils.config_manager import ConfigManager
import json
import time
import hashlib

config = ConfigManager.load_config()

class KafkaProducerClient:
    def __init__(self, topic, retries=5, retry_delay=2, batch_size=10, partition_key=None, partition_strategy=None):
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
        self.partition_key = partition_key
        self.partition_strategy = partition_strategy

        value_schema_path = f'schemas/{self.topic}.avsc'
        key_schema_path = f'schemas/{self.topic}_key.avsc'

        with open(value_schema_path, 'r') as schema_file:
            self.value_schema = avro.loads(schema_file.read())

        with open(key_schema_path, 'r') as schema_file:
            self.key_schema = avro.loads(schema_file.read())

    def hash_text(self, text):
        hashed_value = hashlib.md5(text.encode('utf-8')).hexdigest()
        return hashed_value

    def send_message(self, message):
        if self.partition_strategy == 'hash':
            key = {"key": self.hash_text(message[self.partition_key])}
        elif self.partition_strategy == 'natural':
            key = {"key": self.partition_key}
            
        self.message_queue.append((key, message))
        if len(self.message_queue) >= self.batch_size:
            self.flush_messages()

    def flush_messages(self):
        successes = 0
        for key, message in self.message_queue:
            attempt = 0
            while attempt < self.retries:
                try:
                    log_info = ", ".join([f"{k}: {v}" for k, v in list(message.items())[:3]])
                    print(f"Sending message {key}: {log_info}...")
                    self.producer.produce(
                        topic=self.topic, 
                        value=message, 
                        key=key,
                        value_schema=self.value_schema,
                        key_schema=self.key_schema)
                    successes += 1
                    break
                except Exception as e:
                    log_info = ", ".join([f"{k}: {v}" for k, v in list(message.items())[:3]])
                    print(f"Failed to send message: {key}: {log_info} on attempt {attempt + 1}. Error: {e}")
                    attempt += 1
                    time.sleep(self.retry_delay)
                    if attempt == self.retries:
                        print("Exceeded maximum retries. Message not sent.")
        self.producer.flush()
        self.message_queue = []
        print(f"{successes} messages sent to topic: {self.topic}")

    def close(self):
        self.flush_messages()
