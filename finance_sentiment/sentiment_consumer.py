from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroConsumer
from shared_frameworks.producer_client import KafkaProducerClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from utils.config_manager import ConfigManager
import time, torch
import torch.nn.functional as F

config = ConfigManager.load_config()

class SentimentAnalysisConsumer:
    def __init__(self, consumer_id):
        self.consumer_id = consumer_id
        self.consumer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['consumers']['sentiment_analysis_consumer']['group'],
            'schema.registry.url': config['kafka']['schema_registry_url'],
            'auto.offset.reset': 'earliest',
            'client.id': f'consumer-{consumer_id}'
        }
        self.producer_client = KafkaProducerClient(
            topic=config['producers']['financial_sentiments']['topic'],
            partition_key='sentiment',
            partition_strategy='natural'
        )
        self.consumer = AvroConsumer(self.consumer_config)
        self.consumer.subscribe(
            [config['consumers']['sentiment_analysis_consumer']['topic']]
        )
        self.last_message_time = time.time()

        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

    def analyze_sentiment(self, text):
        print("Analysing sentiment...")
        try:
            inputs = self.tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
            outputs = self.model(**inputs)
            probs = F.softmax(outputs.logits, dim=-1)
            sentiment = torch.argmax(probs).item()

            sentiment_mapping = {0: -1, 1: 0, 2: 1}
            return sentiment_mapping[sentiment]
        except Exception as e:
            print(f"Error while analyzing sentiment: {e} Returning 0.")
            return 0

    def run(self):
        try:
            message_count = 0
            print(f"Consumer {self.consumer_id} polling for messages...")
            while True:
                msg = self.consumer.poll(timeout=1.0)
                current_time = time.time()
                if msg is None:
                    if current_time - self.last_message_time > 5 and len(self.producer_client.message_queue) > 0:
                        self.producer_client.flush_messages()
                        self.last_message_time = current_time
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                message = msg.value()
                print(f"Message received: {message} at consumer {self.consumer_id}")
                self.last_message_time = current_time
                message_count += 1
                if message_count % 10 == 0:
                    self.last_message_time = current_time

                sentiment = self.analyze_sentiment(message['text'])

                sentiment_message = {
                    'source': message['source'],
                    'sentiment': sentiment,
                    'stock': message['stock']
                }
                self.producer_client.send_message(sentiment_message)

        except Exception as e:
            print(f"Error while consuming messages: {e}")
        finally:
            self.consumer.close()
            self.producer_client.close()
