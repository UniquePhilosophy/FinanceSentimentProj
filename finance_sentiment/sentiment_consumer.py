from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroConsumer
from utils.config_manager import ConfigManager
from shared_frameworks.producer_client import KafkaProducerClient

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
            partition_key='sentiment'
        )
        self.consumer = AvroConsumer(self.consumer_config)
        self.consumer.subscribe(
            [config['producers']['financial_sentiments']['topic']]
            )

    def analyze_sentiment(self, message):
        # sentiment analysis logic
        sentiment = 0
        return sentiment

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                message = msg.value()
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
