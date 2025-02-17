from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroConsumer
from utils.config_manager import ConfigManager
from shared_frameworks.producer_client import KafkaProducerClient

config = ConfigManager.load_config()

def analyze_sentiment(message):
    sentiment = "neutral"
    return sentiment

def consume_and_produce():
    consumer_config = {
        'bootstrap.servers': config['kafka']['bootstrap_servers'],
        'group.id': 'finance-sentiment-group',
        'schema.registry.url': config['kafka']['schema_registry_url'],
        'auto.offset.reset': 'earliest',
    }
    
    producer_client = KafkaProducerClient(topic=config['producers']['financial_sentiment']['topic'], batch_size=10)
    
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(['finance-concern'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Extract message and run sentiment analysis
            message = msg.value()
            sentiment = analyze_sentiment(message['message'])
            
            # Produce sentiment analysis result to a new Kafka topic
            sentiment_message = {
                'source': message['source'],
                'message': message['message'],
                'stock': message['stock'],
                'sentiment': sentiment
            }
            producer_client.send_message(sentiment_message)
    
    except Exception as e:
        print(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        producer_client.close()

consume_and_produce()
