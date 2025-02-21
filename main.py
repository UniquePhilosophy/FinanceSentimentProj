from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from schedule import Scheduler
import datetime, time, subprocess, threading
from utils.config_manager import ConfigManager
from finance_concern import news_producer
from finance_sentiment.sentiment_consumer import SentimentAnalysisConsumer

def create_kafka_topics(config, retries=5, retry_delay=5):
    attempt = 0
    print("Waiting for Kafka to spin up...")
    time.sleep(10)

    while attempt < retries:
        try:
            print("Attempting to create topics...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=config['kafka']['bootstrap_servers']
            )

            topics = []
            for producer_name, producer_config in config['producers'].items():
                topic_name = producer_config['topic']
                partitions = producer_config.get('partitions', 2)
                replication_factor = producer_config.get('replication_factor', 1)
                topic = NewTopic(
                    name=topic_name, 
                    num_partitions=partitions, 
                    replication_factor=replication_factor
                )
                topics.append(topic)

            admin_client.create_topics(new_topics=topics, validate_only=False)
            admin_client.close()

            print("Topics created successfully.")
            break
        except TopicAlreadyExistsError:
            print("Topics already exist.")
            break
        except Exception as e:
            print(f"Failed to create topics: {e}")
            attempt += 1
            time.sleep(retry_delay)
            if attempt == retries:
                print("Exceeded maximum retries. Topics not created.")

def start_docker_compose():
    try:
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("Docker Compose started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error starting Docker Compose: {e}")

def run_consumer():
    pass

def should_shutdown():
    now = datetime.datetime.now()
    shutdown_time = datetime.time(hour=23)
    return now.time() >= shutdown_time

if __name__ == "__main__":
    config = ConfigManager.load_config()

    start_docker_compose()

    create_kafka_topics(config)

    scheduler = Scheduler()

    # for testing purposes this function will generate a 
    # timestamp 2 minutes in the future
    def get_future_timestamp():
        future_time = datetime.datetime.now() + datetime.timedelta(minutes=2)
        print(f"Producer will run at {future_time.strftime('%H:%M')}.")
        return future_time.strftime("%H:%M")

    timestamp = get_future_timestamp()

    scheduler.every().day.at(timestamp).do(lambda: (
        print(f"{timestamp}: Executing News Producer."),
        news_producer.produce_articles_to_kafka()        
    ))

    sentimentAnalysisConsumer1 = SentimentAnalysisConsumer(1)
    sentimentAnalysisConsumer2 = SentimentAnalysisConsumer(2)
    sentimentAnalysisConsumerThread1 = threading.Thread(target=sentimentAnalysisConsumer1.run)
    sentimentAnalysisConsumerThread2 = threading.Thread(target=sentimentAnalysisConsumer2.run)
    sentimentAnalysisConsumerThread1.start()
    sentimentAnalysisConsumerThread2.start()

    while not should_shutdown():
        try:
            scheduler.run_pending()
        except Exception as e:
            print(f"Error running scheduled tasks: {e}")

        time.sleep(5)

    try:
        subprocess.run(["docker-compose", "kdown", "-v"], check=True)
        print("Docker Compose ended successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error ending Docker Compose: {e}")
    print("Shutting down at 11 PM.")
