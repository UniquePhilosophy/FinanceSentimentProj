import requests, json
from shared_frameworks.producer_client import KafkaProducerClient
from utils.config_manager import ConfigManager

config = ConfigManager.load_config()

with open('API_KEYS.json') as f:
    api_keys = json.load(f)

news_token = api_keys['news-token']

def fetch_articles_for_stock(stock):
    url = (f"https://newsapi.org/v2/everything?q={stock}&from=2025-02-18T20:36:55&to=2025-02-18T20:40:55&sortBy=popularity&apiKey={news_token}")

    response = requests.get(url)
    
    if response.status_code == 200:
        print(f"Scrape successful for stock type: {stock}.")
        return response.json().get('articles', [])
    else:
        print(f"Scrape failed for stock type: {stock}. Status code: {response.status_code}")
        return []

def produce_articles_to_kafka():
    producer_client = KafkaProducerClient(
        topic=config['producers']['financial_concerns']['topic'], 
        partition_key='text', 
        partition_strategy='hash'
        )
    
    for stock, details in config['stocks'].items():

        articles = fetch_articles_for_stock(stock)
        # for article in articles[]: # uncomment to use ALL articles (DISABLE GCP BILLING!)
        for article in articles[:2]:
            message = {
                'source': 'x_api', 
                'text': article['content'],
                'stock': stock
            }
            producer_client.send_message(message)
    
    producer_client.close()
    print("News producer task completed.")