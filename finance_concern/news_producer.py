import requests, json
from shared_frameworks.producer_client import KafkaProducerClient
from utils.config_manager import ConfigManager

config = ConfigManager.load_config()

with open('API_KEYS.json') as f:
    api_keys = json.load(f)

news_token = api_keys['news-token']

def fetch_articles_for_stock(stock):
    print(f"{stock}")
    url = (f"https://newsapi.org/v2/everything?q={stock}&from=2025-02-18T20:36:55&to=2025-02-18T20:40:55&sortBy=popularity&apiKey={news_token}")

    response = requests.get(url)
    
    if response.status_code == 200:
        print("Articles found.")
        return response.json().get('articles', [])
    else:
        print(f"Failed to fetch articles for query: {stock}. Status code: {response.status_code}")
        return []

def produce_articles_to_kafka():
    producer_client = KafkaProducerClient(topic=config['producers']['financial_concerns']['topic'], batch_size=10)
    
    for stock, details in config['stocks'].items():

        articles = fetch_articles_for_stock(stock)
        for article in articles:
            message = {
                'source': 'x_api', 
                'message': article['content'],
                'stock': stock
            }
            producer_client.send_message(message)
    
    producer_client.close()
    print("News producer task completed.")