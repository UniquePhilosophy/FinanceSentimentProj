import requests, json
from producer_client import KafkaProducerClient
from utils.config_manager import ConfigManager

config = ConfigManager.load_config()

with open('API_KEYS.json') as f:
    api_keys = json.load(f)

x_token = api_keys['x-token']

def fetch_tweets_for_stock(stock_query):
    url = "https://api.x.com/2/tweets/search/recent"
    headers = {
        "Authorization": f"Bearer {x_token}"
    }
    querystring = {
        "query": stock_query,
        "tweet.fields": "created_at,author_id,text",
        "max_results": "1"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        print(f"Failed to fetch tweets for query: {stock_query}. Status code: {response.status_code}")
        return []

def produce_tweets_to_kafka():
    producer_client = KafkaProducerClient(topic=config['producers']['financial_concerns']['topic'], batch_size=10)
    
    for stock, details in config['stocks'].items():
        stock_query = details['query']
        tweets = fetch_tweets_for_stock(stock_query)
        for tweet in tweets:
            message = {
                'source': 'x_api', 
                'message': tweet['text'],
                'stock': stock
            }
            producer_client.send_message(message)
    
    producer_client.close()

produce_tweets_to_kafka()
