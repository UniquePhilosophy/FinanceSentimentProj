# Finance Sentiments

## Overview
This application allows users to collect financial opinions from across the web and convert them into normalized sentimentclass. s for analytics.

The concept is that producers can be easily created by using the "producer_client.py" and creating new instances of the KafkaProducerClient. This means that the user can create a producer for each scraper, and have them all producing to the same topic. The producers can be run at scheduled intervals using the "schedule" library.

Consumers running continously will poll the kafka topics for new financial opinions, then using FinBERT will convert those opinions into a normalized sentiment and produce them back into Kafka.

Analytics programs need only to connect to the financial-sentiment topic to be able to gain real-time insight into financial sentiments.

## Features
- Makes use of Kafka for real-time data streaming.
- Multiple producers scrape financial opinions from across the web.
- Sentiment is analysed with FinBERT.
- Sentiments, along with name of stock-type and source, produced to kafka.
- Scheduler to schedule producers to run at different times.

## Installation
### Step-by-Step Installation
- git clone
- configure yaml file to meet computational requirements
- edit main.py with appropriate schedules
- create an API_KEYS.json file with your API keys for News API etc.
- run main.py (first run will take a while as FinBERT models download locally)
