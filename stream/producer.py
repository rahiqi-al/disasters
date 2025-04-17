from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import logging
import requests
from stream.streamConfig.config  import config


logger = logging.getLogger(__name__)

def report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to topic={msg.topic()}, partition={msg.partition()}')

def fetch_weather_api():

    params = {'q': 'Chefchaouen,MA','appid': config.api_key,'units': 'metric'}

    for attempt in range(3):
        try:
            response = requests.get(config.weather_api_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            weather_data = {
                'Temperature': data['main']['temp'],
                'RH': data['main']['humidity'],
                'Ws': data['wind']['speed'] * 3.6,  # we converted from m/s to km/h
                'Rain': data.get('rain', {}).get('1h', 0.0)  # rain in the last hour
            }

            logger.info('Weather data fetched successfully')
            return weather_data
        
        except requests.exceptions.RequestException as e:
            logger.error(f'API fetch failed on attempt {attempt+1}: {e}')
            if attempt < 2:
                logger.info('Retrying after 30 seconds')
                time.sleep(30)
            else:
                logger.error('All retries failed')
                raise

def producer():

    try:
        admin = AdminClient(config.producer_config)
        if config.topic_name not in admin.list_topics().topics:
            topic = NewTopic(config.topic_name, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            logger.info(f'Created topic: {config.topic_name}')
        else:
            logger.info(f'Topic already exists: {config.topic_name}')

        producer = Producer(config.producer_config)

        while True:
            try:

                weather_data = fetch_weather_api()

                producer.produce(topic=config.topic_name, value=json.dumps(weather_data), callback=report)
                producer.flush()
                logger.info('Data produced to Kafka')

                time.sleep(3600)

            except Exception as e:
                logger.error(f'Producer loop error: {e}', exc_info=True)
                time.sleep(60)  

    except Exception as e:
        logger.error(f'Producer initialization error: {e}', exc_info=True)

