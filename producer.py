import sys
import pika
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from decouple import config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# RabbitMQ параметры
RABBITMQ_HOST = config('RABBITMQ_HOST', default='localhost')
RABBITMQ_QUEUE = config('RABBITMQ_QUEUE', default='links')

def get_links(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        base_url = url
        links = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]
        title = soup.title.string if soup.title else 'No title'
        logging.info(f"Processed {url} - Title: {title}")
        for link in links:
            logging.info(f"Found link: {link}")
        return links
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        return []

def main():
    if len(sys.argv) < 2:
        print("Usage: python producer.py <URL>")
        return

    url = sys.argv[1]
    links = get_links(url)

    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    for link in links:
        channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=link)
        logging.info(f"Published link: {link}")

    connection.close()

if __name__ == "__main__":
    main()
