import os
import pika
from bs4 import BeautifulSoup
import requests
import time

# Получаем параметры подключения из переменных окружения
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))

# Установка соединения с RabbitMQ
def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
            channel = connection.channel()
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("Ошибка соединения. Повторная попытка через 5 секунд...")
            time.sleep(5)

connection, channel = connect_to_rabbitmq()

# Указываем имя очереди
queue_name = 'links'
channel.queue_declare(queue=queue_name)

print("Consumer готов к работе. Ожидаем сообщений...")

# Функция обработки сообщений
def callback(ch, method, properties, body):
    url = body.decode()
    print(f"Получена ссылка: {url}")

    try:
        # Загрузка HTML страницы
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Получение заголовка страницы
        title = soup.title.string if soup.title else 'Без названия'
        print(f"Заголовок: {title}")

        # Извлечение внутренних ссылок
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/'):  # Относительная ссылка
                href = f"{url.rstrip('/')}/{href.lstrip('/')}"

            print(f"Найдена ссылка: {link.string.strip() if link.string else 'Без текста'} -> {href}")

    except Exception as e:
        print(f"Ошибка обработки ссылки: {e}")

    # Подтверждение обработки сообщения
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Настройка потребителя
channel.basic_consume(queue=queue_name, on_message_callback=callback)

# Главный цикл
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Завершение работы.")
finally:
    connection.close()
