#!/usr/bin/env python3
import json
import time
from confluent_kafka import Producer

BROKERS = "rc1a-13m8vuuiie3pfbse.mdb.yandexcloud.net:9091"  
TOPIC = "loan-json-topic"
USERNAME = "producer"
PASSWORD = "Ekaterina2003!"  
CA_PATH = "YandexInternalRootCA.crt"

conf = {
    'bootstrap.servers': BROKERS,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': CA_PATH,
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': USERNAME,
    'sasl.password': PASSWORD,
}

def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка: {err}')
    else:
        print(f'Отправлено: {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

producer = Producer(conf)

# Загружаем твой подготовленный JSON-файл (kafka_ready_data.json)
with open('kafka_ready_data.json', 'r') as f:
    messages = json.load(f)

messages = messages[:5000]  #Обрезала, так как очень долго (это тоже норм, так как мой файл больше 100 МБ)

print(f"Отправка {len(messages)} сообщений в топик {TOPIC}...")

for i, msg in enumerate(messages):
    producer.produce(TOPIC, value=json.dumps(msg), callback=delivery_report)
    if (i + 1) % 100 == 0:
        print(f"Отправлено {i+1} / {len(messages)}")
    time.sleep(0.01)  # небольшая пауза

producer.flush()
print(f"Готово. Отправлено {len(messages)} сообщений")