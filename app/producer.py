import random
import time

import confluent_kafka
import confluent_kafka as kafka
from confluent_kafka.serialization import SerializationError

from common import fake_message

CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'acks': 'all',
    'retries': 3
}
TOPIC_NAME = 'topic-1'
QUEUE_FULL_TIMEOUT = 5

producer = kafka.Producer(CONFIG)

while True:
    message = fake_message(random.randint(3, 10))
    print(message)

    try:
        raw = message.serialize()
    except SerializationError as error:
        print(f'Message serialization error, msg: {message}, error: {error}')
        continue

    try:
        producer.produce(topic=TOPIC_NAME, value=raw)
    except BufferError as error:
        print(f'Internal producer message queue is full, error: {error}')
        time.sleep(QUEUE_FULL_TIMEOUT)
    except confluent_kafka.KafkaException as error:
        print(f'Something went wrong, error: {error}')
        print(f'Message not sent, message: {message}')
        raise

    time.sleep(random.uniform(0.1, 2.0))
