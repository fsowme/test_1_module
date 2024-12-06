import logging
import random
import time

import confluent_kafka
import confluent_kafka as kafka
from confluent_kafka.serialization import SerializationError

from common import fake_message

logger = logging.getLogger(__name__)

CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'acks': 'all',  # ждем подтверждение от всех синхронизированных в данный момент реплик
    'retries': 3,  # 3 попытки отправки при ошибках
}
TOPIC_NAME = 'topic-1'
QUEUE_FULL_TIMEOUT = 5  # время ожидания в случае заполненности очереди продюсера (секунды)

producer = kafka.Producer(CONFIG)

while True:
    message = fake_message(random.randint(3, 10))
    print(message)

    try:
        raw = message.serialize()
    except SerializationError as error:
        logger.error('Message serialization error, msg: %s, error: %s', message, error)
        continue

    try:
        producer.produce(topic=TOPIC_NAME, value=raw)
    except BufferError as error:
        logger.error('Internal producer message queue is full, error: %s', error)
        time.sleep(QUEUE_FULL_TIMEOUT)
    except confluent_kafka.KafkaException as error:
        logger.error('Something went wrong, message:%s, error: %s', message, error)
        raise

    time.sleep(random.uniform(0.1, 2.0))
