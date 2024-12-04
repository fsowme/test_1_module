from confluent_kafka import Consumer
from confluent_kafka.error import SerializationError

from common import Message

CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'consumer_push',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'fetch.min.bytes': 1024,
}
TOPIC_NAME = 'topic-1'

consumer = Consumer(CONFIG)
consumer.subscribe([TOPIC_NAME])


try:
    while True:
        msg = consumer.poll()

        error = msg.error()
        if error is not None:
            print(f"Ошибка: {error}")
            continue

        value = msg.value()

        try:
            message = Message.deserialize(value)
        except SerializationError as error:
            print(f'Message deserialization error, msg: {value}, error: {error}')
            continue
        print(f"Получено сообщение: {message}, offset={msg.offset()}, partition={msg.partition()}")

finally:
    consumer.close()
