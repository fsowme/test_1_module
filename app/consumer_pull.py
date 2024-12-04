from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationError

from common import Message

CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'consumer_pull',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}
TOPIC_NAME = 'topic-1'

consumer = Consumer(CONFIG)
consumer.subscribe([TOPIC_NAME])


try:
    while True:
        msg = consumer.poll(0.5)

        if msg is None:
            continue

        error = msg.error()
        if error is not None:
            print(f"Ошибка: {error}")
            continue

        value = msg.value()

        try:
            message = Message.deserialize(value)
        except SerializationError as error:
            print(f'Message deserialization error, msg: {value}, error: {error}')
            consumer.commit(asynchronous=False)
            continue


        print(f"Получено сообщение: {message}, offset={msg.offset()}, partition={msg.partition()}")
        consumer.commit(asynchronous=False)

finally:
    consumer.close()
