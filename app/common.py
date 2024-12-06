import dataclasses
import logging
import random
import string
import typing

from confluent_kafka import Consumer, serialization
from confluent_kafka.serialization import SerializationError

logger = logging.getLogger(__name__)


class Message:
    class DeserializationError(Exception):
        pass

    serializer_class = serialization.StringSerializer
    deserializer_class = serialization.StringDeserializer

    _codec = 'utf-8'

    def __init__(self, body: str):
        self.body = body

    def serialize(self) -> bytes:
        return self.serializer_class(self._codec)(self.body)

    @classmethod
    def deserialize(cls, raw: bytes) -> 'Message':
        try:
            return cls.deserializer_class(cls._codec)(raw)
        except (SerializationError, UnicodeDecodeError) as error:
            raise cls.DeserializationError() from error

    def __str__(self):
        return f'Message: {self.body}'


def fake_message(length: int) -> Message:
    body = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return Message(body)


@dataclasses.dataclass
class Config:
    group__id: str
    auto__offset__reset : str  # начинаем читать сообщения с начала
    enable__auto__commit: bool  # не коммитим смещение автоматически при получении
    bootstrap__servers: str = 'localhost:9094'
    fetch__min__bytes: int = 1

    def to_dict(self) -> dict:
        return {
            field.name.replace('__', '.'): getattr(self, field.name, field.default)
            for field in dataclasses.fields(self)
        }


class ConsumeJob:
    def __init__(self, config: Config):
        self.config = config
        self.consumer = Consumer(self.config.to_dict())

    def run(self, topic, timeout: int|float = None) -> typing.Generator[Message, None, None]:
        self.consumer.subscribe([topic])
        while True:
            kafka_message = self.consumer.poll(timeout=timeout or -1)
            if kafka_message is None:
                continue

            error = kafka_message.error()
            if error is not None:
                logger.error(f'Error receiving message: %s', error)
                continue

            value = kafka_message.value()

            try:
                yield Message.deserialize(value)
            except Message.DeserializationError as error:
                logger.error(f'Message deserialization error, msg: %s, error: %s', value, error)
                continue
            finally:
                if not self.config.enable__auto__commit:
                    self.consumer.commit(asynchronous=False)

    def __enter__(self) -> 'ConsumeJob':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()
