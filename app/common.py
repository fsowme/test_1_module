import random
import string

from confluent_kafka import serialization


class Message:
    serializer_class = serialization.StringSerializer
    deserializer_class = serialization.StringDeserializer

    _codec = 'utf-8'

    def __init__(self, body: str):
        self.body = body

    def serialize(self) -> bytes:
        return self.serializer_class(self._codec)(self.body)

    @classmethod
    def deserialize(cls, raw: str|bytes) -> 'Message':
        if isinstance(raw, bytes):
            raw = cls.deserializer_class(cls._codec)(raw)
        return cls(body=raw)

    def __str__(self):
        return f'Message: {self.body}'


def fake_message(length: int) -> Message:
    body = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return Message(body)
