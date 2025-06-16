from __future__ import annotations

import json

from diaspora_event_sdk import KafkaConsumer
from diaspora_event_sdk import KafkaProducer

from streaming.base import Streaming
from streaming.base import Producer
from streaming.base import Consumer


def value_serializer(v):
    return json.dumps(v).encode("utf-8")


def value_deserializer(x):
    return json.loads(x.decode("utf-8"))


class Octopus(Streaming):
    def __init__(self, *args, **kwargs):
        self.producer = None
        self.consumer = None
        self.args = args
        self.kwargs = kwargs

    def producer(self, *args, **kwargs) -> OctopusProducer:
        self.producer = OctopusProducer(*args, **kwargs)

    def consumer(self, *args, **kwargs) -> OctopusConsumer:
        self.consumer = OctopusConsumer(*args, **kwargs)


class OctopusProducer(Producer):

    def __init__(self, value_serializer):
        self.producer = KafkaProducer(value_serializer=value_serializer)

    def send(self, topic: str, message: str) -> None:
        self.producer.send(topic, message)

    def flush(self) -> None:
        self.producer.flush()


class OctopusConsumer(Consumer):

    def __init__(self, topic: str, auto_offset_reset="earliest"):
        self.consumer = KafkaConsumer(topic=topic, auto_offset_reset=auto_offset_reset)

    def __iter__(self):
        return self

    def __next__(self) -> str:
        message = next(self.consumer)

    def close(self):
        self.consumer.close()
