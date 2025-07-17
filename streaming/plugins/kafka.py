from __future__ import annotations

import json

from streaming.base import BaseStream
from streaming.base import Producer
from streaming.base import Consumer


def value_serializer(v):
    return json.dumps(v).encode("utf-8")


def value_deserializer(x):
    return json.loads(x.decode("utf-8"))


class Kafka(BaseStream):
    def __init__(self, streamtype: str = "kafka"):
        self.producer = None
        self.consumer = None
        self.type = streamtype

    def create_producer(self, *args, **kwargs) -> StreamKafkaProducer:
        self.producer = StreamKafkaProducer(self.type, *args, **kwargs)
        return self.producer

    def create_consumer(self, *args, **kwargs) -> StreamKafkaConsumer:
        self.consumer = StreamKafkaConsumer(self.type, *args, **kwargs)
        return self.consumer

    def close(self):
        self.producer.close()
        self.consumer.close()


class StreamKafkaProducer(Producer):

    def __init__(self, streamtype: str, value_serializer=value_serializer, **kwargs):

        if streamtype.lower() == "octopus":
            from diaspora_event_sdk import KafkaProducer
        else:
            from kafka import KafkaProducer

        self._service = KafkaProducer(value_serializer=value_serializer, **kwargs)

    def send(self, topic: str, metadata: str, data: str = None) -> None:
        self._service.send(topic, metadata)

    def flush(self) -> None:
        self._service.flush()

    def close(self) -> None:
        self._service.close()


class StreamKafkaConsumer(Consumer):

    def __init__(
        self,
        streamtype: str,
        topic: str,
        auto_offset_reset="earliest",
        value_deserializer=value_deserializer,
        consumer_timeout_ms=-1,
        **kwargs,
    ):
        if streamtype.lower() == "octopus":
            from diaspora_event_sdk import KafkaConsumer
        else:
            from kafka import KafkaConsumer

        self._service = KafkaConsumer(
            topic,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=value_deserializer,
            consumer_timeout_ms=consumer_timeout_ms,
            **kwargs,
        )

        max_attempts, attempts = 10, 0

        while not self._service.assignment() and attempts < max_attempts:
            self._service.poll(100)
            attempts += 1

        if not self._service.assignment():
            raise Exception(
                "consumer cannot acquire partition assignment; maybe the user has not registered the topic."
            )

    def __iter__(self):
        return self

    def __next__(self) -> str:
        return next(self._service)

    def close(self):
        self._service.close()
