from __future__ import annotations

import cloudpickle
import json
import threading
import uuid

try:
    import pymargo.core
    from mochi.mofka.client import AdaptiveBatchSize
    from mochi.mofka.client import ByteArrayAllocator
    from mochi.mofka.client import DataDescriptor
    from mochi.mofka.client import FullDataSelector
    from mochi.mofka.client import MofkaDriver
    from mochi.mofka.client import Ordering
    from mochi.mofka.client import ThreadPool

    mofka_import_error = None
except ImportError as e:
    mofka_import_error = e

from streaming.base import BaseStream
from streaming.base import Producer
from streaming.base import Consumer


class Singleton(type):
    """Singleton class for the Mofka Driver.

    There can only be one driver per process leading to the need for this singleton.

    Args:
        group_file: Bedrock generated group file.
    """

    _instance = None
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
                    cls._instance.driver = MofkaDriver(*args, **kwargs)
        return cls._instance


class MofkaStreamDriver(metaclass=Singleton):
    driver: MofkaDriver

    def __init__(self, group_file, use_progress_thread=True):
        pass


class Mofka(BaseStream):
    def __init__(self, group_file, use_progress_threads=True, **kwargs):
        if mofka_import_error is not None:  # pragma: no cover
            raise mofka_import_error

        self._driver = MofkaStreamDriver(
            group_file=group_file, use_progress_thread=use_progress_threads, **kwargs
        ).driver

        self.producer = None
        self.consumer = None

    def create_producer(self, *args, **kwargs) -> MofkaProducer:
        self.producer = MofkaProducer(*args, driver=self._driver, **kwargs)

    def create_consumer(self, *args, **kwargs) -> MofkaConsumer:
        self.consumer = MofkaConsumer(*args, driver=self._driver, **kwargs)


class MofkaProducer(Producer):

    def __init__(self, driver, **kwargs):
        self._topics: dict = {}
        self._producers: dict = {}
        self._driver = driver

    def send(self, topic: str, metadata: str, data: str = None) -> None:
        batch_size = AdaptiveBatchSize
        ordering = Ordering.Strict

        if topic not in self._topics.keys():
            open_topic = self._driver.open_topic(topic)

            producer = open_topic.producer(
                batch_size=batch_size,
                ordering=ordering,
                thread_pool=ThreadPool(0),
            )
            self._topics[topic] = open_topic
            self._producers[topic] = producer

        else:
            producer = self._producers[topic]

            if data is None:
                producer.push(
                    metadata=metadata,
                    data=cloudpickle.dumps(""),
                )
            else:
                producer.push(metadata=metadata, data=cloudpickle.dumps(data))

        # TODO: figure out how to properly batch in mofka
        producer.flush()

    def flush(self) -> None:
        for p in self._producers.values():
            p.flush()

    def close(self) -> None:
        """Close this publisher."""
        del self._topics
        del self._producers


class MofkaConsumer(Consumer):

    def __init__(self, driver, topic: str, auto_offset_reset="earliest", **kwargs):
        self._driver = driver

        if "subscriber_name" in kwargs:
            subscriber_name = kwargs["subscriber_name"]
        else:
            subscriber_name = f"mofka-sub-{uuid.uuid4()}"
        self._topic = self._driver.open_topic(topic)
        self.consumer = self._topic.consumer(
            name=subscriber_name,
            thread_pool=ThreadPool(0),
        )

    def __iter__(self):
        return self

    def __next__(self) -> str:
        message = next(self.consumer)

    def close(self):
        self.consumer.close()
