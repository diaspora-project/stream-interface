import pytest
import os

import streaming
from streaming.plugins.kafka import Kafka
from streaming.plugins.octopus import Octopus
from streaming.plugins.redpanda import Redpanda
from streaming import Stream


@pytest.mark.parametrize(
    "StreamFramework, bootstrap_servers",
    [
        (Octopus, os.environ["OCTOPUS_BOOTSTRAP_SERVERS"]),
        (Kafka, ["localhost:9094"]),
        (Redpanda, ["localhost:19092"]),
    ],
)
def test_stream_init(StreamFramework, bootstrap_servers):

    topic = "test-streaming-api-2"

    framework = StreamFramework()

    assert framework.producer is None
    assert framework.consumer is None

    framework.create_producer(bootstrap_servers=bootstrap_servers)
    framework.create_consumer(topic=topic, bootstrap_servers=bootstrap_servers)

    assert framework.producer is not None
    assert framework.consumer is not None

    framework.close()


@pytest.mark.parametrize(
    "StreamFramework, bootstrap_servers",
    [
        (Octopus, os.environ["OCTOPUS_BOOTSTRAP_SERVERS"]),
        (Kafka, ["localhost:9094"]),
        (Redpanda, ["localhost:19092"]),
    ],
)
def test_produce_and_consume(StreamFramework, bootstrap_servers):
    topic = "test-streaming-api-2"

    framework = StreamFramework()
    framework.create_consumer(bootstrap_servers=bootstrap_servers, topic=topic)
    framework.create_producer(bootstrap_servers=bootstrap_servers)

    for i in range(0, 100):
        framework.producer.send(topic=topic, metadata=f"message-{i}")

    framework.producer.close()

    count = 0
    for i, m in enumerate(framework.consumer):
        if i == 99:
            break

    assert m.value == "message-99"

    framework.close()


def test_framework_resolve():

    s = Stream(stream_type="kafka")
    assert isinstance(s, Kafka)

    s = Stream(stream_type="octopus")
    assert isinstance(s, Octopus)

    s = Stream(stream_type="redpanda")
    assert isinstance(s, Redpanda)


@pytest.mark.parametrize(
    "config_file",
    [
        "tests/configs/kafka_config.toml",
        "tests/configs/redpanda_config.toml",
        "tests/configs/octopus_config.toml",
    ],
)
def test_config(config_file):
    topic = "test-streaming-api-2"
    s = streaming.from_config(config_file)

    for i in range(0, 100):
        s.producer.send(topic=topic, metadata=f"message-{i}")

    s.producer.close()

    count = 0
    for i, m in enumerate(s.consumer):
        if i == 99:
            break

    assert m.value == "message-99"

    s.close()
