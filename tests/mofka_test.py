from streaming import Stream
from streaming.plugins.mofka import Mofka


def test_stream_init(group_file="mofka.json"):

    topic = "test_streaming_api_2"

    framework = Mofka(group_file=group_file)

    assert framework.producer is None
    assert framework.consumer is None

    framework.create_producer()
    framework.create_consumer(topic=topic)

    assert framework.producer is not None
    assert framework.consumer is not None


def test_produce_and_consume(group_file="mofka.json"):
    topic = "test_streaming_api_2"

    framework = Mofka(group_file=group_file)
    framework.create_consumer(topic=topic)
    framework.create_producer()

    for i in range(0, 100):
        framework.producer.send(topic=topic, metadata=f"message-{i}")

    framework.producer.close()

    count = 0
    for i, m in enumerate(framework.consumer):
        if i == 99:
            break

    assert m.value == "message-99"


def test_framework_resolve():

    s = Stream(stream_type="mofka")
    assert isinstance(s, Mofka)
