from streaming.plugins.kafka import Kafka


class Redpanda(Kafka):
    def __init__(self):
        super().__init__(stream_type="redpanda")
