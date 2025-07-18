from streaming.plugins.kafka import Kafka


class Octopus(Kafka):
    def __init__(self):
        super().__init__(stream_type="octopus")
