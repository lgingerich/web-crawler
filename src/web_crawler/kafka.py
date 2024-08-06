from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaTopicManager:
    def __init__(self) -> None:
        pass

    def create_topics(
        self,
        topic_name: str,
        num_partitions: int,
        replication_factor: int,
        replication_assigment: list,
        config: dict,
        operation_timeout: float,
        request_timeout: float,
        validate_only: bool,
    ):
        pass

    def delete_topics(self, topic_name):
        pass

    def create_partitions(self):
        pass


class KafkaProducer:
    def __init__(self) -> None:
        pass


class KafkaConsumer:
    def __init__(self) -> None:
        pass
