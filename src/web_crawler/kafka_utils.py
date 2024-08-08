from utils import setup_logger
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, KafkaException
from typing import Dict, Any

# Setup logging
logger = setup_logger()

class KafkaManager:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        # self.producer = None
        # self.consumer = None

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> None:
        """
        Create a new Kafka topic.

        :param admin_client: AdminClient instance
        :param topic_name: Name of the topic to create
        :param num_partitions: Number of partitions for the topic
        :param replication_factor: Replication factor for the topic
        """
        topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        fs = self.admin_client.create_topics([topic])

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")

    def delete_topic(self, topic_name: str, timeout: float = 30.0) -> None:
        """Delete a Kafka topic."""
        try:
            fs = self.admin_client.delete_topics(
                [topic_name], operation_timeout=timeout
            )

            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Topic '{topic}' deleted successfully")
                except KafkaException as e:
                    logger.error(f"Failed to delete topic '{topic}': {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while trying to delete topic '{topic_name}': {e}"
            )

    def topic_exists(self, topic_name: str) -> bool:
        """
        Check if a topic exists in the Kafka cluster.

        :param admin_client: AdminClient instance
        :param topic_name: Name of the topic to check
        :return: True if the topic exists, False otherwise
        """
        try:
            topics = self.admin_client.list_topics(timeout=5)
            return topic_name in topics.topics
        except Exception as e:
            logger.error(f"Failed to check topic existence: {e}")
            return False

    def get_topic_config(self, topic_name: str) -> Dict[str, Any]:
        """Get the configuration of a specific topic."""
        try:
            resource = self.admin_client.describe_configs(
                [ConfigResource(ConfigResource.Type.TOPIC, topic_name)]
            )
            return {key: config.value for key, config in resource[0].items()}
        except Exception as e:
            logger.error(f"Failed to get config for topic '{topic_name}': {e}")
            return {}
