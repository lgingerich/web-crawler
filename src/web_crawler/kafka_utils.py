from utils import logger
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException, KafkaError
from typing import Dict, Any, Optional
import json
import time
import socket
import logging

class KafkaLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if msg.startswith("%"):
            # Handle internal librdkafka debug messages
            level, timestamp, _, client, message = msg.split("|", 4)
            level = int(level[1:])  # Remove the % and convert to int
            if level <= 3:
                logger.debug(f"Kafka internal: {message.strip()}")
            elif level <= 5:
                logger.info(f"Kafka internal: {message.strip()}")
            else:
                logger.warning(f"Kafka internal: {message.strip()}")
        else:
            # Handle regular messages
            logger.info(f"Kafka: {msg}")

kafka_log_handler = KafkaLogHandler()
kafka_log_handler.setFormatter(logging.Formatter('%(message)s'))

class KafkaAdmin:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka admin client.

        :param bootstrap_servers: A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster
        """
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": 10000,  # 10 seconds
            "security.protocol": "PLAINTEXT",
            "logger": kafka_log_handler
        })

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> None:
        """
        Create a new Kafka topic.

        :param topic_name: Name of the topic to create
        :param num_partitions: Number of partitions for the topic
        :param replication_factor: Replication factor for the topic
        :return: None
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
        """
        Delete a Kafka topic.

        :param topic_name: Name of the topic to delete
        :param timeout: Operation timeout in seconds (default: 30.0)
        :return: None
        """
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
        
    def check_broker_availability(self, max_retries: int = 5, retry_delay: float = 2.0) -> bool:
        """
        Check if the Kafka broker is available with retries.

        :param max_retries: Maximum number of connection attempts
        :param retry_delay: Delay between retries in seconds
        :return: True if the broker is available, False otherwise
        """
        for attempt in range(max_retries):
            try:
                # First, try a simple socket connection
                host, port = self.bootstrap_servers.split(':')
                with socket.create_connection((host, int(port)), timeout=10):
                    pass

                # If socket connection succeeds, try listing topics
                cluster_metadata = self.admin_client.list_topics(timeout=10.0)
                logger.info("Successfully connected to Kafka broker")
                return True
            except (socket.error, KafkaException) as e:
                logger.warning(f"Failed to connect to Kafka broker (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts")
        return False


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka producer.

        :param bootstrap_servers: A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "api.version.request": True,
            "api.version.fallback.ms": 0,
            "socket.timeout.ms": 10000,  # 10 seconds
            "security.protocol": "PLAINTEXT",
            "logger": kafka_log_handler
        })

    def produce(self, topic: str, value: Any, key: Optional[str] = None) -> None:
        """
        Produce a message to a Kafka topic.

        :param topic: The topic to produce the message to
        :param value: The value of the message to be sent
        :param key: The key of the message (optional)
        :return: None
        """
        try:
            self.producer.produce(
                topic,
                key=key,
                value=json.dumps(value) if isinstance(value, dict) else value,
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def close(self):
        """
        Close the producer connection.
        """
        self.producer.flush()


class KafkaConsumer:
    def __init__(
        self, bootstrap_servers: str = "localhost:9092", group_id: str = "url_group"
    ):
        """
        Initialize the Kafka consumer.

        :param bootstrap_servers: A comma-separated list of host and port pairs that are the addresses of the Kafka brokers
        :param group_id: An identifier for the consumer group this consumer belongs to
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",  # Changed to 'false' for manual offset commit
            "api.version.request": True,
            "api.version.fallback.ms": 0,
            "socket.timeout.ms": 10000,  # 10 seconds
            "security.protocol": "PLAINTEXT",
            "logger": kafka_log_handler
        })

    def subscribe(self, topic: str) -> None:
        """
        Subscribe to a Kafka topic.

        :param topic: The name of the topic to subscribe to
        :return: None
        """
        self.consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}")

    def poll(self, timeout: float = 1.0) -> Dict[str, Any]:
        """
        Poll the consumer for messages.

        :param timeout: The maximum time to block waiting for a message
        :return: A dictionary containing the message details, or None if no message is available
        """
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            return None
        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8"),
        }

    def close(self):
        """
        Close the consumer connection.
        """
        self.consumer.close()
