from utils import logger
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from typing import Dict, Any, Optional
import json
import time
import socket
import logging
import asyncio

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
kafka_log_handler.setFormatter(logging.Formatter("%(message)s"))


class KafkaAdmin:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
                "socket.timeout.ms": 10000,  # 10 seconds
                "security.protocol": "PLAINTEXT",
                "logger": kafka_log_handler,
            }
        )

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> None:
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

    async def async_create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> None:
        await asyncio.get_running_loop().run_in_executor(
            None, self.create_topic, topic_name, num_partitions, replication_factor
        )

    def delete_topic(self, topic_name: str, timeout: float = 30.0) -> None:
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

    async def async_delete_topic(self, topic_name: str, timeout: float = 30.0) -> None:
        await asyncio.get_running_loop().run_in_executor(
            None, self.delete_topic, topic_name, timeout
        )

    def topic_exists(self, topic_name: str) -> bool:
        try:
            topics = self.admin_client.list_topics(timeout=5)
            return topic_name in topics.topics
        except Exception as e:
            logger.error(f"Failed to check topic existence: {e}")
            return False

    async def async_topic_exists(self, topic_name: str) -> bool:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.topic_exists, topic_name
        )

    def check_broker_availability(
        self, max_retries: int = 5, retry_delay: float = 2.0
    ) -> bool:
        for attempt in range(max_retries):
            try:
                host, port = self.bootstrap_servers.split(":")
                with socket.create_connection((host, int(port)), timeout=10):
                    pass

                self.admin_client.list_topics(timeout=10.0)
                logger.info("Successfully connected to Kafka broker")
                return True
            except (socket.error, KafkaException) as e:
                logger.warning(
                    f"Failed to connect to Kafka broker (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts")
        return False

    async def async_check_broker_availability(
        self, max_retries: int = 5, retry_delay: float = 2.0
    ) -> bool:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.check_broker_availability, max_retries, retry_delay
        )


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka producer.

        :param bootstrap_servers: A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "api.version.request": True,
                "api.version.fallback.ms": 0,
                "socket.timeout.ms": 10000,  # 10 seconds
                "security.protocol": "PLAINTEXT",
                "logger": kafka_log_handler,
            }
        )

    def produce(self, topic: str, value: Any, key: Optional[str] = None) -> None:
        try:
            self.producer.produce(
                topic,
                key=key,
                value=json.dumps(value) if isinstance(value, dict) else value,
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    async def async_produce(self, topic: str, value: Any, key: Optional[str] = None) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.produce, topic, value, key)

    def flush(self, timeout: Optional[float] = None) -> int:
        return self.producer.flush(timeout if timeout is not None else -1)

    async def async_flush(self, timeout: Optional[float] = None) -> int:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.flush, timeout)

    def close(self):
        self.producer.flush()
        # self.producer.close()

    async def async_close(self):
        await self.async_flush()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)


class KafkaConsumer:
    def __init__(self, config):
        self.consumer = Consumer({
            "bootstrap.servers": config["bootstrap_servers"],
            "group.id": config["group_id"],
            "auto.offset.reset": config["auto_offset_reset"],
            "enable.auto.commit": str(config["enable_auto_commit"]).lower(),
            "max.poll.interval.ms": config["max_poll_interval_ms"],
            "session.timeout.ms": config["session_timeout_ms"],
            "socket.timeout.ms": config["socket_timeout_ms"],
        })
        self.subscribed = False

    def subscribe(self, topic: str) -> None:
        self.consumer.subscribe([topic])
        self.subscribed = True
        logger.info(f"Subscribed to topic: {topic}")

    async def async_subscribe(self, topic: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.subscribe, topic)

    def poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        if not self.subscribed:
            raise RuntimeError("Consumer is not subscribed to any topics")

        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info("Reached end of partition")
            else:
                logger.error(f"Error while polling: {msg.error()}")
            return None

        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8"),
        }

    async def async_poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.poll, timeout)

    def close(self):
        self.consumer.close()

    async def async_close(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    def commit(self):
        self.consumer.commit()

    async def async_commit(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.commit)