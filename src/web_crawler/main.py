import asyncio
import os
import time
from urllib.parse import urlparse
from kafka_utils import KafkaAdmin, KafkaProducer, KafkaConsumer
from scraper import Scraper
from utils import logger

# Configuration
KAFKA_CONFIG = {
    "topic_name": "url_queue",
    "bootstrap_servers": "localhost:9092",
    "group_id": "scraper_group",
}

SCRAPER_CONFIG = {
    "urls": [
        "https://google.com",
        "https://amazon.com",
        "https://example.com",
        "https://dell.com",
    ],
    "headless": True,
    "slow_mo": 0,  # in milliseconds
    "metadata_file": os.path.join("scraped_data", "metadata.json"),
}


def setup_kafka(config):
    # Setup Kafka clients
    kafka_admin = KafkaAdmin(config["bootstrap_servers"])
    producer = KafkaProducer(config["bootstrap_servers"])
    consumer = KafkaConsumer(config["bootstrap_servers"], config["group_id"])

    # Create topic if it doesn't exist
    if not kafka_admin.topic_exists(config["topic_name"]):
        kafka_admin.create_topic(
            config["topic_name"], num_partitions=1, replication_factor=1
        )
    else:
        logger.info(f"Topic {config['topic_name']} already exists")

    return kafka_admin, producer, consumer


def setup_scraper(config):
    return Scraper(
        headless=config["headless"],
        slow_mo=config["slow_mo"],
        metadata_file=config["metadata_file"],
    )


async def process_url(scraper, url):
    try:
        ########### Probably better way to handle this ############
        # Prepare filename and filepath
        parsed_url = urlparse(url)
        filename = f"{parsed_url.netloc.replace('.', '_')}.html"
        filepath = os.path.join("scraped_data", filename)

        # Scrape URL
        title, content = await scraper.scrape_url(url)

        # Save content and metadata
        if content:
            try:
                await scraper.save_html(content, filepath)
                hashed_content = scraper.hash_str(content)
                logger.info(f"Saved HTML content for {url} to {filepath}")
            except IOError as e:
                logger.error(f"Failed to save HTML for {url}: {e}")
                await scraper.save_metadata(url, title, None, filepath, False)
                return

            await scraper.save_metadata(url, title, hashed_content, filepath, True)
            logger.info(f"Successfully processed {url}")
        else:
            logger.warning(f"No content retrieved for {url}")
            await scraper.save_metadata(url, None, None, filepath, False)

    except Exception as e:
        logger.error(f"Unexpected error processing {url}: {e}")
        await scraper.save_metadata(url, None, None, filepath, False)


async def produce_urls(producer, topic, urls):
    for url in urls:
        producer.produce(topic, value=url)
        logger.info(f"Produced URL: {url}")
    producer.flush()


async def consume_and_process(consumer, scraper, topic):
    consumer.subscribe(topic)
    last_message_time = time.time()
    timeout = 30  # 30 seconds timeout

    try:
        while True:
            logger.debug("Before poll")
            msg = consumer.poll(1.0)
            logger.debug(f"After poll: {msg}")

            current_time = time.time()
            if msg is None:
                if current_time - last_message_time > timeout:
                    logger.info(f"No new message for {timeout} seconds. Exiting.")
                    break
                continue

            # Reset the last message time
            last_message_time = current_time

            if isinstance(msg, dict):
                if "value" in msg:
                    url = msg["value"]
                    if isinstance(url, bytes):
                        url = url.decode("utf-8")
                    logger.info(f"Received message: {msg}")
                    logger.info(f"Processed URL: {url}")
                    await process_url(scraper, url)
                else:
                    logger.warning(f"Message has no 'value' key: {msg}")
            else:
                logger.warning(f"Unexpected message format: {msg}")
    except asyncio.CancelledError:
        logger.info("Consume and process task was cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in consume_and_process: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info("Consumer closed")


async def run_kafka_scraper(producer, consumer, scraper, kafka_config, scraper_config):
    producer_task = asyncio.create_task(
        produce_urls(producer, kafka_config["topic_name"], scraper_config["urls"])
    )
    consumer_task = asyncio.create_task(
        consume_and_process(consumer, scraper, kafka_config["topic_name"])
    )

    # Wait for both tasks to start
    await asyncio.sleep(1)

    # Wait for producer to finish
    await producer_task

    # Give consumer some time to process messages
    await asyncio.sleep(10)  # Adjust this time as needed

    # Cancel consumer task
    consumer_task.cancel()

    try:
        await consumer_task
    except asyncio.CancelledError:
        print("Consumer task was cancelled")


def main():
    # Kafka Setup
    kafka_admin, producer, consumer = setup_kafka(KAFKA_CONFIG)

    # Scraper Setup
    scraper = setup_scraper(SCRAPER_CONFIG)

    # Run Kafka-integrated scraper
    asyncio.run(
        run_kafka_scraper(producer, consumer, scraper, KAFKA_CONFIG, SCRAPER_CONFIG)
    )

    # Cleanup
    producer.flush()
    consumer.close()


if __name__ == "__main__":
    main()
