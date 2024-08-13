import asyncio
import os
import time
import yaml
from urllib.parse import urlparse
from kafka_utils import KafkaAdmin, KafkaProducer, KafkaConsumer
from scraper import Scraper
from utils import logger

# Load configuration from config.yaml
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Access configurations
KAFKA_CONFIG = config['kafka']
SCRAPER_CONFIG = config['scraper']

def setup_kafka(config):
    # Setup Kafka clients
    kafka_admin = KafkaAdmin(config["bootstrap_servers"])
    
    # Check if Kafka broker is available
    if not kafka_admin.check_broker_availability(max_retries=5, retry_delay=2.0):
        logger.error("Kafka broker is not available. Exiting.")
        return None, None, None

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
                await scraper.save_metadata(url, title, hashed_content, filepath, True)
                logger.info(f"Successfully processed {url}")
            except IOError as e:
                logger.error(f"Failed to save HTML for {url}: {e}")
                await scraper.save_metadata(url, title, None, filepath, False)
        else:
            logger.warning(f"No content retrieved for {url}")
            await scraper.save_metadata(url, None, None, filepath, False)

    except asyncio.CancelledError:
        logger.info(f"Processing of {url} was cancelled")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing {url}: {e}", exc_info=True)
        await scraper.save_metadata(url, None, None, filepath, False)

async def produce_urls(producer, topic, urls):
    for url in urls:
        producer.produce(topic, value=url)
        logger.info(f"Produced URL: {url}")
    producer.close()

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
    try:
        producer_task = asyncio.create_task(
            produce_urls(producer, kafka_config["topic_name"], scraper_config["urls"])
        )
        consumer_task = asyncio.create_task(
            consume_and_process(consumer, scraper, kafka_config["topic_name"])
        )

        await producer_task
        await asyncio.wait_for(consumer_task, timeout=60)  # Adjust timeout as needed
    except asyncio.TimeoutError:
        logger.info("Consumer task timed out. Shutting down.")
    except asyncio.CancelledError:
        logger.info("Kafka scraper tasks were cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in run_kafka_scraper: {e}", exc_info=True)
    finally:
        producer.close()
        consumer.close()

def main():
    try:
        # Kafka Setup
        kafka_admin, producer, consumer = setup_kafka(KAFKA_CONFIG)
        if not all([kafka_admin, producer, consumer]):
            return

        # Scraper Setup
        scraper = setup_scraper(SCRAPER_CONFIG)

        # Run Kafka-integrated scraper
        asyncio.run(
            run_kafka_scraper(producer, consumer, scraper, KAFKA_CONFIG, SCRAPER_CONFIG)
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}", exc_info=True)
    finally:
        # Cleanup
        if 'producer' in locals():
            producer.close()
        if 'consumer' in locals():
            consumer.close()
        logger.info("Kafka scraper shutdown complete.")

if __name__ == "__main__":
    main()