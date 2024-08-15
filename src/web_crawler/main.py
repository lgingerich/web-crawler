import asyncio
import os
import time
import yaml
from urllib.parse import urlparse
from kafka_utils import KafkaAdmin, KafkaProducer, KafkaConsumer
from scraper import Scraper
from database import DatabaseManager
from utils import logger, fetch_tranco_list

# Load configuration from config.yaml
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Access configurations
KAFKA_CONFIG = config["kafka"]
SCRAPER_CONFIG = config["scraper"]
DB_CONFIG = config["database"]
TRANCO_CONFIG = config["tranco"]


async def setup_kafka(config):
    # Setup Kafka clients
    kafka_admin = KafkaAdmin(config["bootstrap_servers"])

    # Check if Kafka broker is available
    if not await kafka_admin.async_check_broker_availability(max_retries=5, retry_delay=2.0):
        logger.error("Kafka broker is not available. Exiting.")
        return None, None, None

    producer = KafkaProducer(config["bootstrap_servers"])
    consumer = KafkaConsumer(config)  # Pass the entire config dictionary

    # Create topic if it doesn't exist
    if not await kafka_admin.async_topic_exists(config["topic_name"]):
        await kafka_admin.async_create_topic(
            config["topic_name"], num_partitions=1, replication_factor=1
        )
    else:
        logger.info(f"Topic {config['topic_name']} already exists")

    return kafka_admin, producer, consumer

def setup_scraper(scraper_config, db):
    return Scraper(**scraper_config, db=db)


def load_tranco_list(tranco_config: dict):
    """
    Load the Tranco list from local cache or download if not available.
    
    :param tranco_config: Dictionary containing Tranco configuration (list_date, top_n, cache_dir)
    :return: List of domain names
    """
    list_date = tranco_config.get("list_date")
    top_n = tranco_config.get("top_n")
    cache_dir = tranco_config.get("cache_dir")

    file_path = os.path.join(cache_dir, f"tranco_top_{top_n}_{list_date}.txt")

    if not os.path.exists(file_path):
        file_path = fetch_tranco_list(list_date, top_n, cache_dir)
    
    with open(file_path, "r") as file:
        return [line.strip() for line in file.readlines()]



async def process_url(scraper, url):
    parsed_url = urlparse(url)
    filename = f"{parsed_url.netloc.replace('.', '_')}.html"
    filepath = os.path.join("scraped_data", filename)

    title, content, response_info = await scraper.scrape_url(url)

    if content:
        try:
            await scraper.save_html(content, filepath)
            hashed_content = scraper.hash_str(content)
            logger.info(f"Saved HTML content for {url} to {filepath}")

            status_code = response_info["status"]
            content_type = response_info["headers"].get("content-type", "text/html")

            await scraper.save_metadata(
                url,
                title,
                hashed_content,
                filename,
                True,
                status_code,
                content_type,
            )
            logger.info(f"Successfully processed {url}")
        except IOError as e:
            logger.error(f"Failed to save HTML for {url}: {e}")
            await scraper.save_metadata(url, title, None, filename, False, 500, None)
    else:
        status_code = response_info.get("status", 404)
        error_message = response_info.get("error", "Unknown error")
        error_type = response_info.get("error_type", "unknown")

        logger.warning(f"No content retrieved for {url}")
        logger.error(f"Error scraping {url}: {error_message}")
        logger.info(f"Error type: {error_type}, Status code: {status_code}")

        await scraper.save_metadata(url, None, None, filename, False, status_code, None)

    logger.info(f"Finished processing {url}")


async def produce_urls(producer, topic, urls):
    try:
        for url in urls:
            await producer.async_produce(topic, value=url)
            logger.info(f"Produced URL: {url}")

        # Flush after producing all URLs
        messages_in_queue = await producer.async_flush()
        logger.info(f"All URLs have been produced. Messages still in queue: {messages_in_queue}")
    except Exception as e:
        logger.error(f"Error in produce_urls: {e}")
        raise


async def consume_and_process(consumer, scraper, topic):
    await consumer.async_subscribe(topic)
    
    try:
        while True:
            msg = await consumer.async_poll(1.0)
            if msg:
                # Process the message
                await process_url(scraper, msg['value'])
                await consumer.async_commit()
    except asyncio.CancelledError:
        logger.info("Consume and process task was cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in consume_and_process: {e}", exc_info=True)
    finally:
        await consumer.async_close()


async def run_kafka_scraper(producer, consumer, scraper, urls, kafka_config):
    try:
        producer_task = asyncio.create_task(
            produce_urls(producer, kafka_config["topic_name"], urls)
        )
        consumer_task = asyncio.create_task(
            consume_and_process(consumer, scraper, kafka_config["topic_name"])
        )

        # Wait for the producer to finish
        await producer_task
        logger.info("Producer task completed")

        # Let the consumer run for a set time or until a condition is met
        try:
            await asyncio.wait_for(consumer_task, timeout=3600)  # 1 hour timeout
        except asyncio.TimeoutError:
            logger.info("Consumer task timed out")

    except asyncio.CancelledError:
        logger.info("Kafka scraper tasks were cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in run_kafka_scraper: {e}", exc_info=True)
    finally:
        await producer.async_close()
        await consumer.async_close()


async def cleanup(producer, consumer, db):
    if producer:
        await producer.async_close()
    if consumer:
        await consumer.async_close()
    if db:
        await db.async_close()
    logger.info("Web crawler shutdown complete.")


async def main():
    try:
        # Kafka Setup
        kafka_admin, producer, consumer = await setup_kafka(KAFKA_CONFIG)
        if not all([kafka_admin, producer, consumer]):
            return

        # Database Setup
        db = DatabaseManager(**DB_CONFIG)
        await db.async_create_pool()  # Create the connection pool
        await db.async_create_table()  # Ensure the table exists
        

        # Get list of URLs to crawl with Tranco configuration
        url_list = load_tranco_list(TRANCO_CONFIG)
        print(f"Loaded {len(url_list)} domains from Tranco list")

        # Scraper Setup
        scraper = setup_scraper(SCRAPER_CONFIG, db)

        # Run Kafka-integrated scraper
        await run_kafka_scraper(producer, consumer, scraper, url_list, KAFKA_CONFIG)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}", exc_info=True)
    finally:
        await cleanup(producer, consumer, db)


if __name__ == "__main__":
    asyncio.run(main())
