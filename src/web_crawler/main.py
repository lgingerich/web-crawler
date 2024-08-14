import asyncio
import os
import time
import yaml
from urllib.parse import urlparse
from kafka_utils import KafkaAdmin, KafkaProducer, KafkaConsumer
from scraper import Scraper
from database import DatabaseManager
from utils import logger, fetch_tranco_list
from confluent_kafka import KafkaError

# Load configuration from config.yaml
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Access configurations
KAFKA_CONFIG = config["kafka"]
SCRAPER_CONFIG = config["scraper"]
DB_CONFIG = config["database"]


def setup_kafka(config):
    # Setup Kafka clients
    kafka_admin = KafkaAdmin(config["bootstrap_servers"])

    # Check if Kafka broker is available
    if not kafka_admin.check_broker_availability(max_retries=5, retry_delay=2.0):
        logger.error("Kafka broker is not available. Exiting.")
        return None, None, None

    producer = KafkaProducer(config["bootstrap_servers"])
    consumer = KafkaConsumer(config)  # Pass the entire config dictionary

    # Create topic if it doesn't exist
    if not kafka_admin.topic_exists(config["topic_name"]):
        kafka_admin.create_topic(
            config["topic_name"], num_partitions=1, replication_factor=1
        )
    else:
        logger.info(f"Topic {config['topic_name']} already exists")

    return kafka_admin, producer, consumer


def setup_scraper(scraper_config, db):
    return Scraper(**scraper_config, db=db)


def load_tranco_list(
    list_date: str = "2024-01-01", top_n: int = 10000, cache_dir: str = "url_data"
):
    """
    Load the Tranco list from local cache or download if not available.

    :param list_date: Date of the list to fetch ('latest' or YYYY-MM-DD format)
    :param top_n: Number of top domains to fetch
    :param cache_dir: Directory to save/load the Tranco list
    :return: List of domain names
    """
    file_name = f"tranco_top_{top_n}_{list_date}.txt"
    file_path = os.path.join(cache_dir, file_name)

    if not os.path.exists(file_path):
        logger.info("Tranco list not found locally. Downloading...")
        file_path = fetch_tranco_list(list_date, top_n, cache_dir)

    with open(file_path, "r") as f:
        domains = [line.strip() for line in f]

    return domains


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
    for url in urls:
        producer.produce(topic, value=url)
        logger.info(f"Produced URL: {url}")
    producer.close()


async def consume_and_process(consumer, scraper, topic):
    consumer.subscribe(topic)
    
    messages_processed = 0
    end_of_stream = False

    try:
        while not end_of_stream:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg == {}:  # Empty dictionary indicates no message
                continue

            if 'error' in msg:
                if msg['error'] == KafkaError._PARTITION_EOF:
                    logger.info('End of partition reached')
                    end_of_stream = True
                else:
                    logger.error(f"Error: {msg['error']}")
                    break
            else:
                url = msg['value']
                logger.info(f"Processing URL: {url}")
                await process_url(scraper, url)
                messages_processed += 1

                if messages_processed % 100 == 0:
                    logger.info(f"Processed {messages_processed} messages so far")

    except asyncio.CancelledError:
        logger.info("Consume and process task was cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in consume_and_process: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Processed {messages_processed} messages in total")


async def run_kafka_scraper(producer, consumer, scraper, urls, kafka_config):
    try:
        producer_task = asyncio.create_task(
            produce_urls(producer, kafka_config["topic_name"], urls)
        )
        consumer_task = asyncio.create_task(
            consume_and_process(consumer, scraper, kafka_config["topic_name"])
        )

        await producer_task
        await consumer_task  # Remove the timeout here
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

        # Database Setup
        db = DatabaseManager(**DB_CONFIG)
        db.create_table()  # Ensure the table exists

        # Get list of URLs to crawl
        url_list = load_tranco_list()
        print(f"Loaded {len(url_list)} domains from Tranco list")

        # Scraper Setup
        scraper = setup_scraper(SCRAPER_CONFIG, db)

        # Run Kafka-integrated scraper
        asyncio.run(
            run_kafka_scraper(producer, consumer, scraper, url_list, KAFKA_CONFIG)
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}", exc_info=True)
    finally:
        # Cleanup
        if "producer" in locals():
            producer.close()
        if "consumer" in locals():
            consumer.close()
        if "db" in locals():
            db.close()
        logger.info("Kafka scraper shutdown complete.")


if __name__ == "__main__":
    main()
