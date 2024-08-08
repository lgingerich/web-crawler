import asyncio
import logging
import os
from urllib.parse import urlparse
from kafka_utils import KafkaManager
from scraper import Scraper
from utils import setup_logger

# Setup logging
logger = setup_logger()

# Configuration
KAFKA_CONFIG = {
    "topic_name": "url_queue",
    "bootstrap_servers": "localhost:9092",
}

SCRAPER_CONFIG = {
    "urls": ["google.com", "amazon.com", "example.com", "dell.com"],
    "headless": True,
    "slow_mo": 0,  # in milliseconds
    "metadata_file": os.path.join("scraped_data", "metadata.json"),
}

def setup_kafka(config):
    kafka_manager = KafkaManager(config["bootstrap_servers"])
    if not kafka_manager.topic_exists(config["topic_name"]):
        kafka_manager.create_topic(
            config["topic_name"], num_partitions=1, replication_factor=1
        )
    else:
        logger.info(f"Topic {config['topic_name']} already exists")
    return kafka_manager


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


async def run_scraper(scraper, urls):
    tasks = [process_url(scraper, url) for url in urls]
    await asyncio.gather(*tasks)


def main():
    # Kafka Setup
    kafka_manager = setup_kafka(KAFKA_CONFIG)

    # Scraper Setup
    scraper = setup_scraper(SCRAPER_CONFIG)

    # Run scraper
    asyncio.run(run_scraper(scraper, SCRAPER_CONFIG["urls"]))


if __name__ == "__main__":
    main()
