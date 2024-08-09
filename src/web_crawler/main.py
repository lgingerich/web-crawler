import asyncio
import os
from urllib.parse import urlparse
from kafka_utils import KafkaAdmin, KafkaProducer, KafkaConsumer
from scraper import Scraper
from utils import setup_logger

# Setup logging
logger = setup_logger()

# Configuration
KAFKA_CONFIG = {
    "topic_name": "url_queue",
    "bootstrap_servers": "localhost:9092",
    "group_id": "scraper_group",
}

SCRAPER_CONFIG = {
    "urls": ["https://google.com", "https://amazon.com", "https://example.com", "https://dell.com"],
    "headless": True,
    "slow_mo": 0,  # in milliseconds
    "metadata_file": os.path.join("scraped_data", "metadata.json"),
}


def setup_kafka(config):
    kafka_admin = KafkaAdmin(config["bootstrap_servers"])
    if not kafka_admin.topic_exists(config["topic_name"]):
        kafka_admin.create_topic(
            config["topic_name"], num_partitions=1, replication_factor=1
        )
    else:
        logger.info(f"Topic {config['topic_name']} already exists")
    
    producer = KafkaProducer(config["bootstrap_servers"])
    consumer = KafkaConsumer(config["bootstrap_servers"], config["group_id"])
    
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
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        url = msg.value().decode('utf-8')
        logger.info(f"Consumed URL: {url}")
        await process_url(scraper, url)

async def run_kafka_scraper(producer, consumer, scraper, kafka_config, scraper_config):
    producer_task = asyncio.create_task(produce_urls(producer, kafka_config["topic_name"], scraper_config["urls"]))
    consumer_task = asyncio.create_task(consume_and_process(consumer, scraper, kafka_config["topic_name"]))
    
    await producer_task
    await asyncio.sleep(5)  # Give some time for messages to be processed
    consumer_task.cancel()  # Stop consuming after processing all URLs

def main():
    # Kafka Setup
    kafka_admin, producer, consumer = setup_kafka(KAFKA_CONFIG)

    # Scraper Setup
    scraper = setup_scraper(SCRAPER_CONFIG)

    # Run Kafka-integrated scraper
    asyncio.run(run_kafka_scraper(producer, consumer, scraper, KAFKA_CONFIG, SCRAPER_CONFIG))

    # Cleanup
    producer.close()
    consumer.flush()


if __name__ == "__main__":
    main()
