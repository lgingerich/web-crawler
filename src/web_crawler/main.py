import asyncio
from scraper import Scraper
from kafka_consumer import ScraperConsumer
import json

async def process_kafka_data(consumer):
    for data in consumer.consume():
        if data['success']:
            print(f"Successfully scraped: {data['url']}")
        else:
            print(f"Failed to scrape: {data['url']}")

async def main(urls: list, output_dir: str = "scraped_data", headless: bool = True, slow_mo: int = 0):
    scraper = Scraper(output_dir=output_dir, headless=headless, slow_mo=slow_mo)
    scrape_tasks = [scraper.process_url(url) for url in urls]
    
    # Start Kafka consumer
    consumer = ScraperConsumer(['scraped_data'])
    await asyncio.sleep(2)  # Wait for 2 seconds to allow topic creation
    consumer_task = asyncio.create_task(process_kafka_data(consumer))
    
    # Run scraping tasks
    await asyncio.gather(*scrape_tasks)
    
    # Wait a bit for any final messages to be consumed
    await asyncio.sleep(5)
    
    # Cancel the consumer task
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    urls = [
        'google.com',
        'amazon.com',
        'example.com'
    ]
    headless = True
    slow_mo = 0  # in milliseconds

    asyncio.run(main(urls, headless=headless, slow_mo=slow_mo))