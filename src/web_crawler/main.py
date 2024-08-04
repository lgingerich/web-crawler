import asyncio
import logging
import os
from scraper import Scraper
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def process(scraper, url):
    try:
        # Ensure URL has the scheme
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

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


async def main(scraper: Scraper, urls: list):
    tasks = [process(scraper, url) for url in urls]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Inputs
    urls = ["google.com", "amazon.com", "example.com", "dell.com"]
    headless = True
    slow_mo = 0  # in milliseconds
    metadata_file = os.path.join("scraped_data", "metadata.json")

    # Initialize Scraper class
    scraper = Scraper(headless=headless, slow_mo=slow_mo, metadata_file=metadata_file)

    # Run scraper
    asyncio.run(main(scraper, urls))
