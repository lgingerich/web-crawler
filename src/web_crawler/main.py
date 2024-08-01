import asyncio
from scraper import Scraper

async def main(urls: list, output_dir: str = "scraped_data", headless: bool = True, slow_mo: int = 0):
    scraper = Scraper(output_dir=output_dir, headless=headless, slow_mo=slow_mo)
    tasks = [scraper.process_url(url) for url in urls]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    urls = [
        'google.com',
        'amazon.com',
        'example.com'
    ]
    headless = True
    slow_mo = 0  # in milliseconds

    asyncio.run(main(urls, headless=headless, slow_mo=slow_mo))