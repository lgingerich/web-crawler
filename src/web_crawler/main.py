import asyncio
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse

async def scrape_url(url: str, headless: bool = True, slow_mo: int = 0):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, slow_mo=slow_mo)
        page = await browser.new_page()
        
        try:
            await page.goto(url)
            title = await page.title()
            content = await page.content()  
            return title, content
        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            return None, None
        finally:
            await browser.close()


def save_html(content: str, filename: str):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)


def save_metadata(url: str, title: str, filename: str, success: bool, metadata_file: str):
    metadata = {
        "url": url,
        "title": title,
        "filename": filename,
        "success": success,
        "lastCrawlTime": datetime.now().isoformat()
    }
    
    try:
        with open(metadata_file, 'r+') as f:
            data = json.load(f)
            data.append(metadata)
            f.seek(0)
            json.dump(data, f, indent=4)
    except FileNotFoundError:
        with open(metadata_file, 'w') as f:
            json.dump([metadata], f, indent=4)


async def process_url(url: str, output_dir: str, metadata_file: str, headless: bool = True, slow_mo: int = 0):
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
    
    parsed_url = urlparse(url)
    filename = f"{parsed_url.netloc.replace('.', '_')}.html"
    filepath = os.path.join(output_dir, filename)
    
    os.makedirs(output_dir, exist_ok=True)
    
    title, content = await scrape_url(url, headless, slow_mo)
    
    if content:
        save_html(content, filepath)
        save_metadata(url, title, filepath, True, metadata_file)
        print(f"Successfully scraped and saved data from {url}")
    else:
        save_metadata(url, None, filepath, False, metadata_file)
        print(f"Failed to scrape data from {url}")


async def main(urls: list, output_dir: str = "scraped_data", headless: bool = True, slow_mo: int = 0):
    metadata_file = os.path.join(output_dir, "metadata.json")
    tasks = [process_url(url, output_dir, metadata_file, headless=headless, slow_mo=slow_mo) for url in urls]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    urls = [
        'google.com',
        'amazon.com',
        'example.com'
    ]
    headless = False
    slow_mo = 1000 * 0  # X seconds

    asyncio.run(main(urls, headless=headless, slow_mo=slow_mo))