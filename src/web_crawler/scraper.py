import asyncio
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urlparse
import aiofiles
from typing import Tuple, Dict, Any
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class Scraper:
    def __init__(self, output_dir: str = "scraped_data", headless: bool = True, slow_mo: int = 0):
        self.output_dir = output_dir
        self.headless = headless
        self.slow_mo = slow_mo
        self.metadata_file = os.path.join(output_dir, "metadata.json")
        os.makedirs(output_dir, exist_ok=True)

    async def scrape_url(self, url: str) -> Tuple[str, str]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
            page = await browser.new_page()
            
            try:
                await page.goto(url)
                title = await page.title()
                content = await page.content()  
                return title, content
            except Exception as e:
                logger.error(f"Error scraping {url}: {str(e)}")
                return None, None
            finally:
                await browser.close()

    async def save_html(self, content: str, filename: str) -> None:
        try:
            async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
                await f.write(content)
            logger.info(f"Successfully saved HTML to {filename}")
        except IOError as e:
            logger.error(f"Failed to save HTML to {filename}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while saving HTML to {filename}: {e}")
            raise

    async def save_metadata(self, url: str, title: str, filename: str, success: bool) -> None:
        metadata: Dict[str, Any] = {
            "url": url,
            "title": title,
            "filename": filename,
            "success": success,
            "lastCrawlTime": datetime.now().isoformat()
        }
        
        try:
            async with aiofiles.open(self.metadata_file, 'r+') as f:
                content = await f.read()
                data = json.loads(content) if content else []
                data.append(metadata)
                await f.seek(0)
                await f.truncate()
                await f.write(json.dumps(data, indent=4))
            logger.info(f"Metadata saved successfully for {url}")
        except FileNotFoundError:
            async with aiofiles.open(self.metadata_file, 'w') as f:
                await f.write(json.dumps([metadata], indent=4))
            logger.info(f"Created new metadata file and saved data for {url}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in metadata file. Overwriting with new data.")
            async with aiofiles.open(self.metadata_file, 'w') as f:
                await f.write(json.dumps([metadata], indent=4))
        except IOError as e:
            logger.error(f"IO error when saving metadata for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when saving metadata for {url}: {e}")
            raise

    async def process_url(self, url: str) -> None:
        try:
            # Ensure URL has a scheme
            if not url.startswith(('http://', 'https://')):
                url = f"https://{url}"
            
            # Prepare filename and filepath
            parsed_url = urlparse(url)
            filename = f"{parsed_url.netloc.replace('.', '_')}.html"
            filepath = os.path.join(self.output_dir, filename)
            
            # Scrape URL
            title, content = await self.scrape_url(url)
            
            # Save content and metadata
            if content:
                try:
                    await self.save_html(content, filepath)
                    logger.info(f"Saved HTML content for {url} to {filepath}")
                except IOError as e:
                    logger.error(f"Failed to save HTML for {url}: {e}")
                    await self.save_metadata(url, title, filepath, False)
                    return
                
                await self.save_metadata(url, title, filepath, True)
                logger.info(f"Successfully processed {url}")
            else:
                logger.warning(f"No content retrieved for {url}")
                await self.save_metadata(url, None, filepath, False)
        
        except Exception as e:
            logger.error(f"Unexpected error processing {url}: {e}")
            await self.save_metadata(url, None, filepath, False)