import aiofiles
from datetime import datetime
import hashlib
import os
from playwright.async_api import async_playwright
from typing import Tuple, Dict, Any
from utils import logger
from database import DatabaseManager


class Scraper:
    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        metadata_file: str = "scraped_data/metadata.json",
        db: DatabaseManager = None,
    ):
        self.headless = headless
        self.slow_mo = slow_mo
        self.metadata_file = metadata_file
        self.db = db

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)

    async def scrape_url(self, url: str) -> Tuple[str, str]:
        # Ensure URL has the proper scheme
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless, slow_mo=self.slow_mo
            )
            page = await browser.new_page()

            try:
                await page.goto(url)
                title = await page.title()
                # content = await page.content() # Get full page content

                content = await page.locator(
                    "body"
                ).inner_text()  # Get content of the body tag
                return title, content
            except Exception as e:
                logger.error(f"Error scraping {url}: {str(e)}")
                return None, None
            finally:
                await browser.close()

    def hash_str(self, content: str) -> str:
        return hashlib.blake2b(content.encode("utf-8")).hexdigest()

    async def save_html(self, content: str, filename: str) -> None:
        try:
            async with aiofiles.open(filename, "w", encoding="utf-8") as f:
                await f.write(content)
            logger.info(f"Successfully saved HTML to {filename}")
        except IOError as e:
            logger.error(f"Failed to save HTML to {filename}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while saving HTML to {filename}: {e}")
            raise

    async def save_metadata(
        self,
        url: str,
        title: str,
        content_hash: str,
        filename: str,
        success: bool,
        status_code: int,
        content_type: str,
    ) -> None:
        metadata: Dict[str, Any] = {
            "title": title,
            "last_crawl_time": datetime.now().isoformat(),
            "crawl_success": success,
            "content_hash": content_hash,
            "filename": filename,
            "metadata": {
                "status_code": status_code,
                "content_type": content_type,
                "crawl_depth": 1,  # You might want to implement logic to determine this
            },
        }

        try:
            self.db.upsert_record(url, metadata)
            logger.info(f"Metadata saved for {url}")
        except Exception as e:
            logger.error(f"Error saving metadata for {url}: {e}")
            raise
