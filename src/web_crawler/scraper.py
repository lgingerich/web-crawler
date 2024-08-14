import aiofiles
import asyncio
from datetime import datetime
import hashlib
import os
from playwright.async_api import (
    async_playwright,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
)
from typing import Tuple, Dict, Any, Optional
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

    async def scrape_url(
        self, url: str
    ) -> Tuple[Optional[str], Optional[str], Optional[dict]]:
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=self.headless, slow_mo=self.slow_mo
                )
                try:
                    page = await browser.new_page()
                    response = await page.goto(
                        url, wait_until="networkidle", timeout=30000
                    )

                    if response is None:
                        raise PlaywrightError("No response received")

                    title = await page.title()
                    content = await page.locator("body").inner_text()

                    response_info = {
                        "status": response.status,
                        "status_text": response.status_text,
                        "headers": dict(response.headers),
                        "url": response.url,
                    }

                    return title, content, response_info

                except PlaywrightTimeoutError as e:
                    logger.error(f"Timeout error scraping {url}: {str(e)}")
                    return (
                        None,
                        None,
                        {"status": 408, "error": str(e), "error_type": "timeout"},
                    )
                except PlaywrightError as e:
                    error_info = {
                        "status": 500,
                        "error": str(e),
                        "error_type": "playwright",
                    }
                    if "ERR_CONNECTION_REFUSED" in str(e):
                        error_info["status"] = 503
                        error_info["error_type"] = "connection_refused"
                    elif "ERR_NAME_NOT_RESOLVED" in str(e):
                        error_info["status"] = 502
                        error_info["error_type"] = "dns_resolution_failed"
                    logger.error(f"Playwright error scraping {url}: {str(e)}")
                    return None, None, error_info
                finally:
                    await browser.close()

        except asyncio.CancelledError:
            logger.info(f"Scraping of {url} was cancelled")
            return (
                None,
                None,
                {
                    "status": 499,
                    "error": "Client closed request",
                    "error_type": "cancelled",
                },
            )
        except Exception as e:
            logger.error(f"Unexpected error scraping {url}: {str(e)}", exc_info=True)
            return (
                None,
                None,
                {"status": 500, "error": str(e), "error_type": "unexpected"},
            )

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
