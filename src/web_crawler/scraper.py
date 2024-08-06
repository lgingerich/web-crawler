import aiofiles
from datetime import datetime
import hashlib
import json
import logging
import os
from playwright.async_api import async_playwright
from typing import Tuple, Dict, Any

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Scraper:
    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        metadata_file: str = "scraped_data/metadata.json",
    ):
        self.headless = headless
        self.slow_mo = slow_mo
        self.metadata_file = metadata_file

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
        self, url: str, title: str, content_hash: str, filename: str, success: bool
    ) -> None:
        metadata: Dict[str, Any] = {
            "url": url,
            "title": title,
            "contentHash": content_hash,
            "filename": filename,
            "success": success,
            "lastCrawlTime": datetime.now().isoformat(),
        }

        try:
            # Read existing data
            async with aiofiles.open(self.metadata_file, "r") as f:
                content = await f.read()
                data = json.loads(content) if content else []

            # Update or append the new metadata
            updated = False
            for item in data:
                if item["url"] == url:
                    item.update(metadata)
                    updated = True
                    break

            if not updated:
                data.append(metadata)

            # Write the updated data back to the file
            async with aiofiles.open(self.metadata_file, "w") as f:
                await f.write(json.dumps(data, indent=4))

            logger.info(f"Metadata {'updated' if updated else 'saved'} for {url}")
        except FileNotFoundError:
            async with aiofiles.open(self.metadata_file, "w") as f:
                await f.write(json.dumps([metadata], indent=4))
            logger.info(f"Created new metadata file and saved data for {url}")
        except json.JSONDecodeError:
            logger.error("Invalid JSON in metadata file. Overwriting with new data.")
            async with aiofiles.open(self.metadata_file, "w") as f:
                await f.write(json.dumps([metadata], indent=4))
        except IOError as e:
            logger.error(f"IO error when saving metadata for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when saving metadata for {url}: {e}")
            raise
