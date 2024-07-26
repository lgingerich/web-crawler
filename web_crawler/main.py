import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse

async def get_links(page, base_url):
    base_domain = urlparse(base_url).netloc
    links = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('a'))
            .map(a => a.href)
            .filter(href => href.includes('solana/pair-explorer/'));
    }''')
    return [link for link in links if urlparse(link).netloc == base_domain]

async def main(headless: bool = True, slow_mo: int = 0):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, slow_mo=slow_mo)
        page = await browser.new_page()
        
        base_url = "https://www.dextools.io/app/en/solana/pool-explorer"
        await page.goto(base_url)
        # await page.screenshot(path="example.png")
        print(f"Page title: {await page.title()}")

        links = await get_links(page, base_url)
        for link in links:
            print(link)
        
        await browser.close()


if __name__ == "__main__":
    headless = False
    slow_mo = 1000 * 0 # X seconds 

    asyncio.run(main(headless, slow_mo))