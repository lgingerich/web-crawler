import asyncio
from playwright.async_api import async_playwright

async def test_connection():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto('https://example.com')
        print(await page.title())
        await browser.close()

asyncio.run(test_connection())