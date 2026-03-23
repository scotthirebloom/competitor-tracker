import asyncio
import json
import random
from contextlib import asynccontextmanager
from pathlib import Path

from playwright.async_api import async_playwright, Browser, BrowserContext, Page


# Slight viewport variation prevents canvas/resolution fingerprinting
_VIEWPORTS = [
    {"width": 1280, "height": 800},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]


async def random_delay(min_sec: float = 1.5, max_sec: float = 4.0) -> None:
    """Sleep for a random duration to simulate human pacing."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))


async def slow_scroll(page: Page, total_distance: int = 1500) -> None:
    """
    Scroll down a page in small random increments with short pauses between
    each step, mimicking how a human would scroll to read content.
    """
    scrolled = 0
    while scrolled < total_distance:
        step = random.randint(80, 220)
        await page.evaluate(f"window.scrollBy(0, {step})")
        scrolled += step
        await asyncio.sleep(random.uniform(0.2, 0.7))


def _stealth_headers() -> dict:
    return {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }


@asynccontextmanager
async def get_browser(headless: bool = True):
    """Async context manager yielding a shared Playwright Browser instance."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        try:
            yield browser
        finally:
            await browser.close()


async def new_stealth_context(browser: Browser) -> BrowserContext:
    """Browser context with a realistic UA, randomized viewport, and extra headers."""
    return await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport=random.choice(_VIEWPORTS),
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers=_stealth_headers(),
    )


async def new_linkedin_context(
    browser: Browser, session_path: Path
) -> BrowserContext:
    """
    Browser context pre-loaded with saved LinkedIn session cookies.
    Raises FileNotFoundError with a helpful message if the session file is missing.
    """
    if not session_path.exists():
        raise FileNotFoundError(
            f"LinkedIn session not found at {session_path}.\n"
            "Run `python setup_auth.py` first to authenticate with LinkedIn."
        )
    storage_state = json.loads(session_path.read_text())
    return await browser.new_context(
        storage_state=storage_state,
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport=random.choice(_VIEWPORTS),
        locale="en-US",
        extra_http_headers=_stealth_headers(),
    )
