import logging
import random
import re
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup
from playwright.async_api import Browser

logger = logging.getLogger(__name__)

# Tags whose content we always discard before extracting text
_STRIP_TAGS = {"script", "style", "noscript", "nav", "footer", "header", "aside"}


@dataclass
class WebsiteResult:
    competitor_name: str
    page_type: str       # 'homepage' | 'blog' | 'pricing' | 'careers'
    url: str
    text: str            # full extracted visible text (for hashing + diffing)
    new_items: list[str] = field(default_factory=list)  # blog titles or job titles
    error: Optional[str] = None


async def scrape_homepage(
    browser: Browser, competitor_name: str, url: str
) -> WebsiteResult:
    try:
        html = await _load_page(browser, url)
        text = _extract_text(html, focus_selectors=["h1", "h2", "h3", "p", "button", "a"])
        return WebsiteResult(competitor_name=competitor_name, page_type="homepage",
                             url=url, text=text)
    except Exception as exc:
        logger.warning("Homepage scrape failed for %s (%s): %s", competitor_name, url, exc)
        return WebsiteResult(competitor_name=competitor_name, page_type="homepage",
                             url=url, text="", error=str(exc))


async def scrape_blog(
    browser: Browser, competitor_name: str, url: str
) -> WebsiteResult:
    try:
        html = await _load_page(browser, url)
        items = _extract_article_titles(html)
        # Sort for stable hash regardless of page ordering
        text = "\n".join(sorted(items)) if items else _extract_text(html)
        return WebsiteResult(competitor_name=competitor_name, page_type="blog",
                             url=url, text=text, new_items=items)
    except Exception as exc:
        logger.warning("Blog scrape failed for %s (%s): %s", competitor_name, url, exc)
        return WebsiteResult(competitor_name=competitor_name, page_type="blog",
                             url=url, text="", error=str(exc))


async def scrape_pricing(
    browser: Browser, competitor_name: str, url: str
) -> WebsiteResult:
    try:
        html = await _load_page(browser, url)
        text = _extract_text(html)
        return WebsiteResult(competitor_name=competitor_name, page_type="pricing",
                             url=url, text=text)
    except Exception as exc:
        logger.warning("Pricing scrape failed for %s (%s): %s", competitor_name, url, exc)
        return WebsiteResult(competitor_name=competitor_name, page_type="pricing",
                             url=url, text="", error=str(exc))


async def scrape_careers(
    browser: Browser, competitor_name: str, url: str
) -> WebsiteResult:
    try:
        html = await _load_page(browser, url)
        items = _extract_job_titles(html)
        text = "\n".join(sorted(items)) if items else _extract_text(html)
        return WebsiteResult(competitor_name=competitor_name, page_type="careers",
                             url=url, text=text, new_items=items)
    except Exception as exc:
        logger.warning("Careers scrape failed for %s (%s): %s", competitor_name, url, exc)
        return WebsiteResult(competitor_name=competitor_name, page_type="careers",
                             url=url, text="", error=str(exc))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _load_page(browser: Browser, url: str, timeout: int = 30_000) -> str:
    """Load a URL with Playwright and return the full page HTML."""
    from ..browser import new_stealth_context, random_delay, slow_scroll
    context = await new_stealth_context(browser)
    try:
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        # Give JS-rendered content a moment to settle
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass  # networkidle timeout is acceptable — use what we have
        # Simulate a human pausing to read, then scrolling down the page
        await random_delay(1.5, 3.5)
        await slow_scroll(page, total_distance=random.randint(600, 1200))
        await random_delay(0.5, 1.5)
        return await page.content()
    finally:
        await context.close()


def _extract_text(
    html: str,
    focus_selectors: Optional[list[str]] = None,
) -> str:
    """
    Parse HTML, remove boilerplate tags, return normalized visible text.
    If focus_selectors is given, only extract text from those elements.
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()

    if focus_selectors:
        parts = []
        for selector in focus_selectors:
            for el in soup.select(selector):
                t = el.get_text(separator=" ", strip=True)
                if t:
                    parts.append(t)
        text = " ".join(parts)
    else:
        text = soup.get_text(separator=" ", strip=True)

    # Collapse whitespace
    return re.sub(r"\s+", " ", text).strip()


def _extract_article_titles(html: str) -> list[str]:
    """
    Heuristically extract article/post titles from a blog index page.
    Returns list of "title | url" strings for stable hashing.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()

    titles = []

    # Try <article> elements first
    for article in soup.find_all("article"):
        heading = article.find(["h1", "h2", "h3", "h4"])
        if heading:
            link = heading.find("a")
            title = heading.get_text(strip=True)
            href = link.get("href", "") if link else ""
            if title:
                titles.append(f"{title} | {href}")

    # Fallback: look for heading tags that contain links (common blog patterns)
    if not titles:
        for heading in soup.find_all(["h2", "h3"]):
            link = heading.find("a")
            if link:
                title = heading.get_text(strip=True)
                href = link.get("href", "")
                if title and len(title) > 10:  # skip short nav items
                    titles.append(f"{title} | {href}")

    return titles[:50]  # cap to avoid noise


def _extract_job_titles(html: str) -> list[str]:
    """
    Heuristically extract job listing titles from a careers page.
    Returns list of "title — location" strings.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()

    jobs = []

    # Many ATS platforms (Greenhouse, Lever, Workday) use <a> tags with job titles
    # inside a list or table structure
    for el in soup.find_all(["li", "tr", "div"], class_=re.compile(
        r"(job|position|opening|role|listing|posting)", re.I
    )):
        text = el.get_text(separator=" ", strip=True)
        if text and 5 < len(text) < 200:
            jobs.append(re.sub(r"\s+", " ", text))

    # Fallback: headings that look like job titles
    if not jobs:
        for heading in soup.find_all(["h2", "h3", "h4"]):
            text = heading.get_text(strip=True)
            if text and 5 < len(text) < 150:
                jobs.append(text)

    return list(dict.fromkeys(jobs))[:100]  # dedupe, cap at 100
