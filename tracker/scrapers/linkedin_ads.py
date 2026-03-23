import asyncio
import hashlib
import json
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import Browser, BrowserContext, Page

from ..file_io import write_private_json

logger = logging.getLogger(__name__)

_DEBUG_DIR = Path(__file__).parent.parent.parent / "data" / "debug"
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504, 999}
_MAX_NAV_RETRIES = 3
_BASE_NAV_BACKOFF_SEC = 6.0
_LOGIN_WALL_MARKERS = ("/login", "/authwall", "/checkpoint")
_AD_CARD_SELECTOR = (
    "li.search-result-item, "
    "div.search-result-item, "
    "li[data-test-search-result], "
    "div[data-test-search-result]"
)
_POST_CARD_SELECTOR = (
    "div.feed-shared-update-v2, "
    "div.occludable-update, "
    "div.update-components-update-v2, "
    "div[data-urn*='activity'], "
    "div[data-id*='activity'], "
    "article"
)
_POST_NAV_SELECTOR = (
    "a[href*='/posts/'], "
    "a[data-control-name*='page_member_main_nav_posts'], "
    "button[aria-label*='Posts'], "
    "a[aria-label*='Posts']"
)


class AuthExpiredError(Exception):
    """Raised when the LinkedIn session cookies are no longer valid."""
    pass


@dataclass
class LinkedInAdResult:
    ad_id: str
    ad_text: str
    ad_format: str             # 'Single Image' | 'Video' | 'Carousel' | 'Text' | 'Unknown'
    impressions_range: Optional[str]
    date_range: Optional[str]


@dataclass
class LinkedInOrganicPostResult:
    post_id: str
    post_text: str
    post_url: Optional[str]
    posted_label: Optional[str]


async def scrape_linkedin_ads(
    browser: Browser,
    company_id: str,
    competitor_name: str,
    session_path: Path,
    linkedin_username: Optional[str] = None,
    linkedin_password: Optional[str] = None,
    max_ads: int = 20,
    debug: bool = False,
) -> list[LinkedInAdResult]:
    """
    Scrape LinkedIn Ad Library for ads from company_id.
    Requires a valid saved session (run setup_auth.py first).
    Raises AuthExpiredError if the session has expired and cannot be refreshed.
    Returns [] on other failures.
    """
    from ..browser import new_linkedin_context, random_delay, slow_scroll

    url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
    context = await new_linkedin_context(browser, session_path)

    try:
        page = await context.new_page()
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

        # Detect login wall redirect
        has_credentials = bool(linkedin_username and linkedin_password)
        final_url = page.url
        if _is_login_wall_url(final_url):
            if has_credentials:
                refreshed = await _attempt_linkedin_auto_login(
                    page=page,
                    context=context,
                    session_path=session_path,
                    competitor_name=competitor_name,
                    linkedin_username=linkedin_username or "",
                    linkedin_password=linkedin_password or "",
                    debug=debug,
                )
                if refreshed:
                    await random_delay(1.5, 3.0)
                    response = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=30_000,
                    )
                    final_url = page.url

            if _is_login_wall_url(final_url):
                raise AuthExpiredError(_auth_expired_message(auto_login_attempted=has_credentials))

        # Pause to let the page finish rendering before interacting
        await random_delay(2.0, 4.5)

        if debug:
            await _save_screenshot(page, f"linkedin_{company_id}_loaded")

        # Wait for ad results to render
        try:
            await page.wait_for_selector(_AD_CARD_SELECTOR, timeout=15_000)
        except Exception:
            page_text = await page.inner_text("body")
            await _save_screenshot(page, f"linkedin_{company_id}_no_selector")
            logger.info(
                "LinkedIn: selector not found for %s — screenshot saved to data/debug/",
                competitor_name,
            )
            if "no ads" in page_text.lower() or "no results" in page_text.lower():
                logger.info("LinkedIn: no ads found for %s (company %s)",
                            competitor_name, company_id)
                return []
            logger.info("LinkedIn: could not find ad cards for %s", competitor_name)
            return []

        # Pause on the page before scrolling
        await random_delay(1.0, 2.5)

        # Scroll gradually to load more ads
        for _ in range(2):
            await slow_scroll(page, total_distance=random.randint(800, 1400))
            await random_delay(1.5, 3.5)

        cards = await page.query_selector_all(_AD_CARD_SELECTOR)

        if not cards:
            await _save_screenshot(page, f"linkedin_{company_id}_no_cards")
            logger.info(
                "LinkedIn: no card elements found for %s — screenshot saved to data/debug/",
                competitor_name,
            )

        results = []
        for card in cards[:max_ads]:
            try:
                result = await _parse_card(card)
                if result:
                    results.append(result)
            except Exception as exc:
                logger.debug("Failed to parse LinkedIn ad card: %s", exc)

        logger.info("LinkedIn: found %d ads for %s", len(results), competitor_name)
        return results

    except AuthExpiredError:
        raise
    except Exception as exc:
        logger.warning("LinkedIn Ad Library scrape failed for %s: %s", competitor_name, exc)
        raise
    finally:
        await context.close()


async def _save_screenshot(page, name: str) -> None:
    """Save a screenshot to data/debug/ — silently skip on any error."""
    try:
        _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        path = _DEBUG_DIR / f"{name}.png"
        await page.screenshot(path=str(path), full_page=False)
        logger.info("Screenshot saved: %s", path)
    except Exception as exc:
        logger.debug("Screenshot failed: %s", exc)


async def _parse_card(card) -> Optional[LinkedInAdResult]:
    """Extract fields from a single LinkedIn ad card element."""
    full_text = await card.inner_text()
    if not full_text or len(full_text.strip()) < 10:
        return None

    lines = [l.strip() for l in full_text.splitlines() if l.strip()]
    ad_text = " ".join(lines[:15])

    # Impressions range e.g. "1,000 - 5,000 impressions"
    impressions = None
    for line in lines:
        if re.search(r"\d[\d,]* ?[-–] ?\d[\d,]* ?impressions", line, re.I):
            impressions = line
            break

    # Date range e.g. "Started Jan 1, 2026"
    date_range = None
    for line in lines:
        if re.search(r"(started|running|active|launched)", line, re.I):
            date_range = line
            break

    # Ad format hint
    ad_format = "Unknown"
    text_lower = full_text.lower()
    if "carousel" in text_lower:
        ad_format = "Carousel"
    elif "video" in text_lower:
        ad_format = "Video"
    elif "single image" in text_lower:
        ad_format = "Single Image"
    elif "text ad" in text_lower:
        ad_format = "Text"

    # Generate stable ID from ad text
    ad_id = hashlib.sha256(ad_text.encode()).hexdigest()[:16]

    return LinkedInAdResult(
        ad_id=ad_id,
        ad_text=ad_text.strip(),
        ad_format=ad_format,
        impressions_range=impressions,
        date_range=date_range,
    )


async def scrape_linkedin_organic_posts(
    browser: Browser,
    company_id: str,
    competitor_name: str,
    session_path: Path,
    linkedin_username: Optional[str] = None,
    linkedin_password: Optional[str] = None,
    company_url: Optional[str] = None,
    max_posts: int = 20,
    max_post_age_days: int = 7,
    debug: bool = False,
) -> list[LinkedInOrganicPostResult]:
    """
    Scrape recent organic LinkedIn posts from a company page.
    Requires a valid saved session (run setup_auth.py first).
    Raises AuthExpiredError if the session has expired.
    Returns [] on other failures.
    """
    from ..browser import new_linkedin_context, random_delay, slow_scroll

    target_url = _build_linkedin_posts_url(company_id, company_url)
    fallback_url = f"https://www.linkedin.com/company/{company_id}/"
    urls_to_try = _build_linkedin_organic_urls(company_id, company_url)
    context = await new_linkedin_context(browser, session_path)

    try:
        page = await context.new_page()
        loaded = False

        for idx, url in enumerate(urls_to_try):
            endpoint_label = _label_linkedin_organic_endpoint(
                url=url,
                target_url=target_url,
                fallback_url=fallback_url,
                position=idx,
            )
            # Conservative pacing before each LinkedIn navigation to reduce anti-bot triggers.
            await random_delay(3.0, 6.0)
            response = await _goto_with_retries(
                page=page,
                url=url,
                competitor_name=competitor_name,
                endpoint_label=endpoint_label,
            )

            has_credentials = bool(linkedin_username and linkedin_password)
            final_url = page.url
            if _is_login_wall_url(final_url):
                if has_credentials:
                    refreshed = await _attempt_linkedin_auto_login(
                        page=page,
                        context=context,
                        session_path=session_path,
                        competitor_name=competitor_name,
                        linkedin_username=linkedin_username or "",
                        linkedin_password=linkedin_password or "",
                        debug=debug,
                    )
                    if refreshed:
                        await random_delay(1.5, 3.0)
                        response = await _goto_with_retries(
                            page=page,
                            url=url,
                            competitor_name=competitor_name,
                            endpoint_label=f"{endpoint_label}-after-relogin",
                        )
                        final_url = page.url

                if _is_login_wall_url(final_url):
                    raise AuthExpiredError(
                        _auth_expired_message(auto_login_attempted=has_credentials)
                    )

            if response is None or response.status >= 400:
                logger.info(
                    "LinkedIn organic: got status %s for %s (%s)",
                    response.status if response is not None else "unknown",
                    competitor_name,
                    endpoint_label,
                )
                continue

            await random_delay(4.0, 8.0)
            if debug:
                await _save_screenshot(page, f"linkedin_org_{company_id}_{endpoint_label}_loaded")

            post_cards = await _collect_post_cards_with_retries(
                page=page,
                competitor_name=competitor_name,
                endpoint_label=endpoint_label,
            )
            if post_cards:
                loaded = True
                if endpoint_label != "primary":
                    logger.info(
                        "LinkedIn organic: using alternate company URL for %s (%s)",
                        competitor_name,
                        url,
                    )
                break

        if not loaded:
            hint = await _summarize_page_hint(page)
            logger.info(
                "LinkedIn organic: no post cards found for %s (hint=%s)",
                competitor_name,
                hint,
            )
            return []

        # Scroll to reveal recent posts
        for _ in range(4):
            await slow_scroll(page, total_distance=random.randint(650, 1200))
            await random_delay(2.5, 5.5)

        cards = await page.query_selector_all(_POST_CARD_SELECTOR)
        seen_ids: set[str] = set()
        results: list[LinkedInOrganicPostResult] = []
        parsed_total = 0
        filtered_by_age = 0
        sample_labels: list[str] = []

        for card in cards:
            parsed = await _parse_organic_card(card)
            if not parsed:
                continue
            parsed_total += 1
            if parsed.posted_label and len(sample_labels) < 8:
                sample_labels.append(parsed.posted_label)
            if parsed.post_id in seen_ids:
                continue
            if parsed.posted_label and not _is_within_days(parsed.posted_label, max_post_age_days):
                filtered_by_age += 1
                continue

            seen_ids.add(parsed.post_id)
            results.append(parsed)
            if len(results) >= max_posts:
                break

        if parsed_total and not results:
            logger.info(
                "LinkedIn organic: parsed %d card(s) for %s but none within %dd "
                "(filtered_by_age=%d, sample_labels=%s)",
                parsed_total,
                competitor_name,
                max_post_age_days,
                filtered_by_age,
                sample_labels or "none",
            )

        logger.info(
            "LinkedIn organic: found %d post(s) within %dd for %s",
            len(results),
            max_post_age_days,
            competitor_name,
        )
        return results

    except AuthExpiredError:
        raise
    except Exception as exc:
        logger.warning("LinkedIn organic scrape failed for %s: %s", competitor_name, exc)
        raise
    finally:
        await context.close()


async def _goto_with_retries(
    page,
    url: str,
    competitor_name: str,
    endpoint_label: str,
):
    """
    Navigate with conservative retry/backoff for LinkedIn throttle/transient failures.
    """
    for attempt in range(1, _MAX_NAV_RETRIES + 1):
        response = await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        status = response.status if response is not None else None

        if status is None or status < 400 or status not in _RETRYABLE_STATUS_CODES:
            return response

        if attempt >= _MAX_NAV_RETRIES:
            return response

        delay = _BASE_NAV_BACKOFF_SEC * (2 ** (attempt - 1)) + random.uniform(0.8, 2.2)
        logger.info(
            "LinkedIn organic: retryable status %s for %s (%s, attempt %d/%d), sleeping %.1fs",
            status,
            competitor_name,
            endpoint_label,
            attempt,
            _MAX_NAV_RETRIES,
            delay,
        )
        await asyncio.sleep(delay)

    return None


def _is_login_wall_url(url: str) -> bool:
    lower = (url or "").lower()
    return any(marker in lower for marker in _LOGIN_WALL_MARKERS)


def _auth_expired_message(auto_login_attempted: bool) -> str:
    if auto_login_attempted:
        return (
            "LinkedIn redirected to login and credential re-login did not clear the auth wall "
            "(checkpoint/2FA may be required). Run `python setup_auth.py` to re-authenticate."
        )
    return (
        "LinkedIn redirected to login. Session has expired. "
        "Run `python setup_auth.py` to re-authenticate."
    )


async def _attempt_linkedin_auto_login(
    *,
    page: Page,
    context: BrowserContext,
    session_path: Path,
    competitor_name: str,
    linkedin_username: str,
    linkedin_password: str,
    debug: bool,
) -> bool:
    """
    Attempt credential-based login and refresh storage_state on success.
    Returns False when login is blocked or fails.
    """
    try:
        logger.info(
            "LinkedIn: session expired for %s; attempting credential re-login",
            competitor_name,
        )
        await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded", timeout=45_000)
        await page.wait_for_selector(
            "input#username, input[name='session_key']",
            timeout=15_000,
        )

        username_selector = "input#username"
        password_selector = "input#password"
        if await page.query_selector(username_selector) is None:
            username_selector = "input[name='session_key']"
        if await page.query_selector(password_selector) is None:
            password_selector = "input[name='session_password']"

        await page.fill(username_selector, linkedin_username)
        await asyncio.sleep(random.uniform(0.4, 1.1))
        await page.fill(password_selector, linkedin_password)
        await asyncio.sleep(random.uniform(0.3, 0.8))
        await page.click("button[type='submit']")
        await page.wait_for_load_state("domcontentloaded", timeout=45_000)
        await asyncio.sleep(random.uniform(2.0, 4.0))

        current_url = page.url
        login_inputs_still_visible = (
            await page.query_selector("input#username, input[name='session_key']") is not None
        )
        if _is_login_wall_url(current_url) or login_inputs_still_visible:
            if debug:
                suffix = hashlib.sha256(competitor_name.encode("utf-8")).hexdigest()[:8]
                await _save_screenshot(page, f"linkedin_auto_login_blocked_{suffix}")
            logger.warning(
                "LinkedIn credential re-login blocked for %s (url=%s)",
                competitor_name,
                current_url,
            )
            return False

        await _persist_linkedin_session(context, session_path)
        logger.info("LinkedIn: credential re-login succeeded and session was refreshed")
        return True
    except Exception as exc:
        logger.warning("LinkedIn credential re-login failed for %s: %s", competitor_name, exc)
        return False


async def _persist_linkedin_session(context: BrowserContext, session_path: Path) -> None:
    storage = await context.storage_state()
    write_private_json(session_path, storage)


def _build_linkedin_posts_url(company_id: str, company_url: Optional[str]) -> str:
    if company_url and "linkedin.com/company/" in company_url:
        base = company_url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        if base.endswith("/posts"):
            return f"{base}/?feedView=all"
        return f"{base}/posts/?feedView=all"
    return f"https://www.linkedin.com/company/{company_id}/posts/?feedView=all"


def _build_linkedin_organic_urls(company_id: str, company_url: Optional[str]) -> list[str]:
    primary = _build_linkedin_posts_url(company_id, company_url)
    if "?" in primary:
        member_view = f"{primary}&viewAsMember=true"
    else:
        member_view = f"{primary}?viewAsMember=true"
    fallback = f"https://www.linkedin.com/company/{company_id}/"

    urls: list[str] = []
    for url in [primary, member_view, fallback]:
        if url not in urls:
            urls.append(url)
    return urls


def _label_linkedin_organic_endpoint(
    *,
    url: str,
    target_url: str,
    fallback_url: str,
    position: int,
) -> str:
    if url == target_url:
        return "primary"
    if url == fallback_url:
        return "fallback"
    if "viewAsMember=true" in url:
        return "member-view"
    return f"alt-{position + 1}"


async def _collect_post_cards_with_retries(
    *,
    page: Page,
    competitor_name: str,
    endpoint_label: str,
) -> list:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            await page.wait_for_selector(_POST_CARD_SELECTOR, timeout=9_000)
        except Exception:
            pass

        cards = await page.query_selector_all(_POST_CARD_SELECTOR)
        if cards:
            return cards

        if await _try_click_posts_tab(page):
            await asyncio.sleep(random.uniform(1.4, 2.8))
        else:
            await page.mouse.wheel(0, random.randint(500, 1000))
            await asyncio.sleep(random.uniform(1.8, 3.8))

        logger.debug(
            "LinkedIn organic: no cards yet for %s (%s attempt %d/%d)",
            competitor_name,
            endpoint_label,
            attempt,
            max_attempts,
        )
    return []


async def _try_click_posts_tab(page: Page) -> bool:
    try:
        candidates = await page.query_selector_all(_POST_NAV_SELECTOR)
    except Exception:
        return False

    for candidate in candidates[:8]:
        try:
            label = " ".join((await candidate.inner_text()).split()).lower()
            aria_label = (await candidate.get_attribute("aria-label") or "").lower()
            if "post" not in label and "post" not in aria_label:
                continue

            aria_current = (await candidate.get_attribute("aria-current") or "").lower()
            aria_selected = (await candidate.get_attribute("aria-selected") or "").lower()
            if aria_current == "page" or aria_selected == "true":
                return False

            await candidate.click(timeout=3_000)
            return True
        except Exception:
            continue
    return False


async def _summarize_page_hint(page: Page) -> str:
    try:
        body_text = await page.inner_text("body")
    except Exception:
        return "body-unavailable"
    return _summarize_linkedin_page_state(body_text)


def _summarize_linkedin_page_state(text: str) -> str:
    normalized = " ".join((text or "").split()).lower()
    if not normalized:
        return "empty-body"

    markers: list[str] = []
    checks = [
        ("sign in", "sign-in"),
        ("join now", "join-now"),
        ("security verification", "security-verification"),
        ("challenge", "challenge"),
        ("something went wrong", "error-banner"),
        ("try again", "retry-prompt"),
        ("no posts yet", "no-posts"),
        ("page isn't available", "page-unavailable"),
        ("this page is unavailable", "page-unavailable"),
    ]
    for needle, marker in checks:
        if needle in normalized:
            markers.append(marker)

    if not markers:
        return "no-known-marker"
    return ",".join(dict.fromkeys(markers))


async def _parse_organic_card(card) -> Optional[LinkedInOrganicPostResult]:
    body_candidates: list[str] = []

    for selector in ["div.update-components-text", "span.break-words", "div.feed-shared-text"]:
        nodes = await card.query_selector_all(selector)
        for node in nodes[:3]:
            try:
                text = " ".join((await node.inner_text()).split())
            except Exception:
                text = ""
            if text and len(text) > 20:
                body_candidates.append(text)

    post_text = body_candidates[0] if body_candidates else ""
    if not post_text:
        full_text = " ".join((await card.inner_text()).split())
        if len(full_text) < 40:
            return None
        post_text = full_text[:1200]

    posted_label = await _extract_posted_label(card)
    post_url = await _extract_post_url(card)
    post_id = _extract_post_id(post_url, post_text, posted_label)

    return LinkedInOrganicPostResult(
        post_id=post_id,
        post_text=post_text[:1800].strip(),
        post_url=post_url,
        posted_label=posted_label,
    )


async def _extract_posted_label(card) -> Optional[str]:
    for selector in [
        "span.update-components-actor__sub-description",
        "span.update-components-actor__sub-description-link",
        "span.update-components-actor__supplementary-actor-info",
        "span.t-black--light",
    ]:
        nodes = await card.query_selector_all(selector)
        for node in nodes[:4]:
            try:
                raw = " ".join((await node.inner_text()).split())
            except Exception:
                raw = ""
            if not raw:
                continue
            parts = [part.strip() for part in raw.split("•") if part.strip()]
            for part in parts:
                if _looks_like_post_age(part):
                    return part

    # Fallback: scan the whole card text for an age/date token.
    try:
        full_text = " ".join((await card.inner_text()).split())
    except Exception:
        full_text = ""
    token = _extract_age_token_from_text(full_text)
    if token:
        return token
    return None


async def _extract_post_url(card) -> Optional[str]:
    for selector in [
        "a[href*='/feed/update/urn:li:activity:']",
        "a[href*='/posts/']",
    ]:
        node = await card.query_selector(selector)
        if not node:
            continue
        href = await node.get_attribute("href")
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://www.linkedin.com{href}"
        return href.split("?", 1)[0]

    # Fallback: derive a canonical URL from card-level activity URN.
    for attr in ["data-urn", "data-id", "id"]:
        raw = await card.get_attribute(attr)
        if not raw:
            continue
        match = re.search(r"activity:(\d+)", raw)
        if match:
            return f"https://www.linkedin.com/feed/update/urn:li:activity:{match.group(1)}/"
    return None


def _extract_post_id(post_url: Optional[str], post_text: str, posted_label: Optional[str]) -> str:
    if post_url:
        for pat in [
            r"activity:(\d+)",
            r"/posts/[^/?#]+-(\d+)",
            r"share:(\d+)",
        ]:
            match = re.search(pat, post_url)
            if match:
                return match.group(1)
        return hashlib.sha256(post_url.encode("utf-8")).hexdigest()[:16]

    seed = f"{post_text[:300]}|{posted_label or ''}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _looks_like_post_age(value: str) -> bool:
    text = value.lower().strip()
    if "yesterday" in text or "today" in text or "just now" in text:
        return True
    if re.search(r"\b\d+\s*(?:m|min|minute|minutes|h|hr|hour|hours|d|day|days|w|wk|week|weeks|mo|month|months|y|yr|year|years)\b", text):
        return True
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?\b", text, re.I):
        return True
    return False


def _extract_age_token_from_text(text: str) -> Optional[str]:
    if not text:
        return None

    lower = text.lower()
    for kw in ("just now", "today", "yesterday"):
        if kw in lower:
            return kw

    match = re.search(
        r"\b\d+\s*(?:m|min|minute|minutes|h|hr|hour|hours|d|day|days|w|wk|week|weeks|mo|month|months|y|yr|year|years)\b",
        lower,
    )
    if match:
        return match.group(0)

    month_match = re.search(
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?\b",
        text,
        re.I,
    )
    if month_match:
        return month_match.group(0)
    return None


def _is_within_days(label: str, max_days: int) -> bool:
    text = label.lower().strip()
    if not text:
        return True
    if "just now" in text or "today" in text:
        return True
    if "yesterday" in text:
        return max_days >= 1

    numeric = re.search(
        r"\b(\d+)\s*(m|min|minute|minutes|h|hr|hour|hours|d|day|days|w|wk|week|weeks|mo|month|months|y|yr|year|years)\b",
        text,
    )
    if numeric:
        value = int(numeric.group(1))
        unit = numeric.group(2)
        if unit in {"m", "min", "minute", "minutes"}:
            age_days = 0
        elif unit in {"h", "hr", "hour", "hours"}:
            age_days = 0
        elif unit in {"d", "day", "days"}:
            age_days = value
        elif unit in {"w", "wk", "week", "weeks"}:
            age_days = value * 7
        elif unit in {"mo", "month", "months"}:
            age_days = value * 30
        else:
            age_days = value * 365
        return age_days <= max_days

    current = datetime.now(timezone.utc)
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d", "%B %d"):
        try:
            parsed = datetime.strptime(label.strip(), fmt)
        except ValueError:
            continue
        if "%Y" not in fmt:
            parsed = parsed.replace(year=current.year)
            # Handle year rollover (e.g., Dec shown in early Jan).
            if parsed.replace(tzinfo=timezone.utc) > current:
                parsed = parsed.replace(year=current.year - 1)
        parsed = parsed.replace(tzinfo=timezone.utc)
        return (current - parsed).days <= max_days

    # If unknown label format, keep the post to avoid false negatives.
    return True
