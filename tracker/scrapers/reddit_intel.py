import asyncio
import logging
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

_REDDIT_SEARCH_PRIMARY = "https://www.reddit.com/search.json"
_REDDIT_SEARCH_FALLBACK = "https://old.reddit.com/search.json"
_REDDIT_COMMENTS_PRIMARY = "https://www.reddit.com/comments/{post_id}.json"
_REDDIT_COMMENTS_FALLBACK = "https://old.reddit.com/comments/{post_id}.json"

# Posts/comments must mention a concrete dollar amount or per-unit pricing
_PRICE_SIGNALS = re.compile(
    r'\$\s*\d[\d,]*(?:\.\d+)?|per\s+(month|user|seat|agent|mo|hr|hour)\b',
    re.I,
)

# Exclude subreddits that discuss stock prices, not service pricing
_FINANCE_SUBREDDITS = re.compile(
    r'stock|invest|market|trading|wallstreet|options|finance|quant|earnings|ipo',
    re.I,
)

# Conservative fetch depth per keyword query
_LIMIT = 25
_MAX_RETRIES = 2
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_USER_AGENT = "competitor-tracker/1.0 (json-only)"

# Query/parse caps
_MAX_KEYWORDS = 6
_MAX_POSTS_FOR_COMMENT_FETCH = 10
_MAX_COMMENTS_PER_POST = 8
_MAX_COMMENT_CHARS = 400

_DEFAULT_REDDIT_KEYWORDS = (
    "pricing",
    "cost",
    "how much",
    "quote",
    "rate",
    "monthly",
)

_DEFAULT_DISCUSSION_KEYWORDS = (
    "review",
    "experience",
    "anyone used",
    "recommend",
    "worth it",
    "customer service",
)

_CUSTOMER_OR_PROSPECT_SIGNALS = re.compile(
    r"\b(review|experience|recommend|worth it|feedback|customer|client|customer service|"
    r"quality|reliable|thinking of|considering|anyone used|anyone use|"
    r"which provider|compare|alternatives?)\b",
    re.I,
)

_EMPLOYEE_OR_JOB_SIGNALS = re.compile(
    r"\b(i work(?:ed)? at|employee|interview|salary|compensation|hiring|"
    r"recruiter|manager|my boss|benefits|pto|layoff|laid off|glassdoor|"
    r"career|job opening|job offer|work culture)\b",
    re.I,
)


@dataclass
class RedditPost:
    post_id: str
    title: str
    text: str       # post body
    url: str        # full reddit.com URL
    subreddit: str
    date: str       # ISO date string
    comments: list[str] = field(default_factory=list)


def _normalize_list(values: list[str] | None) -> set[str]:
    if not values:
        return set()
    return {v.strip().lower() for v in values if v and v.strip()}


def _normalize_keywords(
    keywords: list[str] | None,
    default_keywords: tuple[str, ...],
) -> list[str]:
    source = keywords or list(default_keywords)
    seen: set[str] = set()
    result: list[str] = []
    for item in source:
        norm = item.strip().lower()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        result.append(item.strip())
        if len(result) >= _MAX_KEYWORDS:
            break
    return result


def _build_query(search_term: str, keyword: str) -> str:
    return f'"{search_term}" "{keyword}"'


def _parse_posts(
    children: list,
    include_subreddits: set[str],
    exclude_subreddits: set[str],
    mode: str = "pricing",
) -> list[RedditPost]:
    """Extract and filter RedditPost objects from a raw Reddit API children list."""
    posts: list[RedditPost] = []
    for child in children:
        p = child.get("data", {})
        title = p.get("title", "")
        body = p.get("selftext", "") or ""

        if body in ("[deleted]", "[removed]"):
            body = ""

        post_id = p.get("id", "")
        subreddit = p.get("subreddit", "")
        subreddit_norm = subreddit.lower()

        if not post_id or not title or not subreddit:
            continue

        if include_subreddits and subreddit_norm not in include_subreddits:
            continue

        if subreddit_norm in exclude_subreddits:
            continue

        if _FINANCE_SUBREDDITS.search(subreddit):
            continue

        combined = title + " " + body
        if mode == "pricing":
            if not _PRICE_SIGNALS.search(combined):
                continue
        else:
            if _EMPLOYEE_OR_JOB_SIGNALS.search(combined) and not _CUSTOMER_OR_PROSPECT_SIGNALS.search(combined):
                continue
            if not _CUSTOMER_OR_PROSPECT_SIGNALS.search(combined):
                continue

        created = p.get("created_utc", 0)
        date_str = datetime.fromtimestamp(created, tz=timezone.utc).strftime("%Y-%m-%d")

        posts.append(RedditPost(
            post_id=post_id,
            title=title,
            text=body,
            url="https://www.reddit.com" + p.get("permalink", ""),
            subreddit=subreddit,
            date=date_str,
        ))
    return posts


def _extract_comment_bodies(children: list, output: list[str]) -> None:
    for child in children:
        if child.get("kind") != "t1":
            continue
        data = child.get("data", {})
        body = (data.get("body") or "").strip()
        if body and body not in ("[deleted]", "[removed]"):
            output.append(body)

        replies = data.get("replies")
        if isinstance(replies, dict):
            reply_children = replies.get("data", {}).get("children", [])
            if isinstance(reply_children, list):
                _extract_comment_bodies(reply_children, output)


def _parse_comment_payload(payload: object) -> list[str]:
    return _parse_comment_payload_for_mode(payload, mode="pricing")


def _parse_comment_payload_for_mode(payload: object, mode: str) -> list[str]:
    if not isinstance(payload, list) or len(payload) < 2:
        return []

    comments_listing = payload[1]
    children = comments_listing.get("data", {}).get("children", [])
    if not isinstance(children, list):
        return []

    all_comments: list[str] = []
    _extract_comment_bodies(children, all_comments)

    if mode == "pricing":
        priority = [c for c in all_comments if _PRICE_SIGNALS.search(c)]
        other_comments = [c for c in all_comments if c not in priority]
    else:
        customer_signal_comments = [
            c for c in all_comments
            if _CUSTOMER_OR_PROSPECT_SIGNALS.search(c)
            and not (
                _EMPLOYEE_OR_JOB_SIGNALS.search(c)
                and not _CUSTOMER_OR_PROSPECT_SIGNALS.search(c)
            )
        ]
        neutral_comments = [
            c for c in all_comments
            if c not in customer_signal_comments
            and not (
                _EMPLOYEE_OR_JOB_SIGNALS.search(c)
                and not _CUSTOMER_OR_PROSPECT_SIGNALS.search(c)
            )
        ]
        priority = customer_signal_comments
        other_comments = neutral_comments

    ordered = priority + other_comments
    trimmed = [c[:_MAX_COMMENT_CHARS] for c in ordered[:_MAX_COMMENTS_PER_POST]]
    return trimmed


async def scrape_reddit_pricing(
    competitor_name: str,
    search_term: str,
    keywords: list[str] | None = None,
    include_subreddits: list[str] | None = None,
    exclude_subreddits: list[str] | None = None,
) -> list[RedditPost]:
    """
    Search Reddit JSON for competitor pricing discussions.

    Strategy:
    - Global keyword search (no fixed subreddit requirement)
    - One page per keyword query (conservative)
    - Include thread comments for matched posts
    - Primary endpoint: www.reddit.com; fallback: old.reddit.com
    """
    search_keywords = _normalize_keywords(keywords, _DEFAULT_REDDIT_KEYWORDS)
    include_set = _normalize_list(include_subreddits)
    exclude_set = _normalize_list(exclude_subreddits)

    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "application/json",
    }

    posts_by_id: dict[str, RedditPost] = {}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for keyword in search_keywords:
                query = _build_query(search_term, keyword)
                params = {
                    "q": query,
                    "sort": "new",
                    "t": "month",
                    "limit": _LIMIT,
                    "raw_json": 1,
                }

                data, endpoint_name = await _fetch_listing_with_fallback(
                    client=client,
                    primary_url=_REDDIT_SEARCH_PRIMARY,
                    fallback_url=_REDDIT_SEARCH_FALLBACK,
                    params=params,
                    headers=headers,
                    purpose=f"search keyword={keyword!r}",
                    log_prefix="Reddit pricing",
                )
                if data is None:
                    continue

                children = data.get("data", {}).get("children", [])
                if not isinstance(children, list):
                    logger.warning(
                        "Reddit pricing: invalid payload shape for %s (endpoint=%s)",
                        competitor_name,
                        endpoint_name,
                    )
                    continue

                parsed = _parse_posts(children, include_set, exclude_set, mode="pricing")
                for post in parsed:
                    posts_by_id.setdefault(post.post_id, post)

            if not posts_by_id:
                logger.info(
                    "Reddit pricing: found 0 posts for %s (keywords=%d)",
                    competitor_name,
                    len(search_keywords),
                )
                return []

            comment_enriched = 0
            targets = list(posts_by_id.values())[:_MAX_POSTS_FOR_COMMENT_FETCH]
            for post in targets:
                comments = await _fetch_comments_for_post(
                    client,
                    post.post_id,
                    headers,
                    mode="pricing",
                )
                if comments:
                    post.comments = comments
                    comment_enriched += 1

        posts = list(posts_by_id.values())
        logger.info(
            "Reddit pricing: found %d posts for %s across %d keyword(s); comments added for %d post(s)",
            len(posts),
            competitor_name,
            len(search_keywords),
            comment_enriched,
        )
        return posts

    except Exception as exc:
        logger.warning("Reddit pricing search failed for %s: %s", competitor_name, exc)
        raise


async def _fetch_comments_for_post(
    client: httpx.AsyncClient,
    post_id: str,
    headers: dict,
    mode: str = "pricing",
) -> list[str]:
    params = {
        "limit": 20,
        "sort": "top",
        "raw_json": 1,
    }
    primary_url = _REDDIT_COMMENTS_PRIMARY.format(post_id=post_id)
    fallback_url = _REDDIT_COMMENTS_FALLBACK.format(post_id=post_id)

    payload, endpoint_name = await _fetch_listing_with_fallback(
        client=client,
        primary_url=primary_url,
        fallback_url=fallback_url,
        params=params,
        headers=headers,
        purpose=f"comments post_id={post_id}",
        log_prefix=f"Reddit {mode}",
    )
    if payload is None:
        return []

    comments = _parse_comment_payload_for_mode(payload, mode=mode)
    if endpoint_name == "fallback" and comments:
        logger.info("Reddit %s: comments used fallback endpoint for post %s", mode, post_id)
    return comments


async def _fetch_listing_with_fallback(
    client: httpx.AsyncClient,
    primary_url: str,
    fallback_url: str,
    params: dict,
    headers: dict,
    purpose: str,
    log_prefix: str = "Reddit pricing",
) -> tuple[object | None, str]:
    for endpoint_name, endpoint_url in (
        ("primary", primary_url),
        ("fallback", fallback_url),
    ):
        data = await _fetch_listing(
            client=client,
            url=endpoint_url,
            params=params,
            headers=headers,
            endpoint_name=endpoint_name,
            purpose=purpose,
            log_prefix=log_prefix,
        )
        if data is not None:
            if endpoint_name == "fallback":
                logger.info("%s: using fallback endpoint for %s", log_prefix, purpose)
            return data, endpoint_name

    return None, "none"


async def _fetch_listing(
    client: httpx.AsyncClient,
    url: str,
    params: dict,
    headers: dict,
    endpoint_name: str,
    purpose: str,
    log_prefix: str = "Reddit pricing",
) -> object | None:
    for attempt in range(_MAX_RETRIES + 1):
        try:
            response = await client.get(url, params=params, headers=headers)

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    logger.warning(
                        "%s: invalid JSON from %s endpoint (%s): %s",
                        log_prefix,
                        endpoint_name,
                        purpose,
                        exc,
                    )
                    return None

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < _MAX_RETRIES:
                delay = _retry_delay_seconds(attempt)
                logger.warning(
                    "%s: %s endpoint retryable status %s (%s, attempt %d/%d), retrying in %.2fs",
                    log_prefix,
                    endpoint_name,
                    response.status_code,
                    purpose,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            logger.warning(
                "%s: %s endpoint failed with status %s (%s)",
                log_prefix,
                endpoint_name,
                response.status_code,
                purpose,
            )
            return None

        except httpx.RequestError as exc:
            if attempt < _MAX_RETRIES:
                delay = _retry_delay_seconds(attempt)
                logger.warning(
                    "%s: %s endpoint request error (%s, attempt %d/%d): %s; retrying in %.2fs",
                    log_prefix,
                    endpoint_name,
                    purpose,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            logger.warning(
                "%s: %s endpoint request error after retries (%s): %s",
                log_prefix,
                endpoint_name,
                purpose,
                exc,
            )
            return None

    return None


def _retry_delay_seconds(attempt: int) -> float:
    return (2 ** attempt) + random.uniform(0.1, 0.6)


async def scrape_reddit_customer_discussions(
    competitor_name: str,
    search_term: str,
    keywords: list[str] | None = None,
    include_subreddits: list[str] | None = None,
    exclude_subreddits: list[str] | None = None,
) -> list[RedditPost]:
    """
    Search Reddit JSON for customer/prospect discussion threads about a competitor.

    Strategy:
    - Conservative: one page per keyword query, month window, limit 25.
    - Filters out employee/job-centric threads and comments.
    - Includes top comments for context.
    """
    search_keywords = _normalize_keywords(keywords, _DEFAULT_DISCUSSION_KEYWORDS)
    include_set = _normalize_list(include_subreddits)
    exclude_set = _normalize_list(exclude_subreddits)

    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "application/json",
    }

    posts_by_id: dict[str, RedditPost] = {}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for keyword in search_keywords:
                query = _build_query(search_term, keyword)
                params = {
                    "q": query,
                    "sort": "new",
                    "t": "month",
                    "limit": _LIMIT,
                    "raw_json": 1,
                }

                data, endpoint_name = await _fetch_listing_with_fallback(
                    client=client,
                    primary_url=_REDDIT_SEARCH_PRIMARY,
                    fallback_url=_REDDIT_SEARCH_FALLBACK,
                    params=params,
                    headers=headers,
                    purpose=f"discussion search keyword={keyword!r}",
                    log_prefix="Reddit discussion",
                )
                if data is None:
                    continue

                children = data.get("data", {}).get("children", [])
                if not isinstance(children, list):
                    logger.warning(
                        "Reddit discussion: invalid payload shape for %s (endpoint=%s)",
                        competitor_name,
                        endpoint_name,
                    )
                    continue

                parsed = _parse_posts(children, include_set, exclude_set, mode="discussion")
                for post in parsed:
                    posts_by_id.setdefault(post.post_id, post)

            if not posts_by_id:
                logger.info(
                    "Reddit discussion: found 0 posts for %s (keywords=%d)",
                    competitor_name,
                    len(search_keywords),
                )
                return []

            comment_enriched = 0
            targets = list(posts_by_id.values())[:_MAX_POSTS_FOR_COMMENT_FETCH]
            for post in targets:
                comments = await _fetch_comments_for_post(
                    client,
                    post.post_id,
                    headers,
                    mode="discussion",
                )
                if comments:
                    post.comments = comments
                    comment_enriched += 1

        posts = list(posts_by_id.values())
        logger.info(
            "Reddit discussion: found %d posts for %s across %d keyword(s); comments added for %d post(s)",
            len(posts),
            competitor_name,
            len(search_keywords),
            comment_enriched,
        )
        return posts

    except Exception as exc:
        logger.warning("Reddit discussion search failed for %s: %s", competitor_name, exc)
        raise
