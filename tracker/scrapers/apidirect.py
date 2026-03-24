"""API Direct client — unified access to Reddit, LinkedIn, Facebook, Twitter.

Used as:
  - Fallback for LinkedIn/Reddit when primary scrapers fail
  - Primary source for Facebook and Twitter data
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://apidirect.io"
_TIMEOUT = 15.0
_MAX_RETRIES = 2
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Endpoint category keys (must match apidirect_usage table)
ENDPOINT_REDDIT_POSTS = "reddit_posts"
ENDPOINT_LINKEDIN_POSTS = "linkedin_posts"
ENDPOINT_LINKEDIN_COMPANY = "linkedin_company_posts"
ENDPOINT_FACEBOOK_PAGE_POSTS = "facebook_page_posts"
ENDPOINT_FACEBOOK_REVIEWS = "facebook_page_reviews"
ENDPOINT_FACEBOOK_SEARCH = "facebook_search"
ENDPOINT_TWITTER_SEARCH = "twitter_search"
ENDPOINT_TWITTER_USER = "twitter_user_tweets"


@dataclass
class ApiDirectPost:
    post_id: str
    title: str
    text: str
    url: str
    source: str  # reddit, linkedin, facebook, twitter
    author: str
    date: str  # ISO-ish or raw from API
    engagement: dict = field(default_factory=dict)
    is_competitor_owned: bool = False


class BudgetExhaustedError(Exception):
    """Raised when monthly request budget for an endpoint is depleted."""


class ApiDirectClient:
    """Thin async wrapper around the API Direct REST API.

    Enforces:
    - Max 3 concurrent requests per endpoint (API limit)
    - Monthly request budget per endpoint (free tier: 50)
    - Auth guard: disables all calls after a 401/403
    """

    def __init__(
        self,
        api_key: str,
        db: object,  # tracker.database.Database instance
        monthly_limit: int = 50,
    ) -> None:
        self._api_key = api_key
        self._db = db
        self._monthly_limit = monthly_limit
        self._semaphore = asyncio.Semaphore(3)
        self._api_disabled = False
        self._client: Optional[httpx.AsyncClient] = None
        # Per-run counters for cost logging
        self.request_counts: dict[str, int] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=_TIMEOUT,
                headers={"X-API-Key": self._api_key},
            )
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── Budget tracking ────────────────────────────────────────────────

    def _check_budget(self, endpoint: str) -> bool:
        """Return True if we have remaining budget for this endpoint."""
        used = self._db.get_apidirect_usage(endpoint)
        return used < self._monthly_limit

    def _record_usage(self, endpoint: str) -> None:
        """Increment monthly usage counter for endpoint."""
        self._db.increment_apidirect_usage(endpoint)
        self.request_counts[endpoint] = self.request_counts.get(endpoint, 0) + 1

    # ── Core request method ────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        endpoint_key: str,
        params: Optional[dict] = None,
    ) -> Optional[dict]:
        """Make a rate-limited, budget-checked, retrying API request."""
        if self._api_disabled:
            logger.debug("API Direct disabled (auth failure); skipping %s", path)
            return None

        if not self._check_budget(endpoint_key):
            raise BudgetExhaustedError(
                f"Monthly budget exhausted for {endpoint_key} "
                f"({self._monthly_limit} requests)"
            )

        client = await self._get_client()
        url = f"{_BASE_URL}{path}"
        last_exc: Optional[Exception] = None

        for attempt in range(_MAX_RETRIES + 1):
            async with self._semaphore:
                try:
                    resp = await client.request(method, url, params=params)

                    if resp.status_code in (401, 403):
                        logger.warning(
                            "API Direct auth error %d on %s — disabling for this run",
                            resp.status_code, path,
                        )
                        self._api_disabled = True
                        return None

                    if resp.status_code in _RETRYABLE_STATUSES:
                        if attempt < _MAX_RETRIES:
                            wait = (2 ** attempt) + 0.5
                            logger.info(
                                "API Direct %d on %s, retry %d in %.1fs",
                                resp.status_code, path, attempt + 1, wait,
                            )
                            await asyncio.sleep(wait)
                            continue
                        logger.warning(
                            "API Direct %d on %s after %d retries",
                            resp.status_code, path, _MAX_RETRIES,
                        )
                        return None

                    if resp.status_code != 200:
                        logger.warning(
                            "API Direct unexpected %d on %s: %s",
                            resp.status_code, path, resp.text[:200],
                        )
                        return None

                    self._record_usage(endpoint_key)
                    return resp.json()

                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_exc = exc
                    if attempt < _MAX_RETRIES:
                        wait = (2 ** attempt) + 0.5
                        logger.info(
                            "API Direct network error on %s (%s), retry %d in %.1fs",
                            path, exc, attempt + 1, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(
                            "API Direct network error on %s after %d retries: %s",
                            path, _MAX_RETRIES, last_exc,
                        )

        return None

    # ── Reddit ─────────────────────────────────────────────────────────

    async def search_reddit(
        self,
        query: str,
        pages: int = 1,
        sort: str = "most_recent",
    ) -> list[ApiDirectPost]:
        """Search Reddit posts by keyword."""
        data = await self._request(
            "GET", "/v1/reddit/posts", ENDPOINT_REDDIT_POSTS,
            params={"query": query[:500], "pages": pages, "sort_by": sort},
        )
        return self._parse_posts(data, "reddit", is_competitor_owned=False)

    # ── LinkedIn ───────────────────────────────────────────────────────

    async def search_linkedin_posts(
        self, query: str
    ) -> list[ApiDirectPost]:
        """Search LinkedIn posts by keyword."""
        data = await self._request(
            "GET", "/v1/linkedin/posts", ENDPOINT_LINKEDIN_POSTS,
            params={"query": query[:500]},
        )
        return self._parse_posts(data, "linkedin", is_competitor_owned=False)

    async def get_linkedin_company_posts(
        self, company_url: str
    ) -> list[ApiDirectPost]:
        """Get posts from a specific LinkedIn company page.

        Args:
            company_url: Full LinkedIn company URL (e.g. https://www.linkedin.com/company/275807/)
        """
        data = await self._request(
            "GET", "/v1/linkedin/company/posts", ENDPOINT_LINKEDIN_COMPANY,
            params={"url": company_url[:500]},
        )
        return self._parse_posts(data, "linkedin", is_competitor_owned=True)

    # ── Facebook ───────────────────────────────────────────────────────

    async def get_facebook_page_posts(
        self, page_id: str
    ) -> list[ApiDirectPost]:
        """Get posts from a Facebook page."""
        data = await self._request(
            "GET", "/v1/facebook/page/posts", ENDPOINT_FACEBOOK_PAGE_POSTS,
            params={"page_id": page_id},
        )
        return self._parse_posts(data, "facebook", is_competitor_owned=True)

    async def get_facebook_page_reviews(
        self, page_id: str
    ) -> list[ApiDirectPost]:
        """Get reviews from a Facebook page."""
        data = await self._request(
            "GET", "/v1/facebook/page/reviews", ENDPOINT_FACEBOOK_REVIEWS,
            params={"page_id": page_id},
        )
        return self._parse_posts(data, "facebook", is_competitor_owned=False)

    async def search_facebook(
        self, query: str
    ) -> list[ApiDirectPost]:
        """Search Facebook posts by keyword."""
        data = await self._request(
            "GET", "/v1/facebook/posts", ENDPOINT_FACEBOOK_SEARCH,
            params={"query": query[:500]},
        )
        return self._parse_posts(data, "facebook", is_competitor_owned=False)

    # ── Twitter ────────────────────────────────────────────────────────

    async def search_twitter(
        self, query: str
    ) -> list[ApiDirectPost]:
        """Search tweets by keyword."""
        data = await self._request(
            "GET", "/v1/twitter/posts", ENDPOINT_TWITTER_SEARCH,
            params={"query": query[:500]},
        )
        return self._parse_posts(data, "twitter", is_competitor_owned=False)

    async def get_twitter_user_tweets(
        self, handle: str
    ) -> list[ApiDirectPost]:
        """Get tweets from a specific Twitter user."""
        data = await self._request(
            "GET", "/v1/twitter/user/tweets", ENDPOINT_TWITTER_USER,
            params={"username": handle},
        )
        return self._parse_posts(data, "twitter", is_competitor_owned=True)

    # ── Response parsing ───────────────────────────────────────────────

    def _parse_posts(
        self,
        data: Optional[dict],
        source: str,
        is_competitor_owned: bool,
    ) -> list[ApiDirectPost]:
        """Normalize API Direct response into ApiDirectPost list."""
        if not data:
            return []

        posts_raw = (
            data.get("posts")
            or data.get("tweets")
            or data.get("reviews")
            or data.get("data")
            or []
        )
        if not isinstance(posts_raw, list):
            return []

        results: list[ApiDirectPost] = []
        for item in posts_raw:
            try:
                text = item.get("snippet") or item.get("text") or item.get("body") or ""
                title = item.get("title") or ""
                url = item.get("url") or ""
                author = item.get("author") or item.get("username") or ""
                date = item.get("date") or ""

                # Generate stable post_id from URL or content hash
                id_source = url or f"{title}{text[:100]}"
                post_id = hashlib.sha256(id_source.encode("utf-8")).hexdigest()[:16]

                engagement = {}
                for key in ("likes", "comments", "shares", "views", "upvotes",
                            "retweets", "reactions", "awards"):
                    if key in item and item[key] is not None:
                        engagement[key] = item[key]

                results.append(ApiDirectPost(
                    post_id=post_id,
                    title=title,
                    text=text,
                    url=url,
                    source=source,
                    author=author,
                    date=date,
                    engagement=engagement,
                    is_competitor_owned=is_competitor_owned,
                ))
            except Exception as exc:
                logger.debug("Failed to parse API Direct item: %s", exc)
                continue

        return results

    # ── Cost summary ───────────────────────────────────────────────────

    def get_usage_summary(self) -> str:
        """Return a one-line summary of API Direct usage this run."""
        if not self.request_counts:
            return "API Direct: no requests made"
        parts = [f"{k}={v}" for k, v in sorted(self.request_counts.items())]
        total = sum(self.request_counts.values())
        return f"API Direct usage: {', '.join(parts)} (total={total})"
