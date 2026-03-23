import unittest
from unittest.mock import AsyncMock, patch

from tracker.scrapers import reddit_intel


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeAsyncClient:
    def __init__(self, responses: list[object]):
        self._responses = list(responses)
        self.calls: list[tuple[str, dict, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, params: dict | None = None, headers: dict | None = None):
        self.calls.append((url, params or {}, headers or {}))
        if not self._responses:
            raise AssertionError("No fake responses left for AsyncClient.get")
        next_response = self._responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response


def _listing(children: list[dict]) -> dict:
    return {"data": {"children": children, "after": None}}


def _child(
    post_id: str,
    title: str,
    *,
    body: str = "",
    subreddit: str = "virtualassistant",
    created_utc: int = 1_710_000_000,
) -> dict:
    return {
        "data": {
            "id": post_id,
            "title": title,
            "selftext": body,
            "subreddit": subreddit,
            "permalink": f"/r/{subreddit}/comments/{post_id}/test",
            "created_utc": created_utc,
        }
    }


class RedditIntelTests(unittest.IsolatedAsyncioTestCase):
    async def test_parse_posts_filters_non_pricing_and_finance_subreddits(self):
        children = [
            _child("a1", "Service plans from $99 per month", body="Starter tier"),
            _child("a2", "What is the best provider?", body="No dollar amount here"),
            _child("a3", "Company now costs $199", subreddit="stocks"),
        ]

        posts = reddit_intel._parse_posts(
            children,
            include_subreddits=set(),
            exclude_subreddits=set(),
        )

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0].post_id, "a1")
        self.assertIn("reddit.com", posts[0].url)

    async def test_scrape_reddit_pricing_uses_fallback_when_primary_retries_exhaust(self):
        fake_client = FakeAsyncClient(
            [
                FakeResponse(429),
                FakeResponse(429),
                FakeResponse(429),
                FakeResponse(200, _listing([_child("b1", "Price is $125 per month")])),
                FakeResponse(200, [{"data": {"children": []}}, {"data": {"children": []}}]),
            ]
        )

        with (
            patch.object(reddit_intel.httpx, "AsyncClient", return_value=fake_client),
            patch.object(reddit_intel.asyncio, "sleep", new=AsyncMock()),
        ):
            posts = await reddit_intel.scrape_reddit_pricing(
                "Acme",
                "Acme",
                keywords=["pricing"],
            )

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0].post_id, "b1")
        self.assertTrue(any("old.reddit.com" in url for url, _, _ in fake_client.calls))

    async def test_scrape_reddit_pricing_returns_empty_on_total_failure(self):
        # One keyword query with primary+fallback exhaustion; no comments call when no posts.
        total_attempts = (reddit_intel._MAX_RETRIES + 1) * 2
        fake_client = FakeAsyncClient([FakeResponse(503)] * total_attempts)

        with (
            patch.object(reddit_intel.httpx, "AsyncClient", return_value=fake_client),
            patch.object(reddit_intel.asyncio, "sleep", new=AsyncMock()),
        ):
            posts = await reddit_intel.scrape_reddit_pricing(
                "Acme",
                "Acme",
                keywords=["pricing"],
            )

        self.assertEqual(posts, [])
        self.assertEqual(len(fake_client.calls), total_attempts)

    async def test_comment_payload_prefers_pricing_comments(self):
        payload = [
            {"data": {"children": []}},
            {
                "data": {
                    "children": [
                        {"kind": "t1", "data": {"body": "General comment"}},
                        {"kind": "t1", "data": {"body": "We paid $299 per month"}},
                    ]
                }
            },
        ]
        comments = reddit_intel._parse_comment_payload(payload)
        self.assertEqual(comments[0], "We paid $299 per month")

    async def test_include_subreddit_filters_results(self):
        fake_client = FakeAsyncClient(
            [
                FakeResponse(
                    200,
                    _listing(
                        [
                            _child("s1", "Price is $99", subreddit="smallbusiness"),
                            _child("h1", "Price is $88", subreddit="hvac"),
                        ]
                    ),
                ),
                FakeResponse(200, [{"data": {"children": []}}, {"data": {"children": []}}]),
            ]
        )

        with (
            patch.object(reddit_intel.httpx, "AsyncClient", return_value=fake_client),
            patch.object(reddit_intel.asyncio, "sleep", new=AsyncMock()),
        ):
            posts = await reddit_intel.scrape_reddit_pricing(
                "Acme",
                "Acme",
                keywords=["pricing"],
                include_subreddits=["smallbusiness"],
            )

        self.assertEqual([p.subreddit for p in posts], ["smallbusiness"])

    async def test_parse_discussion_posts_filters_employee_threads(self):
        children = [
            _child(
                "d1",
                "Any reviews of Acme support?",
                body="Considering them for my business.",
            ),
            _child(
                "d2",
                "I work at Acme and this place is terrible",
                body="Worst manager and benefits",
            ),
        ]

        posts = reddit_intel._parse_posts(
            children,
            include_subreddits=set(),
            exclude_subreddits=set(),
            mode="discussion",
        )

        self.assertEqual([p.post_id for p in posts], ["d1"])

    async def test_discussion_comment_payload_filters_employee_noise(self):
        payload = [
            {"data": {"children": []}},
            {
                "data": {
                    "children": [
                        {"kind": "t1", "data": {"body": "I work there and benefits are bad"}},
                        {"kind": "t1", "data": {"body": "Customer here, response times were solid"}},
                        {"kind": "t1", "data": {"body": "Anyone else considering them?"}},
                    ]
                }
            },
        ]
        comments = reddit_intel._parse_comment_payload_for_mode(payload, mode="discussion")
        self.assertEqual(comments[0], "Customer here, response times were solid")
        self.assertNotIn("I work there and benefits are bad", comments)

    async def test_scrape_reddit_customer_discussions_uses_fallback_when_primary_fails(self):
        fake_client = FakeAsyncClient(
            [
                FakeResponse(429),
                FakeResponse(429),
                FakeResponse(429),
                FakeResponse(
                    200,
                    _listing(
                        [
                            _child(
                                "e1",
                                "Acme review for small businesses?",
                                body="Thinking of switching providers.",
                            )
                        ]
                    ),
                ),
                FakeResponse(
                    200,
                    [
                        {"data": {"children": []}},
                        {
                            "data": {
                                "children": [
                                    {
                                        "kind": "t1",
                                        "data": {"body": "Customer here, onboarding was smooth"},
                                    }
                                ]
                            }
                        },
                    ],
                ),
            ]
        )

        with (
            patch.object(reddit_intel.httpx, "AsyncClient", return_value=fake_client),
            patch.object(reddit_intel.asyncio, "sleep", new=AsyncMock()),
        ):
            posts = await reddit_intel.scrape_reddit_customer_discussions(
                "Acme",
                "Acme",
                keywords=["review"],
            )

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0].post_id, "e1")
        self.assertTrue(posts[0].comments)
        self.assertTrue(any("old.reddit.com" in url for url, _, _ in fake_client.calls))


if __name__ == "__main__":
    unittest.main()
