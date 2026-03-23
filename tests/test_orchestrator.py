import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from tracker.config import CompetitorConfig
from tracker.database import Database
from tracker.orchestrator import _build_coverage_bullet, _process_competitor
from tracker.reporter import CompetitorReport
from tracker.scrapers.reddit_intel import RedditPost
from tracker.scrapers.website import WebsiteResult


class OrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_competitor_uses_reddit_pricing_when_pricing_page_fails(self):
        competitor = CompetitorConfig(
            name="Acme",
            website="https://acme.test",
            homepage_url="https://acme.test/",
            pricing_url="https://acme.test/pricing",
        )
        reddit_posts = [
            RedditPost(
                post_id="rp1",
                title="Acme costs $199 per month",
                text="We were quoted $199 per month.",
                url="https://reddit.example/r/test/rp1",
                subreddit="smallbusiness",
                date="2026-03-01",
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(Path(tmpdir) / "state.db")
            try:
                with (
                    patch(
                        "tracker.orchestrator.scrape_homepage",
                        AsyncMock(
                            return_value=WebsiteResult(
                                competitor_name="Acme",
                                page_type="homepage",
                                url="https://acme.test/",
                                text="Acme homepage",
                            )
                        ),
                    ),
                    patch(
                        "tracker.orchestrator.scrape_pricing",
                        AsyncMock(
                            return_value=WebsiteResult(
                                competitor_name="Acme",
                                page_type="pricing",
                                url="https://acme.test/pricing",
                                text="",
                                error="timeout",
                            )
                        ),
                    ),
                    patch(
                        "tracker.orchestrator.scrape_reddit_pricing",
                        AsyncMock(return_value=reddit_posts),
                    ),
                    patch(
                        "tracker.orchestrator.scrape_reddit_customer_discussions",
                        AsyncMock(return_value=[]),
                    ),
                    patch(
                        "tracker.orchestrator.summarize_pricing_research",
                        AsyncMock(return_value="• Pricing mentioned at $199/month."),
                    ),
                    patch("tracker.orchestrator.random_delay", AsyncMock()),
                ):
                    report, linkedin_auth_failed, linkedin_reauth_attempted = await _process_competitor(
                        competitor=competitor,
                        db=db,
                        browser=object(),
                        session_path=Path(tmpdir) / "linkedin_session.json",
                        linkedin_username=None,
                        linkedin_password=None,
                        linkedin_auth_failed=False,
                        linkedin_reauth_attempted=False,
                        debug=False,
                    )
            finally:
                db.close()

        self.assertFalse(linkedin_auth_failed)
        self.assertFalse(linkedin_reauth_attempted)
        self.assertEqual(report.source_status["website:homepage"], "ok")
        self.assertEqual(report.source_status["website:pricing"], "failed")
        self.assertEqual(report.source_status["reddit:pricing"], "ok")
        self.assertEqual(report.source_status["reddit:discussion"], "ok")
        self.assertEqual(report.source_status["linkedin:ads"], "not_configured")
        self.assertEqual(report.source_status["linkedin:organic"], "not_configured")
        self.assertEqual(report.pricing_research_summary, "• Pricing mentioned at $199/month.")

    def test_build_coverage_bullet_counts_only_actionable_gaps(self):
        report = CompetitorReport(
            competitor_name="Acme",
            website_url="https://acme.test",
        )
        report.set_source_status("website:pricing", "failed", "timeout")
        report.set_source_status("reddit:pricing", "not_needed", "pricing page already exposes concrete pricing")
        report.set_source_status("linkedin:ads", "skipped", "session expired earlier in run")

        bullet = _build_coverage_bullet([report])

        self.assertIn("website 1", bullet)
        self.assertIn("LinkedIn 1", bullet)
        self.assertNotIn("Reddit", bullet)


if __name__ == "__main__":
    unittest.main()
