import unittest

from tracker.scrapers.linkedin_ads import (
    _build_linkedin_organic_urls,
    _build_linkedin_posts_url,
    _extract_post_id,
    _is_within_days,
    _summarize_linkedin_page_state,
)


class LinkedInOrganicHelpersTests(unittest.TestCase):
    def test_build_posts_url_uses_explicit_company_url(self):
        url = _build_linkedin_posts_url(
            "12345",
            "https://www.linkedin.com/company/example-company/",
        )
        self.assertEqual(url, "https://www.linkedin.com/company/example-company/posts/?feedView=all")

    def test_build_posts_url_falls_back_to_company_id(self):
        url = _build_linkedin_posts_url("12345", None)
        self.assertEqual(url, "https://www.linkedin.com/company/12345/posts/?feedView=all")

    def test_build_organic_urls_adds_member_view_without_duplicates(self):
        urls = _build_linkedin_organic_urls("12345", "https://www.linkedin.com/company/12345/posts/")
        self.assertEqual(
            urls,
            [
                "https://www.linkedin.com/company/12345/posts/?feedView=all",
                "https://www.linkedin.com/company/12345/posts/?feedView=all&viewAsMember=true",
                "https://www.linkedin.com/company/12345/",
            ],
        )

    def test_extract_post_id_prefers_activity_id(self):
        post_id = _extract_post_id(
            "https://www.linkedin.com/feed/update/urn:li:activity:7346333881796405248/",
            "text",
            "1d",
        )
        self.assertEqual(post_id, "7346333881796405248")

    def test_recency_parser_relative_labels(self):
        self.assertTrue(_is_within_days("3d", 7))
        self.assertTrue(_is_within_days("1 week", 7))
        self.assertFalse(_is_within_days("2 weeks", 7))
        self.assertFalse(_is_within_days("1mo", 7))

    def test_recency_parser_absolute_dates(self):
        self.assertTrue(_is_within_days("Yesterday", 7))
        self.assertFalse(_is_within_days("January 1, 2020", 7))

    def test_page_hint_summary_detects_sign_in_and_challenge(self):
        hint = _summarize_linkedin_page_state("Sign in to LinkedIn. Complete security verification challenge.")
        self.assertIn("sign-in", hint)
        self.assertIn("security-verification", hint)
        self.assertIn("challenge", hint)


if __name__ == "__main__":
    unittest.main()
