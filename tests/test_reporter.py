import unittest

from tracker.reporter import (
    CompetitorReport,
    _build_payload,
    _compact_model_summary,
    _fit_summary_bullets,
)


class ReporterTests(unittest.TestCase):
    def test_build_payload_summary_only_omits_competitor_sections(self):
        reports = [
            CompetitorReport(
                competitor_name="Acme Corp",
                website_url="https://acmecorp.example.com",
                linkedin_ads_summary="• test",
            )
        ]
        payload = _build_payload(
            reports,
            run_date="2026-03-02",
            executive_summary="• Pricing shifted to usage credits.",
            summary_only=True,
        )

        text_blocks = [
            b.get("text", {}).get("text", "")
            for b in payload["blocks"]
            if isinstance(b.get("text"), dict)
        ]
        joined = "\n".join(text_blocks)
        self.assertIn("*Executive Summary:*", joined)
        self.assertNotIn("Acme Corp", joined)

    def test_compact_model_summary_truncates_cleanly(self):
        raw = "• Managed services pricing changed significantly across enterprise bundles with multi-year minimum commitments and add-on overages."
        compact = _compact_model_summary(raw, max_bullets=1, max_bullet_chars=55)
        self.assertTrue(compact.startswith("• "))
        # Should not hard-cut mid-word in the final bullet.
        self.assertFalse(compact.endswith("bund"))

    def test_compact_model_summary_splits_inline_bullets(self):
        raw = "• First signal on pricing. • Second signal on home services targeting. • Third signal with risk note."
        compact = _compact_model_summary(raw, max_bullets=3, max_bullet_chars=120)
        lines = [line for line in compact.splitlines() if line.strip()]
        self.assertEqual(len(lines), 3)
        self.assertIn("Second signal on home services targeting.", compact)

    def test_fit_summary_bullets_avoids_mid_bullet_cut(self):
        bullets = "\n".join(
            [
                "• " + ("A" * 120),
                "• " + ("B" * 120),
                "• " + ("C" * 120),
            ]
        )
        fitted = _fit_summary_bullets(bullets, max_chars=240)
        self.assertNotIn("• " + ("C" * 60), fitted)
        self.assertIn("• Additional points truncated for Slack length.", fitted)

    def test_payload_surfaces_linkedin_failures_instead_of_no_new_defaults(self):
        report = CompetitorReport(
            competitor_name="Acme Corp",
            website_url="https://acmecorp.example.com",
        )
        report.set_source_status("linkedin:ads", "failed", "auth wall")
        report.set_source_status("linkedin:organic", "failed", "checkpoint")

        payload = _build_payload([report], run_date="2026-03-02", summary_only=False)
        text_blocks = [
            b.get("text", {}).get("text", "")
            for b in payload["blocks"]
            if isinstance(b.get("text"), dict)
        ]
        joined = "\n".join(text_blocks)
        self.assertIn("auth wall", joined)
        self.assertIn("checkpoint", joined)
        self.assertNotIn("*LinkedIn Ads:* No new ads", joined)
        self.assertNotIn("*LinkedIn Organic:* No new posts in last 7 days", joined)


if __name__ == "__main__":
    unittest.main()
