import unittest

from tracker.summarizer import (
    _ensure_pricing_priority,
    _fallback_competitor_signal_card,
    _normalize_competitor_signal_card,
)


class SummarizerTests(unittest.TestCase):
    def test_ensure_pricing_priority_injects_pricing_bullet_when_missing(self):
        summary = (
            "• Messaging shifted toward AI-first positioning.\n"
            "• Home-services audience targeting increased in LinkedIn launches."
        )
        reports = [
            {
                "competitor_name": "TechDesk Pro",
                "pricing_research_summary": (
                    "• Shifted from per-seat plans to credit-based usage pricing for AI agents."
                ),
                "pricing_change": None,
            }
        ]

        out = _ensure_pricing_priority(summary, reports, max_bullets=4)
        lines = out.splitlines()
        self.assertTrue(lines[0].startswith("• TechDesk Pro pricing update:"))
        self.assertLessEqual(len(lines), 4)

    def test_ensure_pricing_priority_does_not_duplicate_existing_pricing_bullet(self):
        summary = (
            "• TechDesk Pro shifted to credit-based pricing for AI agents.\n"
            "• Messaging moved toward enterprise automation buyers."
        )
        reports = [
            {
                "competitor_name": "TechDesk Pro",
                "pricing_research_summary": "• Shifted from per-seat plans to credit pricing.",
                "pricing_change": None,
            }
        ]

        out = _ensure_pricing_priority(summary, reports, max_bullets=4)
        lines = out.splitlines()
        self.assertEqual(lines[0], "• TechDesk Pro shifted to credit-based pricing for AI agents.")
        self.assertEqual(len(lines), 2)

    def test_ensure_pricing_priority_skips_non_material_pricing_text(self):
        summary = "• Messaging remained stable across competitors."
        reports = [
            {
                "competitor_name": "Acme",
                "pricing_research_summary": "• No concrete customer pricing figures were found.",
                "pricing_change": None,
            }
        ]

        out = _ensure_pricing_priority(summary, reports, max_bullets=4)
        self.assertEqual(out, summary)

    def test_normalize_competitor_signal_card_enforces_structure(self):
        raw = (
            "Competitor: Acme Support Co\n"
            "Signals:\n"
            "• Shifted homepage CTA to AI voice receptionist bundles.\n"
            "### Note\n"
            "2) Targeting plumbers and HVAC with seasonal offer language."
        )
        out = _normalize_competitor_signal_card(raw, "Acme Support Co")
        self.assertTrue(out.startswith("Competitor: Acme Support Co\nSignals:\n"))
        self.assertIn("- Shifted homepage CTA to AI voice receptionist bundles.", out)
        self.assertIn("- Targeting plumbers and HVAC with seasonal offer language.", out)

    def test_fallback_competitor_signal_card_includes_material_fields(self):
        report = {
            "competitor_name": "Acme",
            "pricing_research_summary": "• Moved from per-seat pricing to usage credits for AI agents.",
            "linkedin_ads_summary": "• New campaigns target electricians and plumbers with instant booking CTA.",
            "error": None,
        }
        out = _fallback_competitor_signal_card(report)
        self.assertIn("Competitor: Acme", out)
        self.assertIn("- Pricing/packaging:", out)
        self.assertIn("- Messaging/GTM:", out)


if __name__ == "__main__":
    unittest.main()
