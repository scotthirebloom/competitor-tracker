import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Slack Block Kit has a 3000-char limit per text block
_MAX_BLOCK_TEXT = 2900


@dataclass
class CompetitorReport:
    competitor_name: str
    website_url: str
    homepage_change: Optional[str] = None
    blog_change: Optional[str] = None
    pricing_change: Optional[str] = None
    careers_change: Optional[str] = None
    linkedin_ads_summary: Optional[str] = None
    linkedin_organic_summary: Optional[str] = None
    pricing_research_summary: Optional[str] = None
    reddit_discussion_summary: Optional[str] = None
    twitter_summary: Optional[str] = None
    twitter_social_summary: Optional[str] = None
    facebook_summary: Optional[str] = None
    facebook_reviews_summary: Optional[str] = None
    facebook_social_summary: Optional[str] = None
    error: Optional[str] = None
    source_status: dict[str, str] = field(default_factory=dict)
    source_notes: dict[str, str] = field(default_factory=dict)
    # Counters for run_log enrichment (not displayed in Slack)
    _new_ads_count: int = 0
    _new_posts_count: int = 0
    _pages_changed_count: int = 0

    def set_source_status(
        self,
        source: str,
        status: str,
        note: Optional[str] = None,
    ) -> None:
        self.source_status[source] = status
        if note:
            self.source_notes[source] = note.strip()
        else:
            self.source_notes.pop(source, None)


_SLACK_BLOCK_LIMIT = 50


async def send_digest(
    reports: list[CompetitorReport],
    webhook_url: str,
    run_date: str,
    executive_summary: Optional[str] = None,
    summary_only: bool = False,
) -> None:
    """Build and POST the weekly digest to the Slack webhook.

    Slack allows at most 50 blocks per message. With many competitors the
    payload is split into multiple webhook calls, each ≤ 50 blocks, splitting
    only at divider boundaries so no competitor section is broken across messages.
    """
    all_blocks = _build_payload(
        reports,
        run_date,
        executive_summary=executive_summary,
        summary_only=summary_only,
    )["blocks"]
    chunks = _chunk_at_dividers(all_blocks, _SLACK_BLOCK_LIMIT)

    async with httpx.AsyncClient() as client:
        for chunk in chunks:
            try:
                response = await client.post(webhook_url, json={"blocks": chunk}, timeout=15)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else "unknown"
                logger.error("Slack digest send failed with HTTP status %s", status)
                raise RuntimeError(f"Slack digest send failed with HTTP status {status}") from None
            except httpx.HTTPError as exc:
                logger.error("Slack digest send failed (%s)", exc.__class__.__name__)
                raise RuntimeError("Slack digest send failed due to network/client error") from None

    logger.info(
        "Slack digest sent (%d message(s)) for %d competitor(s) (summary_only=%s)",
        len(chunks), len(reports), summary_only,
    )


async def send_run_started(
    webhook_url: str,
    run_date: str,
    competitor_count: int,
) -> None:
    """Post a lightweight start notification before the weekly run begins."""
    text = (
        f":hourglass_flowing_sand: *Competitive Intel — {run_date}*\n"
        f"Run started for {competitor_count} competitor(s)."
    )
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(webhook_url, json=payload)
            response.raise_for_status()
        logger.info("Slack start notification sent for %s", run_date)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning("Slack start notification failed with HTTP status %s", status)
    except httpx.HTTPError as exc:
        logger.warning("Slack start notification failed (%s)", exc.__class__.__name__)


def _build_payload(
    reports: list[CompetitorReport],
    run_date: str,
    executive_summary: Optional[str] = None,
    summary_only: bool = False,
) -> dict:
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Weekly Competitive Intel — {run_date}",
                "emoji": True,
            },
        },
        {"type": "divider"},
    ]

    if not summary_only:
        for report in reports:
            blocks.extend(_competitor_blocks(report))

    if executive_summary:
        compact_exec = _compact_model_summary(
            executive_summary,
            max_bullets=6,
            max_bullet_chars=380,
        )
        exec_text = _fit_summary_bullets(compact_exec, _MAX_BLOCK_TEXT)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": _truncate_block(f"*Executive Summary:*\n{exec_text}")},
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "_Generated by competitor-tracker · "
                        "Edit `competitors.yaml` to add/remove competitors_",
            }
        ],
    })

    return {"blocks": blocks}


def _competitor_blocks(report: CompetitorReport) -> list[dict]:
    blocks: list[dict] = []

    # Competitor header
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*{report.competitor_name}*  |  <{report.website_url}|{report.website_url}>",
        },
    })

    if report.error:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":warning: *Error:* {report.error}",
            },
        })
        blocks.append({"type": "divider"})
        return blocks

    # Website changes — one block per page type so nothing gets truncated
    any_website_change = False
    website_sources = _matching_sources(report, "website:")
    website_issues = _source_issue_lines(report, "website:")
    for label, summary in [
        ("Homepage", report.homepage_change),
        ("Blog", report.blog_change),
        ("Pricing", report.pricing_change),
        ("Careers", report.careers_change),
    ]:
        if summary:
            any_website_change = True
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _truncate_block(f"*{label}:*\n{summary}"),
                },
            })

    if website_issues:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block("*Website Coverage:*\n" + "\n".join(website_issues)),
            },
        })
    elif not any_website_change:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":white_check_mark: *Website:* No changes detected this week"
                    if website_sources else
                    "*Website:* Not configured"
                ),
            },
        })

    # Pricing intel from Reddit (only shown when new posts found)
    if report.pricing_research_summary:
        compact_pricing = _compact_model_summary(
            report.pricing_research_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(
                    f"*Pricing Intel (Reddit):*\n{compact_pricing}"
                ),
            },
        })
    else:
        pricing_issue = _single_source_issue(report, "reddit:pricing")
        if pricing_issue:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _truncate_block(f"*Pricing Intel (Reddit):*\n{pricing_issue}"),
                },
            })

    if report.reddit_discussion_summary:
        compact_discussion = _compact_model_summary(
            report.reddit_discussion_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(
                    f"*Reddit Customer Voice:*\n{compact_discussion}"
                ),
            },
        })
    else:
        discussion_issue = _single_source_issue(report, "reddit:discussion")
        if discussion_issue:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _truncate_block(f"*Reddit Customer Voice:*\n{discussion_issue}"),
                },
            })

    # Ad activity
    if report.linkedin_ads_summary:
        compact_ads = _compact_model_summary(
            report.linkedin_ads_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(f"*LinkedIn Ads:*\n{compact_ads}"),
            },
        })
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _linkedin_fallback_text(report, "linkedin:ads", "*LinkedIn Ads:* No new ads"),
            },
        })

    if report.linkedin_organic_summary:
        compact_org = _compact_model_summary(
            report.linkedin_organic_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(f"*LinkedIn Organic:*\n{compact_org}"),
            },
        })
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _linkedin_fallback_text(
                    report,
                    "linkedin:organic",
                    "*LinkedIn Organic:* No new posts in last 7 days",
                ),
            },
        })

    # Twitter — competitor's own tweets
    if report.twitter_summary:
        compact_twitter = _compact_model_summary(
            report.twitter_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(f"*Twitter:*\n{compact_twitter}"),
            },
        })

    # Twitter — third-party commentary
    if report.twitter_social_summary:
        compact_tw_social = _compact_model_summary(
            report.twitter_social_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(
                    f"*Twitter Commentary (third-party):*\n{compact_tw_social}"
                ),
            },
        })

    # Facebook — competitor's own posts
    if report.facebook_summary:
        compact_fb = _compact_model_summary(
            report.facebook_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(f"*Facebook:*\n{compact_fb}"),
            },
        })

    # Facebook reviews
    if report.facebook_reviews_summary:
        compact_fb_reviews = _compact_model_summary(
            report.facebook_reviews_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(f"*Facebook Reviews:*\n{compact_fb_reviews}"),
            },
        })

    # Facebook — third-party commentary
    if report.facebook_social_summary:
        compact_fb_social = _compact_model_summary(
            report.facebook_social_summary,
            max_bullets=3,
            max_bullet_chars=220,
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _truncate_block(
                    f"*Facebook Commentary (third-party):*\n{compact_fb_social}"
                ),
            },
        })

    blocks.append({"type": "divider"})
    return blocks


def _format_change_line(label: str, summary: Optional[str]) -> Optional[str]:
    """Format a single labelled change for mrkdwn. Returns None if no change."""
    if not summary:
        return None
    return f"*{label}:*\n{summary}"


def _matching_sources(report: CompetitorReport, prefix: str) -> list[str]:
    return [
        source for source in report.source_status
        if source.startswith(prefix)
    ]


def _source_issue_lines(report: CompetitorReport, prefix: str) -> list[str]:
    lines: list[str] = []
    for source in _matching_sources(report, prefix):
        status = report.source_status.get(source)
        if status not in {"failed", "skipped"}:
            continue
        lines.append(_format_source_issue(report, source))
    return lines


def _single_source_issue(report: CompetitorReport, source: str) -> Optional[str]:
    status = report.source_status.get(source)
    if status not in {"failed", "skipped"}:
        return None
    return _format_source_issue(report, source)


def _format_source_issue(report: CompetitorReport, source: str) -> str:
    label = _source_display_name(source)
    status = report.source_status.get(source, "failed")
    note = report.source_notes.get(source)
    prefix = ":warning:" if status == "failed" else ":information_source:"
    action = "failed" if status == "failed" else "skipped"
    detail = f" ({note})" if note else ""
    return f"{prefix} *{label}:* {action}{detail}"


def _source_display_name(source: str) -> str:
    labels = {
        "linkedin:ads": "LinkedIn Ads",
        "linkedin:organic": "LinkedIn Organic",
        "linkedin:apidirect_fallback": "LinkedIn (API Direct)",
        "reddit:pricing": "Pricing Intel (Reddit)",
        "reddit:discussion": "Reddit Customer Voice",
        "reddit:apidirect_fallback": "Reddit (API Direct)",
        "twitter:activity": "Twitter",
        "twitter:social": "Twitter Commentary",
        "facebook:posts": "Facebook",
        "facebook:reviews": "Facebook Reviews",
        "facebook:social": "Facebook Commentary",
        "website:homepage": "Homepage",
        "website:blog": "Blog",
        "website:pricing": "Pricing",
        "website:careers": "Careers",
    }
    return labels.get(source, source)


def _linkedin_fallback_text(
    report: CompetitorReport,
    source: str,
    default_text: str,
) -> str:
    status = report.source_status.get(source)
    if status == "not_configured":
        return default_text.replace("No new ads", "Not configured").replace(
            "No new posts in last 7 days",
            "Not configured",
        )

    issue = _single_source_issue(report, source)
    if issue:
        return issue
    return default_text


def _truncate_block(text: str) -> str:
    if len(text) <= _MAX_BLOCK_TEXT:
        return text
    return text[:_MAX_BLOCK_TEXT] + "\n_[truncated]_"


def _compact_model_summary(
    summary: str,
    max_bullets: int = 3,
    max_bullet_chars: int = 220,
) -> str:
    """
    Normalize model output into a short bullet list for Slack.
    Prevents verbose step-by-step or heading-heavy responses from flooding digest blocks.
    """
    if not summary:
        return ""

    candidates: list[str] = []
    lines = _expand_inline_bullets(summary)
    synth_start: Optional[int] = None
    for idx, raw in enumerate(lines):
        lower = raw.strip().lower()
        if "step 3" in lower or "synthes" in lower:
            synth_start = idx + 1
            break

    scan_sets: list[list[str]] = []
    if synth_start is not None:
        scan_sets.append(lines[synth_start:])
    scan_sets.append(lines)
    heading_pat = re.compile(r"^#{1,6}\s+")
    bullet_pat = re.compile(r"^(?:[•\-\*]\s+|\d+[.)]\s+)")
    skip_prefixes = (
        "step 1",
        "step 2",
        "step 3",
        "extract",
        "normalize",
        "synthesize",
        "important",
        "based on the provided",
        "here is the extraction",
    )

    for scan in scan_sets:
        for raw in scan:
            line = raw.strip()
            if not line:
                continue
            lower = line.lower()

            if heading_pat.match(line):
                continue
            if lower.startswith(skip_prefixes):
                continue

            # Prefer explicit bullets/numbered points
            if bullet_pat.match(line):
                line = bullet_pat.sub("", line).strip()

            # Skip standalone heading-like lines
            if line.endswith(":") and len(line.split()) <= 5:
                continue

            # Remove excessive markdown emphasis wrappers
            line = line.replace("**", "").strip()
            if line:
                candidates.append(line)
        if len(candidates) >= max_bullets:
            break

    if not candidates:
        fallback = re.sub(r"\s+", " ", summary).strip()
        candidates = [s.strip() for s in re.split(r"(?<=[.!?])\s+", fallback) if s.strip()]

    seen: set[str] = set()
    bullets: list[str] = []
    for item in candidates:
        text = re.sub(r"\s+", " ", item).strip(" •-*")
        lower = text.lower()
        if not text or lower in seen:
            continue
        if lower.startswith(skip_prefixes):
            continue
        seen.add(lower)
        bullets.append(f"• {_smart_truncate(text, max_bullet_chars)}")
        if len(bullets) >= max_bullets:
            break

    return "\n".join(bullets) if bullets else "• Summary unavailable"


def _expand_inline_bullets(summary: str) -> list[str]:
    """
    Expand single-line model outputs like:
    "• one • two • three"
    into one bullet candidate per line.
    """
    expanded: list[str] = []
    for raw in summary.splitlines():
        line = raw.strip()
        if not line:
            continue
        segments = re.split(r"(?=(?:•\s+|\d+[.)]\s+))", line)
        for seg in segments:
            chunk = seg.strip()
            if chunk:
                expanded.append(chunk)
    return expanded or summary.splitlines()


def _fit_summary_bullets(summary: str, max_chars: int) -> str:
    """
    Fit bullet summaries to Slack limits without cutting a bullet mid-sentence.
    Falls back to _truncate_block for non-bullet content.
    """
    if len(summary) <= max_chars:
        return summary

    lines = [line.strip() for line in summary.splitlines() if line.strip()]
    if not lines or not all(line.startswith("•") for line in lines):
        return _truncate_block(summary)

    trunc_notice = "• Additional points truncated for Slack length."
    reserve = len(trunc_notice) + 1
    budget = max(200, max_chars - reserve)

    kept: list[str] = []
    used = 0
    for line in lines:
        add = len(line) + (1 if kept else 0)
        if used + add > budget:
            break
        kept.append(line)
        used += add

    if not kept:
        return _truncate_block(summary)
    if len(kept) < len(lines):
        kept.append(trunc_notice)
    return "\n".join(kept)


def _smart_truncate(text: str, max_chars: int) -> str:
    """
    Truncate without cutting mid-word when possible, and prefer sentence endings.
    """
    if len(text) <= max_chars:
        return text

    head = text[:max_chars].rstrip()

    punct_positions = [head.rfind(ch) for ch in ".!?;"]
    punct = max(punct_positions)
    if punct >= 60:
        return head[: punct + 1].strip()

    space = head.rfind(" ")
    if space >= 40:
        return head[:space].rstrip() + "…"
    return head.rstrip() + "…"


def _chunk_at_dividers(blocks: list[dict], max_size: int) -> list[list[dict]]:
    """Split a flat block list into groups of at most max_size.

    Splits only at divider boundaries so competitor sections are never broken
    across messages. Falls back to a hard split if no divider is found in range
    (shouldn't happen with normal reporter output).
    """
    if len(blocks) <= max_size:
        return [blocks]

    divider_positions = [i for i, b in enumerate(blocks) if b["type"] == "divider"]

    chunks: list[list[dict]] = []
    start = 0

    while start < len(blocks):
        end = start + max_size
        if end >= len(blocks):
            chunks.append(blocks[start:])
            break

        # Find the last divider that falls within [start+1, end] and split after it
        split_at = None
        for pos in reversed(divider_positions):
            if start < pos <= end:
                split_at = pos + 1  # include the divider in this chunk
                break

        if split_at is None:
            split_at = end  # hard split — no divider in range

        chunks.append(blocks[start:split_at])
        start = split_at

    return chunks
