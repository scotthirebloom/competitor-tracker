import asyncio
import logging
import random
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .browser import get_browser, random_delay
from .config import AppConfig, CompetitorConfig
from .database import Database, AdSnapshot, PageSnapshot
from .reporter import CompetitorReport, send_digest, send_run_started
from .scrapers.reddit_intel import (
    RedditPost,
    scrape_reddit_customer_discussions,
    scrape_reddit_pricing,
)
from .scrapers.linkedin_ads import (
    AuthExpiredError,
    LinkedInOrganicPostResult,
    scrape_linkedin_ads,
    scrape_linkedin_organic_posts,
)
from .scrapers.website import (
    WebsiteResult,
    scrape_blog,
    scrape_careers,
    scrape_homepage,
    scrape_pricing,
)
from .summarizer import (
    summarize_executive_takeaways,
    summarize_linkedin_organic_posts,
    summarize_new_ads,
    summarize_new_jobs,
    summarize_reddit_customer_discussions,
    summarize_pricing_research,
    summarize_website_change,
)

logger = logging.getLogger(__name__)

# Regex to detect a specific dollar amount in text (e.g. "$99", "$1,200/mo")
_PRICE_RE = re.compile(r'\$\s*\d[\d,]*', re.I)

def _has_specific_price(text: str) -> bool:
    """Return True if text contains a concrete dollar price."""
    return bool(_PRICE_RE.search(text))


async def run_weekly(config: AppConfig) -> None:
    """
    Main entry point for a single weekly run.
    Processes all competitors, isolates failures, and sends the Slack digest.
    """
    db = Database(config.db_path)
    run_date = datetime.now().astimezone().strftime("%Y-%m-%d")
    reports: list[CompetitorReport] = []
    linkedin_auth_failed = False
    linkedin_reauth_attempted = False

    try:
        await send_run_started(
            webhook_url=config.slack_webhook_url,
            run_date=run_date,
            competitor_count=len(config.competitors),
        )

        async with get_browser(headless=not config.debug) as browser:
            for i, competitor in enumerate(config.competitors):
                logger.info("Processing %s ...", competitor.name)
                try:
                    report, linkedin_auth_failed, linkedin_reauth_attempted = await _process_competitor(
                        competitor,
                        db,
                        browser,
                        config.session_path,
                        linkedin_username=config.linkedin_username,
                        linkedin_password=config.linkedin_password,
                        linkedin_auth_failed=linkedin_auth_failed,
                        linkedin_reauth_attempted=linkedin_reauth_attempted,
                        debug=config.debug,
                    )
                    reports.append(report)
                    db.log_run(competitor.name, "success")
                except Exception as exc:
                    logger.error(
                        "Fatal error processing %s: %s",
                        competitor.name, exc, exc_info=True
                    )
                    reports.append(CompetitorReport(
                        competitor_name=competitor.name,
                        website_url=competitor.website,
                        error=str(exc),
                    ))
                    db.log_run(competitor.name, "error", str(exc))

                # Pause between competitors — avoids hammering sites in rapid succession
                if i < len(config.competitors) - 1:
                    delay = random.uniform(20, 50)
                    logger.info(
                        "Waiting %.0f seconds before next competitor...", delay
                    )
                    await asyncio.sleep(delay)

        executive_summary = await _generate_executive_summary(reports)
        coverage_bullet = _build_coverage_bullet(reports)
        if coverage_bullet:
            executive_summary = _append_exec_bullet(
                executive_summary,
                coverage_bullet,
                max_bullets=6,
            )

        if linkedin_auth_failed:
            auth_note = (
                "• LinkedIn auth expired during this run; LinkedIn scraping paused. "
                "Run `python setup_auth.py` and rerun to restore LinkedIn coverage."
            )
            executive_summary = _append_exec_bullet(
                executive_summary,
                auth_note,
                max_bullets=6,
            )

        await send_digest(
            reports,
            config.slack_webhook_url,
            run_date,
            executive_summary=executive_summary,
            summary_only=True,
        )

    finally:
        db.close()


async def run_linkedin_recovery(config: AppConfig) -> None:
    """
    One-off recovery mode:
    - scrape only LinkedIn ads + organic posts
    - store new data in DB
    - resend only executive summary
    """
    db = Database(config.db_path)
    run_date = datetime.now().astimezone().strftime("%Y-%m-%d")
    reports: list[CompetitorReport] = []
    linkedin_auth_failed = False
    linkedin_reauth_attempted = False

    linkedin_competitors = [c for c in config.competitors if c.linkedin_company_id]

    try:
        await send_run_started(
            webhook_url=config.slack_webhook_url,
            run_date=run_date,
            competitor_count=len(linkedin_competitors),
        )

        if not linkedin_competitors:
            await send_digest(
                reports=[],
                webhook_url=config.slack_webhook_url,
                run_date=run_date,
                executive_summary=(
                    "• LinkedIn recovery run skipped: no competitors with "
                    "`linkedin_company_id` are configured."
                ),
                summary_only=True,
            )
            return

        async with get_browser(headless=not config.debug) as browser:
            for i, competitor in enumerate(linkedin_competitors):
                logger.info("LinkedIn recovery: processing %s ...", competitor.name)
                report = CompetitorReport(
                    competitor_name=competitor.name,
                    website_url=competitor.website,
                )

                try:
                    if linkedin_auth_failed:
                        _set_linkedin_auth_warning(report)
                    else:
                        await _collect_linkedin_signals(
                            browser=browser,
                            company_id=competitor.linkedin_company_id or "",
                            competitor_name=competitor.name,
                            session_path=config.session_path,
                            linkedin_username=config.linkedin_username,
                            linkedin_password=config.linkedin_password,
                            company_url=competitor.linkedin_company_url,
                            db=db,
                            report=report,
                            debug=config.debug,
                        )
                    db.log_run(competitor.name, "success")
                except AuthExpiredError as exc:
                    logger.error("LinkedIn auth expired during recovery: %s", exc)
                    if not linkedin_reauth_attempted and await _attempt_linkedin_reauth_interactive():
                        linkedin_reauth_attempted = True
                        logger.info(
                            "LinkedIn session refreshed in recovery; retrying %s once",
                            competitor.name,
                        )
                        try:
                            await random_delay(2.0, 5.0)
                            await _collect_linkedin_signals(
                                browser=browser,
                                company_id=competitor.linkedin_company_id or "",
                                competitor_name=competitor.name,
                                session_path=config.session_path,
                                linkedin_username=config.linkedin_username,
                                linkedin_password=config.linkedin_password,
                                company_url=competitor.linkedin_company_url,
                                db=db,
                                report=report,
                                debug=config.debug,
                            )
                            db.log_run(competitor.name, "success")
                        except Exception as retry_exc:
                            logger.warning(
                                "LinkedIn recovery retry failed for %s: %s",
                                competitor.name,
                                retry_exc,
                            )
                            linkedin_auth_failed = True
                            _set_linkedin_auth_warning(report)
                            db.log_run(competitor.name, "error", str(retry_exc))
                    else:
                        linkedin_reauth_attempted = True
                        linkedin_auth_failed = True
                        _set_linkedin_auth_warning(report)
                        db.log_run(competitor.name, "error", str(exc))
                except Exception as exc:
                    logger.warning("LinkedIn recovery error for %s: %s", competitor.name, exc)
                    report.error = f"LinkedIn recovery failed: {exc}"
                    db.log_run(competitor.name, "error", str(exc))

                reports.append(report)

                if i < len(linkedin_competitors) - 1:
                    # Conservative pacing between competitors for LinkedIn-only pass.
                    await asyncio.sleep(random.uniform(12, 28))

        executive_summary = await _generate_executive_summary(reports)
        coverage_bullet = _build_coverage_bullet(reports)
        if coverage_bullet:
            executive_summary = _append_exec_bullet(
                executive_summary,
                coverage_bullet,
                max_bullets=6,
            )

        has_new_linkedin_summary = any(
            (r.linkedin_ads_summary or r.linkedin_organic_summary) for r in reports
        )
        if not has_new_linkedin_summary:
            executive_summary = _append_exec_bullet(
                executive_summary,
                "• LinkedIn recovery completed, but no net-new LinkedIn ads/posts were detected.",
                max_bullets=4,
            )

        if linkedin_auth_failed:
            executive_summary = _append_exec_bullet(
                executive_summary,
                "• LinkedIn auth expired during recovery; some competitors may still be missing LinkedIn coverage.",
                max_bullets=6,
            )

        await send_digest(
            reports,
            config.slack_webhook_url,
            run_date,
            executive_summary=executive_summary,
            summary_only=True,
        )
    finally:
        db.close()


async def _process_competitor(
    competitor: CompetitorConfig,
    db: Database,
    browser,
    session_path: Path,
    linkedin_username: Optional[str],
    linkedin_password: Optional[str],
    linkedin_auth_failed: bool,
    linkedin_reauth_attempted: bool,
    debug: bool = False,
) -> tuple[CompetitorReport, bool, bool]:
    report = CompetitorReport(
        competitor_name=competitor.name,
        website_url=competitor.website,
    )
    pricing_page_text: Optional[str] = None
    pricing_page_failed = False

    # --- Website scrapes ---
    tasks = {}
    if competitor.homepage_url:
        tasks["homepage"] = scrape_homepage(browser, competitor.name, competitor.homepage_url)
    if competitor.blog_url:
        tasks["blog"] = scrape_blog(browser, competitor.name, competitor.blog_url)
    if competitor.pricing_url:
        tasks["pricing"] = scrape_pricing(browser, competitor.name, competitor.pricing_url)
    if competitor.careers_url:
        tasks["careers"] = scrape_careers(browser, competitor.name, competitor.careers_url)

    if tasks:
        # Run sequentially — concurrent requests to the same domain look bot-like
        for page_type, coro in tasks.items():
            try:
                result = await coro
                if result.error:
                    report.set_source_status(
                        f"website:{page_type}",
                        "failed",
                        _truncate_note(result.error),
                    )
                    if page_type == "pricing":
                        pricing_page_failed = True
                elif not result.text:
                    report.set_source_status(
                        f"website:{page_type}",
                        "failed",
                        "empty response",
                    )
                    if page_type == "pricing":
                        pricing_page_failed = True
                else:
                    await _handle_website_result(result, db, report)
                    report.set_source_status(f"website:{page_type}", "ok")
                    if page_type == "pricing":
                        pricing_page_text = result.text
            except Exception as exc:
                logger.warning(
                    "Scrape error for %s/%s: %s", competitor.name, page_type, exc
                )
                report.set_source_status(
                    f"website:{page_type}",
                    "failed",
                    _truncate_note(str(exc)),
                )
                if page_type == "pricing":
                    pricing_page_failed = True
            # Short pause between page types on the same domain
            await random_delay(3.0, 8.0)

    # --- Reddit Pricing Research ---
    needs_pricing_research = (
        not competitor.pricing_url
        or pricing_page_failed
        or (pricing_page_text is not None and not _has_specific_price(pricing_page_text))
    )

    if needs_pricing_research:
        try:
            search_term = competitor.reddit_search or competitor.name
            posts = await scrape_reddit_pricing(
                competitor.name,
                search_term,
                keywords=competitor.reddit_keywords,
                include_subreddits=competitor.reddit_include_subreddits,
                exclude_subreddits=competitor.reddit_exclude_subreddits,
            )
            await _handle_reddit_pricing(
                posts,
                competitor.name,
                db,
                report,
                existing_pricing=pricing_page_text,
            )
            report.set_source_status("reddit:pricing", "ok")
        except Exception as exc:
            logger.warning("Reddit pricing error for %s: %s", competitor.name, exc)
            report.set_source_status(
                "reddit:pricing",
                "failed",
                _truncate_note(str(exc)),
            )
    else:
        report.set_source_status(
            "reddit:pricing",
            "not_needed",
            "pricing page already exposes concrete pricing",
        )

    # --- Reddit Customer/Prospect Discussion (always-on, conservative) ---
    try:
        discussion_search_term = competitor.reddit_search or competitor.name
        discussion_posts = await scrape_reddit_customer_discussions(
            competitor.name,
            discussion_search_term,
            keywords=competitor.reddit_discussion_keywords,
            include_subreddits=competitor.reddit_include_subreddits,
            exclude_subreddits=competitor.reddit_exclude_subreddits,
        )
        await _handle_reddit_discussions(
            discussion_posts,
            competitor.name,
            db,
            report,
        )
        report.set_source_status("reddit:discussion", "ok")
    except Exception as exc:
        logger.warning("Reddit discussion error for %s: %s", competitor.name, exc)
        report.set_source_status(
            "reddit:discussion",
            "failed",
            _truncate_note(str(exc)),
        )

    # --- LinkedIn Ads ---
    if competitor.linkedin_company_id and not linkedin_auth_failed:
        try:
            await _collect_linkedin_signals(
                browser=browser,
                company_id=competitor.linkedin_company_id,
                competitor_name=competitor.name,
                session_path=session_path,
                linkedin_username=linkedin_username,
                linkedin_password=linkedin_password,
                company_url=competitor.linkedin_company_url,
                db=db,
                report=report,
                debug=debug,
            )
        except AuthExpiredError as exc:
            logger.error("LinkedIn auth expired: %s", exc)
            if not linkedin_reauth_attempted and await _attempt_linkedin_reauth_interactive():
                linkedin_reauth_attempted = True
                logger.info("LinkedIn session refreshed; retrying %s once", competitor.name)
                try:
                    await random_delay(2.0, 5.0)
                    await _collect_linkedin_signals(
                        browser=browser,
                        company_id=competitor.linkedin_company_id,
                        competitor_name=competitor.name,
                        session_path=session_path,
                        linkedin_username=linkedin_username,
                        linkedin_password=linkedin_password,
                        company_url=competitor.linkedin_company_url,
                        db=db,
                        report=report,
                        debug=debug,
                    )
                except AuthExpiredError as retry_exc:
                    logger.error("LinkedIn auth still expired after re-auth: %s", retry_exc)
                    linkedin_auth_failed = True
                    _set_linkedin_auth_warning(report)
                except Exception as retry_exc:
                    logger.warning(
                        "LinkedIn retry failed for %s after re-auth: %s",
                        competitor.name,
                        retry_exc,
                    )
                    _set_linkedin_partial_failure(
                        report,
                        _truncate_note(str(retry_exc)),
                    )
            else:
                linkedin_reauth_attempted = True
                linkedin_auth_failed = True
                _set_linkedin_auth_warning(report)
        except Exception as exc:
            logger.warning("LinkedIn scrape error for %s: %s", competitor.name, exc)
            _set_linkedin_partial_failure(report, _truncate_note(str(exc)))
    elif competitor.linkedin_company_id:
        report.set_source_status(
            "linkedin:ads",
            "skipped",
            "session expired earlier in run",
        )
        report.set_source_status(
            "linkedin:organic",
            "skipped",
            "session expired earlier in run",
        )
    else:
        report.set_source_status("linkedin:ads", "not_configured")
        report.set_source_status("linkedin:organic", "not_configured")

    return report, linkedin_auth_failed, linkedin_reauth_attempted


async def _handle_reddit_pricing(
    posts: list[RedditPost],
    competitor_name: str,
    db: Database,
    report: CompetitorReport,
    existing_pricing: Optional[str],
) -> None:
    """
    Dedup Reddit posts, summarize new ones via Gemini, attach to report.
    """
    if not posts:
        return

    known_ids = db.get_known_ad_ids(competitor_name, "reddit")
    new_posts = [p for p in posts if p.post_id not in known_ids]

    if new_posts:
        summary = await summarize_pricing_research(
            competitor_name,
            new_posts,
            existing_pricing=existing_pricing,
        )
        report.pricing_research_summary = summary

    # Store all posts so they're known on future runs (dedup)
    now = _now()
    db.upsert_ads([
        AdSnapshot(
            competitor_name=competitor_name,
            platform="reddit",
            ad_id=p.post_id,
            ad_text=_build_reddit_ad_text(p),
            creative_desc=p.url,
            first_seen_at=now,
            last_seen_at=now,
        )
        for p in posts
    ])


async def _handle_reddit_discussions(
    posts: list[RedditPost],
    competitor_name: str,
    db: Database,
    report: CompetitorReport,
) -> None:
    """
    Dedup Reddit customer/prospect threads, summarize new ones, and store snapshots.
    """
    if not posts:
        return

    known_ids = db.get_known_ad_ids(competitor_name, "reddit_customer")
    new_posts = [p for p in posts if p.post_id not in known_ids]

    if new_posts:
        summary = await summarize_reddit_customer_discussions(
            competitor_name,
            new_posts,
        )
        report.reddit_discussion_summary = summary

    now = _now()
    db.upsert_ads([
        AdSnapshot(
            competitor_name=competitor_name,
            platform="reddit_customer",
            ad_id=p.post_id,
            ad_text=_build_reddit_ad_text(p),
            creative_desc=p.url,
            first_seen_at=now,
            last_seen_at=now,
        )
        for p in posts
    ])


def _build_reddit_ad_text(post: RedditPost) -> str:
    body = post.text[:700]
    comments = post.comments[:4]
    if not comments:
        return f"{post.title}\n{body}"

    comments_block = "\n".join(f"- {c}" for c in comments)[:900]
    return f"{post.title}\n{body}\n\nTop comments:\n{comments_block}"


async def _handle_website_result(
    result: WebsiteResult,
    db: Database,
    report: CompetitorReport,
) -> None:
    new_hash = Database.hash_content(result.text)
    prev = db.get_last_snapshot(result.competitor_name, result.page_type)

    changed = prev is None or prev.content_hash != new_hash

    if changed and prev is not None:
        # Ask Claude to describe what changed
        summary = await summarize_website_change(
            result.competitor_name,
            result.page_type,
            prev.content_text,
            result.text,
        )
        # For blog/careers, also surface new items explicitly
        if result.page_type == "careers" and result.new_items:
            jobs_summary = await summarize_new_jobs(result.competitor_name, result.new_items)
            if jobs_summary:
                summary = f"{summary}\n\n_New jobs:_ {jobs_summary}"
        setattr(report, f"{result.page_type}_change", summary)

    elif changed and prev is None:
        # First run — no diff available, just record
        if result.page_type == "blog" and result.new_items:
            setattr(report, "blog_change",
                    f"First scan — {len(result.new_items)} post(s) indexed")
        elif result.page_type == "careers" and result.new_items:
            setattr(report, "careers_change",
                    f"First scan — {len(result.new_items)} job(s) indexed")

    if changed:
        db.upsert_snapshot(PageSnapshot(
            competitor_name=result.competitor_name,
            page_type=result.page_type,
            content_hash=new_hash,
            content_text=result.text,
            checked_at=_now(),
        ))


async def _handle_ad_results(
    ads: list,
    platform: str,
    competitor_name: str,
    db: Database,
    report: CompetitorReport,
) -> None:
    if not ads:
        return

    known_ids = db.get_known_ad_ids(competitor_name, platform)
    new_ads = [a for a in ads if a.ad_id not in known_ids]

    if new_ads:
        ads_for_summary = [
            {
                "ad_text": a.ad_text,
                "creative_hint": getattr(a, "creative_hint", ""),
                "duration": getattr(a, "estimated_duration", None)
                            or getattr(a, "date_range", None),
            }
            for a in new_ads
        ]
        summary = await summarize_new_ads(competitor_name, platform, ads_for_summary)
        prefix = f"*{len(new_ads)} new ad(s) detected*\n"
        report.linkedin_ads_summary = prefix + summary

    # Upsert all seen ads to update last_seen_at
    now = _now()
    snapshots = [
        AdSnapshot(
            competitor_name=competitor_name,
            platform=platform,
            ad_id=a.ad_id,
            ad_text=a.ad_text,
            creative_desc=getattr(a, "creative_hint", None),
            first_seen_at=now,
            last_seen_at=now,
        )
        for a in ads
    ]
    db.upsert_ads(snapshots)


async def _handle_linkedin_organic_results(
    posts: list[LinkedInOrganicPostResult],
    competitor_name: str,
    db: Database,
    report: CompetitorReport,
) -> None:
    if not posts:
        return

    known_ids = db.get_known_ad_ids(competitor_name, "linkedin_organic")
    new_posts = [p for p in posts if p.post_id not in known_ids]

    if new_posts:
        posts_for_summary = [
            {
                "post_text": p.post_text,
                "posted_label": p.posted_label,
                "post_url": p.post_url,
            }
            for p in new_posts
        ]
        summary = await summarize_linkedin_organic_posts(competitor_name, posts_for_summary)
        report.linkedin_organic_summary = f"*{len(new_posts)} new post(s) in last 7 days*\n{summary}"

    now = _now()
    db.upsert_ads([
        AdSnapshot(
            competitor_name=competitor_name,
            platform="linkedin_organic",
            ad_id=p.post_id,
            ad_text=_build_linkedin_organic_text(p),
            creative_desc=p.post_url,
            first_seen_at=now,
            last_seen_at=now,
        )
        for p in posts
    ])


def _build_linkedin_organic_text(post: LinkedInOrganicPostResult) -> str:
    label = post.posted_label or "unknown date"
    url = post.post_url or "n/a"
    return f"[{label}] {post.post_text[:1200]}\nURL: {url}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _generate_executive_summary(reports: list[CompetitorReport]) -> Optional[str]:
    try:
        return await summarize_executive_takeaways([
            {
                "competitor_name": report.competitor_name,
                "homepage_change": report.homepage_change,
                "blog_change": report.blog_change,
                "pricing_change": report.pricing_change,
                "pricing_research_summary": report.pricing_research_summary,
                "reddit_discussion_summary": report.reddit_discussion_summary,
                "linkedin_ads_summary": report.linkedin_ads_summary,
                "linkedin_organic_summary": report.linkedin_organic_summary,
                "coverage_summary": _coverage_summary(report),
                "error": report.error,
            }
            for report in reports
        ])
    except Exception as exc:
        logger.warning("Executive summary generation failed: %s", exc)
        return None


def _append_exec_bullet(
    executive_summary: Optional[str],
    bullet: str,
    max_bullets: int = 4,
) -> str:
    """
    Append a high-priority bullet while keeping summary within max bullet count.
    """
    lines = [line.strip() for line in (executive_summary or "").splitlines() if line.strip()]
    lines.append(bullet.strip())
    # keep newest/high-priority bullets by trimming from the front when needed
    if len(lines) > max_bullets:
        lines = lines[-max_bullets:]
    return "\n".join(lines)


def _set_linkedin_auth_warning(report: CompetitorReport) -> None:
    report.set_source_status(
        "linkedin:ads",
        "failed",
        "session expired — run `python setup_auth.py`",
    )
    report.set_source_status(
        "linkedin:organic",
        "skipped",
        "session expired — organic post scan skipped",
    )


def _set_linkedin_partial_failure(report: CompetitorReport, note: str) -> None:
    for source in ("linkedin:ads", "linkedin:organic"):
        if source in report.source_status:
            continue
        report.set_source_status(source, "failed", note or "unknown scrape failure")


async def _collect_linkedin_signals(
    *,
    browser,
    company_id: str,
    competitor_name: str,
    session_path: Path,
    linkedin_username: Optional[str],
    linkedin_password: Optional[str],
    company_url: Optional[str],
    db: Database,
    report: CompetitorReport,
    debug: bool,
) -> None:
    try:
        li_ads = await scrape_linkedin_ads(
            browser,
            company_id,
            competitor_name,
            session_path,
            linkedin_username=linkedin_username,
            linkedin_password=linkedin_password,
            debug=debug,
        )
    except AuthExpiredError:
        raise
    except Exception as exc:
        report.set_source_status(
            "linkedin:ads",
            "failed",
            _truncate_note(str(exc)),
        )
    else:
        await _handle_ad_results(li_ads, "linkedin", competitor_name, db, report)
        report.set_source_status("linkedin:ads", "ok")

    # Conservative pacing between LinkedIn endpoints to reduce throttling.
    await random_delay(6.0, 12.0)

    try:
        li_posts = await scrape_linkedin_organic_posts(
            browser,
            company_id,
            competitor_name,
            session_path,
            linkedin_username=linkedin_username,
            linkedin_password=linkedin_password,
            company_url=company_url,
            max_post_age_days=7,
            debug=debug,
        )
    except AuthExpiredError:
        raise
    except Exception as exc:
        report.set_source_status(
            "linkedin:organic",
            "failed",
            _truncate_note(str(exc)),
        )
    else:
        await _handle_linkedin_organic_results(
            li_posts,
            competitor_name,
            db,
            report,
        )
        report.set_source_status("linkedin:organic", "ok")


async def _attempt_linkedin_reauth_interactive() -> bool:
    """
    In interactive terminal runs, pause and open the re-auth flow, then resume.
    Returns True when re-auth succeeded and the run can continue.
    """
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        logger.warning(
            "LinkedIn session expired in non-interactive mode; cannot pause for re-auth"
        )
        return False

    setup_script = Path(__file__).parent.parent / "setup_auth.py"
    if not setup_script.exists():
        logger.error("setup_auth.py not found at %s", setup_script)
        return False

    logger.warning(
        "LinkedIn session expired. Starting interactive re-auth flow now."
    )
    logger.warning(
        "Complete login in the opened browser, then this run will resume automatically."
    )

    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(setup_script)],
        cwd=str(setup_script.parent),
        check=False,
    )
    if result.returncode != 0:
        logger.error("LinkedIn re-auth failed with exit code %s", result.returncode)
        return False
    return True


def _coverage_summary(report: CompetitorReport) -> Optional[str]:
    issues: list[str] = []
    for source, status in report.source_status.items():
        if status not in {"failed", "skipped"}:
            continue
        label = _source_display_name(source)
        note = report.source_notes.get(source)
        detail = f": {note}" if note else ""
        issues.append(f"{label} {status}{detail}")

    if not issues:
        return None
    return "; ".join(issues[:4])


def _build_coverage_bullet(reports: list[CompetitorReport]) -> Optional[str]:
    source_counts: dict[str, int] = {}
    impacted_competitors = 0

    for report in reports:
        had_issue = False
        for source, status in report.source_status.items():
            if status not in {"failed", "skipped"}:
                continue
            had_issue = True
            bucket = source.split(":", 1)[0]
            source_counts[bucket] = source_counts.get(bucket, 0) + 1
        if had_issue:
            impacted_competitors += 1

    if not source_counts:
        return None

    parts = [
        f"{label} {count}"
        for label, count in (
            ("website", source_counts.get("website", 0)),
            ("Reddit", source_counts.get("reddit", 0)),
            ("LinkedIn", source_counts.get("linkedin", 0)),
        )
        if count
    ]
    return (
        "• Coverage note: partial scrape failures/skips affected "
        f"{impacted_competitors} competitor(s) ({', '.join(parts)}); review coverage before treating the run as complete."
    )


def _source_display_name(source: str) -> str:
    labels = {
        "linkedin:ads": "LinkedIn ads",
        "linkedin:organic": "LinkedIn organic",
        "reddit:pricing": "Reddit pricing intel",
        "reddit:discussion": "Reddit customer voice",
        "website:homepage": "Homepage",
        "website:blog": "Blog",
        "website:pricing": "Pricing page",
        "website:careers": "Careers page",
    }
    return labels.get(source, source)


def _truncate_note(note: str, max_chars: int = 140) -> str:
    compact = re.sub(r"\s+", " ", note).strip()
    if len(compact) <= max_chars:
        return compact
    head = compact[:max_chars].rstrip()
    split = head.rfind(" ")
    if split >= 60:
        head = head[:split]
    return head.rstrip(" ,;:.") + "…"
