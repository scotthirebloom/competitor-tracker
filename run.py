"""
Competitive Intelligence Tracker

Usage:
    python run.py                            # Run all competitors once
    python run.py --competitor "Acme Support Co"    # Run a single competitor (for testing)
    python run.py --daemon                   # Run every Sunday at 08:00

Setup (first time):
    1. pip install -r requirements.txt && playwright install chromium
    2. Add SLACK_WEBHOOK_URL to .env
    3. Edit competitors.yaml with your competitors
    4. python setup_auth.py    # Authenticate with LinkedIn (one-time)
    5. python run.py           # Run it!
"""
import argparse
import asyncio
import fcntl
import logging
import time
from contextlib import contextmanager
from pathlib import Path

import schedule

from tracker.config import load_config
from tracker.orchestrator import run_linkedin_recovery, run_weekly

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
# Avoid logging full request URLs (can expose webhook secrets in logs).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "competitors.yaml"
LOCK_PATH = Path(__file__).parent / "data" / "run.lock"


def _run_once(
    competitor_name: str | None = None,
    debug: bool = False,
    linkedin_recovery: bool = False,
) -> None:
    config = load_config(CONFIG_PATH)
    config.debug = debug
    if competitor_name:
        matches = [c for c in config.competitors
                   if c.name.lower() == competitor_name.lower()]
        if not matches:
            available = ", ".join(c.name for c in config.competitors)
            raise SystemExit(
                f"Competitor '{competitor_name}' not found.\n"
                f"Available: {available}"
            )
        config.competitors = matches
        logger.info("Test mode — running for: %s", matches[0].name)
    if linkedin_recovery:
        asyncio.run(run_linkedin_recovery(config))
    else:
        asyncio.run(run_weekly(config))


@contextmanager
def _single_run_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("w")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                "competitor_tracker is already running; wait for the current run to finish."
            ) from None
        handle.write(str(time.time()))
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        handle.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Competitive Intelligence Tracker")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as a weekly daemon (every Sunday at 08:00 local time)",
    )
    parser.add_argument(
        "--competitor",
        metavar="NAME",
        help='Run a single competitor by name, e.g. --competitor "Acme Support Co"',
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Open a visible browser window and save screenshots to data/debug/",
    )
    parser.add_argument(
        "--linkedin-recovery",
        action="store_true",
        help=(
            "Run LinkedIn-only recovery mode (backfill LinkedIn ads/posts and resend "
            "executive summary without full scrape)."
        ),
    )
    args = parser.parse_args()

    with _single_run_lock():
        if args.daemon:
            if args.linkedin_recovery:
                raise SystemExit("--linkedin-recovery cannot be used with --daemon")
            logger.info("Daemon mode — will run every Sunday at 08:00")
            schedule.every().sunday.at("08:00").do(_run_once)
            while True:
                schedule.run_pending()
                time.sleep(60)
        else:
            run_mode = "LinkedIn recovery" if args.linkedin_recovery else "full run"
            logger.info(
                "Running once (%s)%s",
                run_mode,
                " (debug mode)" if args.debug else "",
            )
            _run_once(
                args.competitor,
                debug=args.debug,
                linkedin_recovery=args.linkedin_recovery,
            )


if __name__ == "__main__":
    main()
