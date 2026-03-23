# Competitor Tracker

An automated competitive intelligence agent that runs weekly, scrapes your competitors across multiple channels, summarizes findings with AI, and delivers a digest to Slack.

Built with Python, Playwright, Google Gemini, and SQLite. Runs on a schedule via macOS LaunchAgent (or the built-in daemon mode).

---

## What It Does

Each weekly run:

1. **Scrapes competitor websites** — homepage, blog/news, pricing, and careers pages. Detects meaningful content changes using SHA-256 hashing.
2. **Scrapes LinkedIn** — new ads from the LinkedIn Ad Library and recent organic company posts.
3. **Scrapes Reddit** — pricing discussions and customer/prospect sentiment threads.
4. **Summarizes with Gemini** — each data source gets an AI summary; the run closes with an executive-level takeaways digest.
5. **Posts to Slack** — a structured Slack digest with per-competitor sections and an executive summary.

All raw data is stored in SQLite for deduplication and change detection across runs.

---

## Architecture

```
competitors.yaml
      │
      ▼
  orchestrator.py
      │
      ├── scrapers/website.py    ──► homepage, blog, pricing, careers
      ├── scrapers/linkedin_ads.py ─► LinkedIn Ad Library + organic posts
      └── scrapers/reddit_intel.py ─► Reddit pricing + discussion search
      │
      ├── database.py            ──► SQLite (change detection + dedup)
      ├── summarizer.py          ──► Gemini API (per-source summaries)
      └── reporter.py            ──► Slack Block Kit digest
```

---

## Features

- **Change detection** — only reports what's new since the last run, not the full page content every time
- **LinkedIn Ad Library scraping** — no paid API required; uses Playwright with a saved session
- **LinkedIn organic posts** — recent company posts scraped from the company page feed
- **Reddit intelligence** — pricing discussions and customer voice threads, with per-competitor keyword tuning and subreddit filtering
- **AI summaries** — Gemini Flash for per-source summaries; Gemini Pro (with extended thinking) for the executive digest
- **Error isolation** — a failure on one competitor or one source doesn't abort the rest of the run
- **Coverage gaps** — when a source fails or is skipped, that gap is called out in the executive summary
- **Slack chunking** — large digests are split automatically to stay within Slack's block limits

---

## Prerequisites

- Python 3.11+
- A [Google Gemini API key](https://aistudio.google.com/app/apikey)
- A [Slack incoming webhook URL](https://api.slack.com/messaging/webhooks)
- A LinkedIn account (for LinkedIn scraping — a dedicated dummy account is recommended)

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/your-username/competitor-tracker.git
cd competitor-tracker
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/playwright install chromium
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in your API keys
```

Required variables:

| Variable | Description |
|---|---|
| `GEMINI_API_KEY` | Google Gemini API key |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `LINKEDIN_USERNAME` | *(optional)* LinkedIn email for auto re-login |
| `LINKEDIN_PASSWORD` | *(optional)* LinkedIn password for auto re-login |

### 3. Add your competitors

Edit `competitors.yaml`. Each entry supports:

| Field | Required | Description |
|---|---|---|
| `name` | yes | Display name used in reports |
| `website` | yes | Canonical domain |
| `homepage_url` | yes | Homepage scrape target |
| `blog_url` | yes | Blog/news scrape target |
| `pricing_url` | no | Pricing page (`null` to skip) |
| `careers_url` | no | Careers page (`null` to skip) |
| `linkedin_company_id` | no | Numeric LinkedIn company ID (`null` to skip LinkedIn) |
| `linkedin_company_url` | no | LinkedIn company page URL |
| `reddit_search` | no | Base Reddit search phrase (defaults to competitor name) |
| `reddit_keywords` | no | Extra terms paired with `reddit_search` |
| `reddit_discussion_keywords` | no | Terms for customer/prospect discussion scan |
| `reddit_include_subreddits` | no | Subreddit allow-list (empty = all) |
| `reddit_exclude_subreddits` | no | Subreddit block-list |

See `competitors.yaml` for annotated examples.

### 4. Set up LinkedIn authentication

LinkedIn scraping requires a saved browser session. Run the interactive setup once:

```bash
./venv/bin/python setup_auth.py
```

This opens a Chromium window. Log in to LinkedIn, then press Enter in the terminal. The session is saved to `data/linkedin_session.json` and reused on subsequent runs.

If you set `LINKEDIN_USERNAME` and `LINKEDIN_PASSWORD` in `.env`, the tracker will attempt automatic re-login when the session expires.

---

## Running

**Full run (all competitors):**

```bash
./venv/bin/python run.py
```

**Single competitor (for testing):**

```bash
./venv/bin/python run.py --competitor "Acme Support Co"
```

**Debug mode** (visible browser window + screenshots saved to `data/debug/`):

```bash
./venv/bin/python run.py --competitor "Acme Support Co" --debug
```

**LinkedIn-only recovery** (re-collect LinkedIn data after a skipped run):

```bash
./venv/bin/python run.py --linkedin-recovery --debug
```

**Built-in daemon mode** (runs every Sunday at 08:00 local time):

```bash
./venv/bin/python run.py --daemon
```

---

## Scheduling (macOS LaunchAgent)

For reliable weekly scheduling on macOS, use a LaunchAgent instead of cron. Create a wrapper script:

```bash
#!/bin/bash
cd /path/to/competitor-tracker
./venv/bin/python run.py >> data/launchd.log 2>> data/launchd.err.log
```

And a plist at `~/Library/LaunchAgents/com.competitor-tracker.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.competitor-tracker</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>/path/to/your/wrapper.sh</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key><integer>1</integer>  <!-- Monday -->
    <key>Hour</key><integer>11</integer>
    <key>Minute</key><integer>0</integer>
  </dict>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
```

Load it:

```bash
launchctl load ~/Library/LaunchAgents/com.competitor-tracker.plist
```

Force a run immediately:

```bash
launchctl start com.competitor-tracker
```

---

## Storage

All data is stored in `data/state.db` (SQLite, gitignored).

| Table | Contents |
|---|---|
| `page_snapshots` | Versioned website content with SHA-256 hashes |
| `ad_snapshots` | Deduped LinkedIn ads, organic posts, and Reddit intel |
| `run_log` | Per-competitor run success/error history |

Quick inspection:

```bash
sqlite3 data/state.db ".tables"
sqlite3 data/state.db "SELECT competitor_name, page_type, checked_at FROM page_snapshots ORDER BY id DESC LIMIT 20;"
```

---

## Troubleshooting

**LinkedIn session expired:**
Re-run `setup_auth.py` to generate a fresh session, or set `LINKEDIN_USERNAME`/`LINKEDIN_PASSWORD` for automatic re-login.

**LinkedIn HTTP 999 (throttled):**
Run in `--debug` mode (non-headless). Avoid running too frequently. The tracker will continue other sources and report LinkedIn as a coverage gap.

**Reddit empty results:**
Widen `reddit_keywords` or remove subreddit filters in `competitors.yaml`. The scraper falls back to `old.reddit.com` automatically on transient failures.

**No changes detected:**
This is expected when content hasn't changed since the last run. Verify by querying recent rows:

```bash
sqlite3 data/state.db "SELECT competitor_name, page_type, checked_at FROM page_snapshots ORDER BY id DESC LIMIT 10;"
```

---

## Running Tests

```bash
./venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

---

## License

MIT — see [LICENSE](LICENSE).
