import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

# Load .env from the project root (one level up from this file's package)
_ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(_ENV_PATH)

_PROJECT_ROOT = Path(__file__).parent.parent


@dataclass
class CompetitorConfig:
    name: str
    website: str
    homepage_url: Optional[str] = None
    blog_url: Optional[str] = None
    pricing_url: Optional[str] = None
    careers_url: Optional[str] = None
    linkedin_company_id: Optional[str] = None
    linkedin_company_url: Optional[str] = None
    reddit_search: Optional[str] = None  # override search term for Reddit pricing research
    reddit_keywords: list[str] = field(default_factory=list)
    reddit_discussion_keywords: list[str] = field(default_factory=list)
    reddit_include_subreddits: list[str] = field(default_factory=list)
    reddit_exclude_subreddits: list[str] = field(default_factory=list)


@dataclass
class AppConfig:
    gemini_api_key: str
    slack_webhook_url: str
    competitors: list[CompetitorConfig]
    db_path: Path
    session_path: Path
    linkedin_username: Optional[str] = None
    linkedin_password: Optional[str] = None
    debug: bool = False


def load_config(yaml_path: Path) -> AppConfig:
    anthropic_key = os.getenv("GEMINI_API_KEY")
    if not anthropic_key:
        raise ValueError("GEMINI_API_KEY is not set in .env")

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_url:
        raise ValueError("SLACK_WEBHOOK_URL is not set in .env")

    linkedin_username = os.getenv("LINKEDIN_USERNAME")
    linkedin_password = os.getenv("LINKEDIN_PASSWORD")
    if (linkedin_username and not linkedin_password) or (linkedin_password and not linkedin_username):
        raise ValueError(
            "Set both LINKEDIN_USERNAME and LINKEDIN_PASSWORD in .env, or leave both unset"
        )

    with open(yaml_path, "r") as f:
        raw = yaml.safe_load(f)

    competitors = _load_competitors(raw.get("competitors", []))
    if not competitors:
        raise ValueError(f"No competitors defined in {yaml_path}")

    return AppConfig(
        gemini_api_key=anthropic_key,
        slack_webhook_url=slack_url,
        competitors=competitors,
        db_path=_PROJECT_ROOT / "data" / "state.db",
        session_path=_PROJECT_ROOT / "data" / "linkedin_session.json",
        linkedin_username=linkedin_username,
        linkedin_password=linkedin_password,
    )


def _load_competitors(raw: list[dict]) -> list[CompetitorConfig]:
    result = []
    for entry in raw:
        if not entry.get("name") or not entry.get("website"):
            continue
        result.append(CompetitorConfig(
            name=entry["name"],
            website=entry["website"],
            homepage_url=entry.get("homepage_url"),
            blog_url=entry.get("blog_url"),
            pricing_url=entry.get("pricing_url"),
            careers_url=entry.get("careers_url"),
            linkedin_company_id=str(entry["linkedin_company_id"])
                if entry.get("linkedin_company_id") else None,
            linkedin_company_url=entry.get("linkedin_company_url"),
            reddit_search=entry.get("reddit_search"),
            reddit_keywords=_as_str_list(entry.get("reddit_keywords")),
            reddit_discussion_keywords=_as_str_list(entry.get("reddit_discussion_keywords")),
            reddit_include_subreddits=_as_str_list(entry.get("reddit_include_subreddits")),
            reddit_exclude_subreddits=_as_str_list(entry.get("reddit_exclude_subreddits")),
        ))
    return result


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
