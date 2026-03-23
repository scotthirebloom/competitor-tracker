import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class PageSnapshot:
    competitor_name: str
    page_type: str
    content_hash: str
    content_text: str
    checked_at: str


@dataclass
class AdSnapshot:
    competitor_name: str
    platform: str
    ad_id: str
    ad_text: str
    creative_desc: Optional[str]
    first_seen_at: str
    last_seen_at: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS page_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    competitor_name TEXT    NOT NULL,
    page_type       TEXT    NOT NULL,
    content_hash    TEXT    NOT NULL,
    content_text    TEXT    NOT NULL,
    checked_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS ad_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    competitor_name TEXT    NOT NULL,
    platform        TEXT    NOT NULL,
    ad_id           TEXT    NOT NULL,
    ad_text         TEXT    NOT NULL,
    creative_desc   TEXT,
    first_seen_at   TEXT    NOT NULL,
    last_seen_at    TEXT    NOT NULL,
    UNIQUE(competitor_name, platform, ad_id)
);

CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    competitor_name TEXT    NOT NULL,
    run_at          TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_page_snapshots_lookup
    ON page_snapshots (competitor_name, page_type, id DESC);
"""


class Database:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def get_last_snapshot(
        self, competitor_name: str, page_type: str
    ) -> Optional[PageSnapshot]:
        row = self._conn.execute(
            """
            SELECT * FROM page_snapshots
            WHERE competitor_name = ? AND page_type = ?
            ORDER BY id DESC LIMIT 1
            """,
            (competitor_name, page_type),
        ).fetchone()
        if row is None:
            return None
        return PageSnapshot(
            competitor_name=row["competitor_name"],
            page_type=row["page_type"],
            content_hash=row["content_hash"],
            content_text=row["content_text"],
            checked_at=row["checked_at"],
        )

    def upsert_snapshot(self, snapshot: PageSnapshot) -> None:
        self._conn.execute(
            """
            INSERT INTO page_snapshots
                (competitor_name, page_type, content_hash, content_text, checked_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                snapshot.competitor_name,
                snapshot.page_type,
                snapshot.content_hash,
                snapshot.content_text,
                snapshot.checked_at,
            ),
        )
        self._conn.commit()

    def get_known_ad_ids(self, competitor_name: str, platform: str) -> set[str]:
        rows = self._conn.execute(
            "SELECT ad_id FROM ad_snapshots WHERE competitor_name = ? AND platform = ?",
            (competitor_name, platform),
        ).fetchall()
        return {row["ad_id"] for row in rows}

    def upsert_ads(self, ads: list[AdSnapshot]) -> None:
        now = _now()
        for ad in ads:
            self._conn.execute(
                """
                INSERT INTO ad_snapshots
                    (competitor_name, platform, ad_id, ad_text, creative_desc,
                     first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(competitor_name, platform, ad_id) DO UPDATE SET
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    ad.competitor_name,
                    ad.platform,
                    ad.ad_id,
                    ad.ad_text,
                    ad.creative_desc,
                    ad.first_seen_at,
                    ad.last_seen_at,
                ),
            )
        self._conn.commit()

    def log_run(
        self,
        competitor_name: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO run_log (competitor_name, run_at, status, error_message) VALUES (?, ?, ?, ?)",
            (competitor_name, _now(), status, error_message),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def hash_content(text: str) -> str:
        normalized = " ".join(text.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
