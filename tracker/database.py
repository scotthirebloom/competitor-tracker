import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
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

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT    NOT NULL
);
"""

# ── Incremental migrations ────────────────────────────────────────────
# Each tuple: (version, sql).  Applied in order; existing DBs skip
# already-applied versions.  New DBs get _SCHEMA first (which creates
# all original tables), then migrations add everything else.

_MIGRATIONS: list[tuple[int, str]] = [
    # 1 — missing index on ad_snapshots for get_known_ad_ids()
    (1, """
        CREATE INDEX IF NOT EXISTS idx_ad_snapshots_lookup
            ON ad_snapshots (competitor_name, platform);
    """),

    # 2 — top-level runs table
    (2, """
        CREATE TABLE IF NOT EXISTS runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at        TEXT    NOT NULL,
            finished_at       TEXT,
            status            TEXT    NOT NULL DEFAULT 'running',
            competitor_count  INTEGER,
            duration_seconds  REAL,
            executive_summary TEXT
        );
    """),

    # 3 — link run_log rows to a run
    (3, "ALTER TABLE run_log ADD COLUMN run_id INTEGER REFERENCES runs(id);"),

    # 4 — summaries table for persisting Gemini output
    (4, """
        CREATE TABLE IF NOT EXISTS summaries (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL REFERENCES runs(id),
            competitor_name TEXT    NOT NULL,
            summary_type    TEXT    NOT NULL,
            summary_text    TEXT    NOT NULL,
            created_at      TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_summaries_lookup
            ON summaries (competitor_name, summary_type, created_at DESC);
    """),

    # 5 — enrich run_log with per-competitor metrics
    (5, "ALTER TABLE run_log ADD COLUMN duration_seconds REAL;"),
    (6, "ALTER TABLE run_log ADD COLUMN new_ads_count INTEGER DEFAULT 0;"),
    (7, "ALTER TABLE run_log ADD COLUMN new_posts_count INTEGER DEFAULT 0;"),
    (8, "ALTER TABLE run_log ADD COLUMN pages_changed_count INTEGER DEFAULT 0;"),
    (9, "ALTER TABLE run_log ADD COLUMN sources_json TEXT;"),

    # 10 — API Direct monthly usage tracking (free-tier budget enforcement)
    (10, """
        CREATE TABLE IF NOT EXISTS apidirect_usage (
            endpoint       TEXT NOT NULL,
            month          TEXT NOT NULL,
            request_count  INTEGER NOT NULL DEFAULT 0,
            UNIQUE(endpoint, month)
        );
    """),
]


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
        self._apply_migrations()

    # ── Migration engine ──────────────────────────────────────────────

    def _apply_migrations(self) -> None:
        row = self._conn.execute(
            "SELECT MAX(version) AS v FROM schema_migrations"
        ).fetchone()
        current = row["v"] or 0

        for version, sql in _MIGRATIONS:
            if version <= current:
                continue
            self._conn.executescript(sql)
            self._conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, _now()),
            )
            self._conn.commit()

    # ── Page snapshots ────────────────────────────────────────────────

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

    # ── Ad snapshots ──────────────────────────────────────────────────

    def get_known_ad_ids(self, competitor_name: str, platform: str) -> set[str]:
        rows = self._conn.execute(
            "SELECT ad_id FROM ad_snapshots WHERE competitor_name = ? AND platform = ?",
            (competitor_name, platform),
        ).fetchall()
        return {row["ad_id"] for row in rows}

    def upsert_ads(self, ads: list[AdSnapshot]) -> None:
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

    # ── Runs ──────────────────────────────────────────────────────────

    def start_run(self, competitor_count: int) -> int:
        cur = self._conn.execute(
            "INSERT INTO runs (started_at, competitor_count) VALUES (?, ?)",
            (_now(), competitor_count),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def finish_run(
        self,
        run_id: int,
        status: str,
        executive_summary: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> None:
        self._conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, status = ?, executive_summary = ?,
                duration_seconds = ?
            WHERE id = ?
            """,
            (_now(), status, executive_summary, duration_seconds, run_id),
        )
        self._conn.commit()

    # ── Run log (per-competitor) ──────────────────────────────────────

    def log_run(
        self,
        competitor_name: str,
        status: str,
        error_message: Optional[str] = None,
        *,
        run_id: Optional[int] = None,
        duration_seconds: Optional[float] = None,
        new_ads_count: int = 0,
        new_posts_count: int = 0,
        pages_changed_count: int = 0,
        sources_json: Optional[str] = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO run_log
                (competitor_name, run_at, status, error_message,
                 run_id, duration_seconds, new_ads_count, new_posts_count,
                 pages_changed_count, sources_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                competitor_name, _now(), status, error_message,
                run_id, duration_seconds, new_ads_count, new_posts_count,
                pages_changed_count, sources_json,
            ),
        )
        self._conn.commit()

    # ── Summaries ─────────────────────────────────────────────────────

    def save_summary(
        self,
        run_id: int,
        competitor_name: str,
        summary_type: str,
        summary_text: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO summaries
                (run_id, competitor_name, summary_type, summary_text, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, competitor_name, summary_type, summary_text, _now()),
        )
        self._conn.commit()

    def get_summaries_for_run(self, run_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM summaries WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_summary(
        self, competitor_name: str, summary_type: str
    ) -> Optional[dict]:
        row = self._conn.execute(
            """
            SELECT * FROM summaries
            WHERE competitor_name = ? AND summary_type = ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (competitor_name, summary_type),
        ).fetchone()
        return dict(row) if row else None

    def get_summary_history(
        self, competitor_name: str, summary_type: str, limit: int = 10
    ) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT * FROM summaries
            WHERE competitor_name = ? AND summary_type = ?
            ORDER BY created_at DESC LIMIT ?
            """,
            (competitor_name, summary_type, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── API Direct usage tracking ──────────────────────────────────────

    def get_apidirect_usage(self, endpoint: str) -> int:
        """Return the number of API Direct requests made this month for an endpoint."""
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        row = self._conn.execute(
            "SELECT request_count FROM apidirect_usage WHERE endpoint = ? AND month = ?",
            (endpoint, month),
        ).fetchone()
        return row["request_count"] if row else 0

    def increment_apidirect_usage(self, endpoint: str) -> None:
        """Increment the monthly request counter for an API Direct endpoint."""
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        self._conn.execute(
            """
            INSERT INTO apidirect_usage (endpoint, month, request_count)
            VALUES (?, ?, 1)
            ON CONFLICT(endpoint, month) DO UPDATE SET
                request_count = request_count + 1
            """,
            (endpoint, month),
        )
        self._conn.commit()

    def get_all_apidirect_usage(self) -> dict[str, int]:
        """Return all endpoint usage counts for the current month."""
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        rows = self._conn.execute(
            "SELECT endpoint, request_count FROM apidirect_usage WHERE month = ?",
            (month,),
        ).fetchall()
        return {row["endpoint"]: row["request_count"] for row in rows}

    # ── Retention / pruning ───────────────────────────────────────────

    def prune_old_snapshots(self, days: int = 180) -> int:
        threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = self._conn.execute(
            """
            DELETE FROM page_snapshots
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY competitor_name, page_type
                        ORDER BY id DESC
                    ) AS rn
                    FROM page_snapshots
                ) WHERE rn = 1
            )
            AND checked_at < ?
            """,
            (threshold,),
        )
        self._conn.commit()
        return cur.rowcount

    def prune_old_ads(self, days: int = 365) -> int:
        threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = self._conn.execute(
            "DELETE FROM ad_snapshots WHERE last_seen_at < ?",
            (threshold,),
        )
        self._conn.commit()
        return cur.rowcount

    # ── Trend queries ─────────────────────────────────────────────────

    def get_ad_trend(
        self,
        competitor_name: Optional[str] = None,
        platform: Optional[str] = None,
        weeks: int = 12,
    ) -> list[dict]:
        threshold = (
            datetime.now(timezone.utc) - timedelta(weeks=weeks)
        ).isoformat()
        conditions = ["first_seen_at >= ?"]
        params: list = [threshold]
        if competitor_name:
            conditions.append("competitor_name = ?")
            params.append(competitor_name)
        if platform:
            conditions.append("platform = ?")
            params.append(platform)
        where = " AND ".join(conditions)
        rows = self._conn.execute(
            f"""
            SELECT strftime('%Y-W%W', first_seen_at) AS week,
                   competitor_name,
                   platform,
                   COUNT(*) AS new_count
            FROM ad_snapshots
            WHERE {where}
            GROUP BY week, competitor_name, platform
            ORDER BY week
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def get_page_change_trend(
        self,
        competitor_name: Optional[str] = None,
        weeks: int = 12,
    ) -> list[dict]:
        threshold = (
            datetime.now(timezone.utc) - timedelta(weeks=weeks)
        ).isoformat()
        conditions = ["checked_at >= ?"]
        params: list = [threshold]
        if competitor_name:
            conditions.append("competitor_name = ?")
            params.append(competitor_name)
        where = " AND ".join(conditions)
        rows = self._conn.execute(
            f"""
            SELECT strftime('%Y-W%W', checked_at) AS week,
                   competitor_name,
                   page_type,
                   COUNT(*) AS change_count
            FROM page_snapshots
            WHERE {where}
            GROUP BY week, competitor_name, page_type
            ORDER BY week
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def get_run_history(self, limit: int = 20) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT r.*,
                   (SELECT COUNT(*) FROM run_log l
                    WHERE l.run_id = r.id AND l.status = 'success') AS success_count,
                   (SELECT COUNT(*) FROM run_log l
                    WHERE l.run_id = r.id AND l.status = 'error') AS error_count
            FROM runs r
            ORDER BY r.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Housekeeping ──────────────────────────────────────────────────

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def hash_content(text: str) -> str:
        normalized = " ".join(text.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
