"""
SQLite-based progress tracking for resumable processing.

Tracks the processing state of each data file (WET or ARC) so the
application can be stopped and resumed at any time without losing progress.

Supports per-crawl tracking for processing multiple crawls.
"""

import logging
import sqlite3
from datetime import datetime, timezone

from config import DB_PATH

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    SQLite-based progress tracker.

    Each data file is tracked with a status:
    - pending: not yet processed
    - processing: currently being processed
    - completed: successfully processed
    - failed: processing failed (will be retried on next run)

    Files are scoped by crawl_id so multiple crawls can be
    tracked independently in the same database.
    """

    def __init__(self):
        self.db_path = str(DB_PATH)
        self._init_db()
        self._recover_stuck()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a new database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS processing_state (
                    file_path TEXT PRIMARY KEY,
                    crawl_id TEXT,
                    status TEXT DEFAULT 'pending',
                    records_processed INTEGER DEFAULT 0,
                    matches_found INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TEXT,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_status
                    ON processing_state(status);

                CREATE INDEX IF NOT EXISTS idx_crawl_status
                    ON processing_state(crawl_id, status);
            """)
            conn.commit()
        finally:
            conn.close()

    def _recover_stuck(self):
        """Reset any files stuck in 'processing' state (from a crash)."""
        conn = self._get_conn()
        try:
            cursor = conn.execute(
                "UPDATE processing_state SET status = 'pending' "
                "WHERE status = 'processing'"
            )
            if cursor.rowcount > 0:
                logger.info(
                    f"Recovered {cursor.rowcount} files stuck in 'processing' state"
                )
            conn.commit()
        finally:
            conn.close()

    def initialize_paths(self, file_paths: list[str], crawl_id: str = ""):
        """
        Populate the database with file paths for a crawl.
        Skips paths that already exist (idempotent).
        """
        conn = self._get_conn()
        try:
            conn.executemany(
                "INSERT OR IGNORE INTO processing_state (file_path, crawl_id) "
                "VALUES (?, ?)",
                [(p, crawl_id) for p in file_paths],
            )
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM processing_state WHERE crawl_id = ?",
                (crawl_id,)
            ).fetchone()[0]
            logger.info(f"Progress database has {count} tracked files for {crawl_id}")
        finally:
            conn.close()

    def get_next_pending(self, crawl_id: str = "") -> str | None:
        """Get the next file with 'pending' status for a given crawl."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT file_path FROM processing_state "
                "WHERE status = 'pending' AND crawl_id = ? LIMIT 1",
                (crawl_id,)
            ).fetchone()
            return row["file_path"] if row else None
        finally:
            conn.close()

    def mark_processing(self, file_path: str):
        """Mark a file as currently being processed."""
        conn = self._get_conn()
        try:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE processing_state SET status = 'processing', "
                "started_at = ? WHERE file_path = ?",
                (now, file_path),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_completed(
        self, file_path: str, records_processed: int, matches_found: int
    ):
        """Mark a file as successfully processed."""
        conn = self._get_conn()
        try:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE processing_state SET status = 'completed', "
                "records_processed = ?, matches_found = ?, "
                "completed_at = ? WHERE file_path = ?",
                (records_processed, matches_found, now, file_path),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_failed(self, file_path: str, error: str):
        """Mark a file as failed."""
        conn = self._get_conn()
        try:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE processing_state SET status = 'failed', "
                "error_message = ?, completed_at = ? WHERE file_path = ?",
                (error, now, file_path),
            )
            conn.commit()
        finally:
            conn.close()

    def get_summary(self, crawl_id: str | None = None) -> dict:
        """
        Get a progress summary.

        Args:
            crawl_id: If provided, only show stats for this crawl.
                      If None, show overall stats.
        """
        conn = self._get_conn()
        try:
            if crawl_id is not None:
                where = "WHERE crawl_id = ?"
                params = (crawl_id,)
                completed_where = "WHERE status = 'completed' AND crawl_id = ?"
            else:
                where = ""
                params = ()
                completed_where = "WHERE status = 'completed'"

            total = conn.execute(
                f"SELECT COUNT(*) FROM processing_state {where}",
                params
            ).fetchone()[0]

            completed = conn.execute(
                f"SELECT COUNT(*) FROM processing_state {where.replace('WHERE', 'WHERE status = %s AND' % repr('completed') if where else 'WHERE status = %s' % repr('completed'))}".replace('%s', '?') if False else
                f"SELECT COUNT(*) FROM processing_state "
                f"{'WHERE status = ? AND crawl_id = ?' if crawl_id is not None else 'WHERE status = ?'}",
                ('completed', crawl_id) if crawl_id is not None else ('completed',)
            ).fetchone()[0]

            failed = conn.execute(
                f"SELECT COUNT(*) FROM processing_state "
                f"{'WHERE status = ? AND crawl_id = ?' if crawl_id is not None else 'WHERE status = ?'}",
                ('failed', crawl_id) if crawl_id is not None else ('failed',)
            ).fetchone()[0]

            pending = conn.execute(
                f"SELECT COUNT(*) FROM processing_state "
                f"{'WHERE status = ? AND crawl_id = ?' if crawl_id is not None else 'WHERE status = ?'}",
                ('pending', crawl_id) if crawl_id is not None else ('pending',)
            ).fetchone()[0]

            total_records = conn.execute(
                f"SELECT COALESCE(SUM(records_processed), 0) "
                f"FROM processing_state {completed_where}",
                (crawl_id,) if crawl_id is not None else ()
            ).fetchone()[0]

            total_matches = conn.execute(
                f"SELECT COALESCE(SUM(matches_found), 0) "
                f"FROM processing_state {completed_where}",
                (crawl_id,) if crawl_id is not None else ()
            ).fetchone()[0]

            return {
                "total_files": total,
                "completed": completed,
                "failed": failed,
                "pending": pending,
                "total_records": total_records,
                "total_matches": total_matches,
                "progress_pct": (completed / total * 100) if total > 0 else 0,
            }
        finally:
            conn.close()

    def get_per_crawl_summary(self) -> list[dict]:
        """Get a progress summary broken down by crawl_id."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT crawl_id, "
                "COUNT(*) as total, "
                "SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed, "
                "COALESCE(SUM(matches_found), 0) as matches "
                "FROM processing_state "
                "GROUP BY crawl_id "
                "ORDER BY crawl_id"
            ).fetchall()

            return [
                {
                    "crawl_id": row["crawl_id"],
                    "total": row["total"],
                    "completed": row["completed"],
                    "matches": row["matches"],
                }
                for row in rows
            ]
        finally:
            conn.close()
