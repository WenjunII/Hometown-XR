"""
SQLite-based progress tracking for resumable processing.

Tracks the processing state of each data file (WET or ARC) so the
application can be stopped and resumed at any time without losing progress.

Supports per-crawl tracking for processing multiple crawls.
"""

import logging
import sqlite3
from datetime import datetime, timezone, timedelta

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
        """Reset any files stuck in 'processing' state for more than 1 hour (from a crash)."""
        conn = self._get_conn()
        try:
            # Only recover files that started processing more than 1 hour ago
            # This prevents status checks from interfering with an active run
            an_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            
            cursor = conn.execute(
                "UPDATE processing_state SET status = 'pending', started_at = NULL "
                "WHERE status = 'processing' AND started_at < ?",
                (an_hour_ago,)
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

    def get_batch_pending(self, crawl_id: str = "", limit: int = 1000) -> list[str]:
        """Get a batch of files with 'pending' status for a given crawl."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT file_path FROM processing_state "
                "WHERE status = 'pending' AND crawl_id = ? LIMIT ?",
                (crawl_id, limit)
            ).fetchall()
            return [row["file_path"] for row in rows]
        finally:
            conn.close()

    def get_next_pending(self, crawl_id: str = "") -> str | None:
        """Get the next file with 'pending' status for a given crawl."""
        batch = self.get_batch_pending(crawl_id, limit=1)
        return batch[0] if batch else None

    def mark_batch_processing(self, file_paths: list[str]):
        """Mark multiple files as currently being processed in a single transaction."""
        if not file_paths:
            return
        conn = self._get_conn()
        try:
            now = datetime.now(timezone.utc).isoformat()
            # SQLite handles about 999 parameters per query usually, 
            # so we use executemany for safety and speed.
            conn.executemany(
                "UPDATE processing_state SET status = 'processing', "
                "started_at = ? WHERE file_path = ?",
                [(now, p) for p in file_paths],
            )
            conn.commit()
        finally:
            conn.close()

    def mark_processing(self, file_path: str):
        """Mark a file as currently being processed."""
        self.mark_batch_processing([file_path])

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
                # Stats for a specific crawl
                query = """
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                        COALESCE(SUM(records_processed), 0) as total_records,
                        COALESCE(SUM(matches_found), 0) as total_matches
                    FROM processing_state
                    WHERE crawl_id = ?
                """
                row = conn.execute(query, (crawl_id,)).fetchone()
            else:
                # Overall stats
                query = """
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                        COALESCE(SUM(records_processed), 0) as total_records,
                        COALESCE(SUM(matches_found), 0) as total_matches
                    FROM processing_state
                """
                row = conn.execute(query).fetchone()

            if not row or row["total"] == 0:
                return {
                    "total_files": 0, "completed": 0, "failed": 0, "pending": 0,
                    "processing": 0, "total_records": 0, "total_matches": 0, "progress_pct": 0
                }

            return {
                "total_files": row["total"],
                "completed": row["completed"],
                "failed": row["failed"],
                "pending": row["pending"],
                "processing": row["processing"],
                "total_records": row["total_records"],
                "total_matches": row["total_matches"],
                "progress_pct": (row["completed"] / row["total"] * 100) if row["total"] > 0 else 0,
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
