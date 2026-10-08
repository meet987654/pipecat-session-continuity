import asyncio
import logging
import sqlite3
import threading
import time
from typing import Optional

from .base import BaseStorage

logger = logging.getLogger(__name__)


class SQLiteStorage(BaseStorage):
    """
    SQLite-backed persistent storage for session continuity.
    Default backend for local development, requiring zero external infrastructure.
    Supports file-based persistence as well as ':memory:' for testing.
    """

    def __init__(self, db_path: str = "pipecat_sessions.db"):
        self.db_path = db_path
        self.is_memory = db_path == ":memory:" or db_path.startswith("file::memory:")
        self._lock = threading.Lock()
        self._mem_conn: Optional[sqlite3.Connection] = None

        if self.is_memory:
            # Persistent connection for in-memory database to prevent state loss across calls
            self._mem_conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=10.0,
            )

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self.is_memory and self._mem_conn:
            return self._mem_conn
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.OperationalError:
            pass
        return conn

    def _init_db(self):
        try:
            with self._lock if self.is_memory else threading.Lock():
                conn = self._get_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS sessions (
                            session_id TEXT PRIMARY KEY,
                            json_data TEXT,
                            updated_at REAL,
                            ttl_seconds REAL
                        )
                        """
                    )
                    conn.commit()
                finally:
                    if not self.is_memory:
                        conn.close()
            logger.info(f"SQLiteStorage initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite storage at {self.db_path}: {e}")
            raise

    def _save_sync(self, key: str, value: str, ttl_seconds: int) -> None:
        with self._lock if self.is_memory else threading.Lock():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO sessions (session_id, json_data, updated_at, ttl_seconds)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                    json_data=excluded.json_data,
                    updated_at=excluded.updated_at,
                    ttl_seconds=excluded.ttl_seconds
                    """,
                    (key, value, time.time(), ttl_seconds),
                )
                conn.commit()
            finally:
                if not self.is_memory:
                    conn.close()

    async def save(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            await asyncio.to_thread(self._save_sync, key, value, ttl_seconds)
        except Exception as e:
            logger.error(f"Failed to save context to SQLite: {e}")

    def _load_sync(self, key: str) -> Optional[str]:
        with self._lock if self.is_memory else threading.Lock():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT json_data, updated_at, ttl_seconds FROM sessions WHERE session_id = ?",
                    (key,),
                )
                row = cursor.fetchone()

                if row:
                    json_data, updated_at, ttl_seconds = row
                    if time.time() - updated_at > ttl_seconds:
                        # Expired, lazily delete
                        cursor.execute("DELETE FROM sessions WHERE session_id = ?", (key,))
                        conn.commit()
                        return None
                    return json_data
                return None
            finally:
                if not self.is_memory:
                    conn.close()

    async def load(self, key: str) -> Optional[str]:
        try:
            return await asyncio.to_thread(self._load_sync, key)
        except Exception as e:
            logger.error(f"Failed to load context from SQLite: {e}")
            return None

    def _delete_sync(self, key: str) -> None:
        with self._lock if self.is_memory else threading.Lock():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM sessions WHERE session_id = ?", (key,))
                conn.commit()
            finally:
                if not self.is_memory:
                    conn.close()

    async def delete(self, key: str) -> None:
        try:
            await asyncio.to_thread(self._delete_sync, key)
        except Exception as e:
            logger.error(f"Failed to clear context in SQLite: {e}")

    def purge_expired(self) -> int:
        """
        Actively removes expired sessions from the database.
        Returns the number of pruned rows.
        """
        with self._lock if self.is_memory else threading.Lock():
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM sessions WHERE (? - updated_at) > ttl_seconds", (time.time(),))
                conn.commit()
                return cursor.rowcount
            finally:
                if not self.is_memory:
                    conn.close()

    def close(self) -> None:
        """Closes any persistent connection (for :memory: databases)."""
        if self._mem_conn:
            try:
                self._mem_conn.close()
            except Exception:
                pass
            self._mem_conn = None
