import asyncio
import sqlite3
import time
import logging
from typing import Optional
from .base import BaseStorage

logger = logging.getLogger(__name__)

class SQLiteStorage(BaseStorage):
    def __init__(self, db_path: str = "pipecat_sessions.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        # We run initialization synchronously since it's just table creation at startup
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sessions (
                        session_id TEXT PRIMARY KEY,
                        json_data TEXT,
                        updated_at REAL,
                        ttl_seconds REAL
                    )
                ''')
                conn.commit()
            logger.info(f"SQLiteStorage initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite storage at {self.db_path}: {e}")
            raise

    def _save_sync(self, key: str, value: str, ttl_seconds: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO sessions (session_id, json_data, updated_at, ttl_seconds)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                json_data=excluded.json_data,
                updated_at=excluded.updated_at,
                ttl_seconds=excluded.ttl_seconds
            ''', (key, value, time.time(), ttl_seconds))
            conn.commit()

    async def save(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            await asyncio.to_thread(self._save_sync, key, value, ttl_seconds)
        except Exception as e:
            logger.error(f"Failed to save context to SQLite: {e}")

    def _load_sync(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT json_data, updated_at, ttl_seconds FROM sessions WHERE session_id = ?', (key,))
            row = cursor.fetchone()
            
            if row:
                json_data, updated_at, ttl_seconds = row
                if time.time() - updated_at > ttl_seconds:
                    # Expired, lazily delete
                    cursor.execute('DELETE FROM sessions WHERE session_id = ?', (key,))
                    conn.commit()
                    return None
                return json_data
            return None

    async def load(self, key: str) -> Optional[str]:
        try:
            return await asyncio.to_thread(self._load_sync, key)
        except Exception as e:
            logger.error(f"Failed to load context from SQLite: {e}")
            return None

    def _delete_sync(self, key: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sessions WHERE session_id = ?', (key,))
            conn.commit()

    async def delete(self, key: str) -> None:
        try:
            await asyncio.to_thread(self._delete_sync, key)
        except Exception as e:
            logger.error(f"Failed to clear context in SQLite: {e}")
