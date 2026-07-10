import asyncio
import logging
from typing import Optional
from .base import BaseStorage

logger = logging.getLogger(__name__)

class RedisStorage(BaseStorage):
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._redis_client = None
        self.use_memory = False
        self._memory_store = {}
        
        self._init_redis()

    def _init_redis(self):
        try:
            import redis
            self._redis_client = redis.Redis.from_url(self.redis_url)
            self._redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(
                "\n" + "="*60 +
                f"\n[CRITICAL WARNING] Failed to connect to Redis at {self.redis_url}." +
                f"\nError: {str(e)}" +
                "\nSession continuity is falling back to IN-MEMORY storage." +
                "\nSessions will NOT survive process restarts or load balancing!" +
                "\n" + "="*60
            )
            self.use_memory = True

    async def save(self, key: str, value: str, ttl_seconds: int) -> None:
        if self.use_memory:
            self._memory_store[key] = value
        else:
            try:
                await asyncio.to_thread(self._redis_client.setex, key, ttl_seconds, value)
            except Exception as e:
                logger.error(f"Failed to save context to Redis: {e}")

    async def load(self, key: str) -> Optional[str]:
        if self.use_memory:
            return self._memory_store.get(key)
        else:
            try:
                res = await asyncio.to_thread(self._redis_client.get, key)
                return res.decode("utf-8") if res else None
            except Exception as e:
                logger.error(f"Failed to load context from Redis: {e}")
                return None

    async def delete(self, key: str) -> None:
        if self.use_memory:
            self._memory_store.pop(key, None)
        else:
            try:
                await asyncio.to_thread(self._redis_client.delete, key)
            except Exception as e:
                logger.error(f"Failed to clear context in Redis: {e}")
