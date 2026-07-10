import json
import time
import asyncio
import logging
from typing import List, Dict, Any, Optional

from .storage.base import BaseStorage

logger = logging.getLogger(__name__)

class SessionContinuityManager:
    def __init__(self, storage_backend: Optional[BaseStorage] = None, redis_url: str = None, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self.checkpoint_times = []
        
        if storage_backend:
            self.storage = storage_backend
        else:
            from .storage.redis_storage import RedisStorage
            # Default to RedisStorage for backward compatibility
            url = redis_url or "redis://localhost:6379"
            self.storage = RedisStorage(redis_url=url)

    def _get_key(self, session_id: str) -> str:
        return f"pipecat:session:{session_id}"

    async def save_context(self, session_id: str, messages: List[Dict[str, Any]], pending_tool_calls: Dict[str, Dict[str, Any]] = None):
        """
        Snapshots the conversation history and tool state to the storage backend under the given session_id.
        """
        if not session_id:
            logger.warning("No session_id provided, skipping context save.")
            return

        start_time = time.time()
        key = self._get_key(session_id)
        try:
            data = json.dumps({
                "messages": messages,
                "pending_tool_calls": pending_tool_calls or {},
                "updated_at": time.time()
            })
            
            await self.storage.save(key, data, self.ttl_seconds)
            
            elapsed_ms = (time.time() - start_time) * 1000
            self.checkpoint_times.append(elapsed_ms)
            logger.info(f"Context saved for session {session_id} ({len(messages)} messages) in {elapsed_ms:.2f}ms.")
        except Exception as e:
            logger.error(f"Error saving context for session {session_id}: {e}")

    async def load_context(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to retrieve the conversation history and tool state.
        Returns a dictionary with 'messages' and 'pending_tool_calls' if found, else None.
        """
        if not session_id:
            return None

        key = self._get_key(session_id)
        try:
            data = await self.storage.load(key)
                
            if data:
                parsed = json.loads(data)
                
                # Compute time away
                updated_at = parsed.get("updated_at", time.time())
                time_away_seconds = time.time() - updated_at
                parsed["time_away_seconds"] = time_away_seconds
                
                logger.info(f"Context restored for session {session_id}.")
                return parsed
            else:
                logger.info(f"No active session found for {session_id}.")
                return None
        except Exception as e:
            logger.error(f"Error loading context for session {session_id}: {e}")
            return None

    async def clear_context(self, session_id: str):
        """
        Clears the session from storage (useful on deliberate end of call).
        """
        key = self._get_key(session_id)
        try:
            await self.storage.delete(key)
            logger.info(f"Context cleared for session {session_id}.")
        except Exception as e:
            logger.error(f"Error clearing context for session {session_id}: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        times = sorted(self.checkpoint_times)
        if not times:
            return {"count": 0, "mean_ms": 0, "p95_ms": 0, "max_ms": 0}
        
        mean = sum(times) / len(times)
        p95_idx = int(len(times) * 0.95)
        if p95_idx >= len(times): p95_idx = len(times) - 1
        
        return {
            "count": len(times),
            "mean_ms": round(mean, 2),
            "p95_ms": round(times[p95_idx], 2),
            "max_ms": round(times[-1], 2)
        }
