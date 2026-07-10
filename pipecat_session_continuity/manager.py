import json
import redis
import os
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SessionContinuityManager:
    # Class-level dictionary for in-memory fallback across instances
    _memory_fallback: Dict[str, Any] = {}

    def __init__(self, redis_url: str = None, ttl_seconds: int = 3600):
        if not redis_url:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.ttl_seconds = ttl_seconds
        self.use_memory = False
        self.checkpoint_times = []
        
        try:
            self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=2)
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {redis_url} for session continuity.")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            print("\n" + "="*80)
            print("🛑 WARNING: REDIS CONNECTION FAILED! 🛑")
            print("SessionContinuityManager is falling back to an IN-MEMORY dictionary.")
            print("Sessions will NOT persist across server restarts!")
            print("="*80 + "\n")
            self.use_memory = True

    def _get_key(self, session_id: str) -> str:
        return f"pipecat:session:{session_id}"

    async def save_context(self, session_id: str, messages: List[Dict[str, Any]], pending_tool_calls: Dict[str, Dict[str, Any]] = None):
        """
        Snapshots the conversation history and tool state to Redis under the given session_id.
        """
        if not session_id:
            logger.warning("No session_id provided, skipping context save.")
            return

        import time
        start_time = time.time()
        key = self._get_key(session_id)
        try:
            data = json.dumps({
                "messages": messages,
                "pending_tool_calls": pending_tool_calls or {}
            })
            if self.use_memory:
                self._memory_fallback[key] = data
            else:
                import asyncio
                await asyncio.to_thread(self.redis_client.setex, key, self.ttl_seconds, data)
            
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
            if self.use_memory:
                data = self._memory_fallback.get(key)
            else:
                import asyncio
                data = await asyncio.to_thread(self.redis_client.get, key)
                
            if data:
                parsed = json.loads(data)
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
        Clears the session from Redis (useful on deliberate end of call).
        """
        key = self._get_key(session_id)
        try:
            if self.use_memory:
                self._memory_fallback.pop(key, None)
            else:
                import asyncio
                await asyncio.to_thread(self.redis_client.delete, key)
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
