from .manager import SessionContinuityManager
from .security import generate_session_token, verify_session_token
from .storage.base import BaseStorage
from .storage.redis_storage import RedisStorage
from .storage.sqlite_storage import SQLiteStorage
from pipecat.frames.frames import LLMMessagesAppendFrame
import logging

__all__ = ["SessionContinuityManager", "BaseStorage", "RedisStorage", "SQLiteStorage"]

logger = logging.getLogger(__name__)

class SessionContinuity:
    def __init__(self, storage_backend=None, redis_url=None, ttl_seconds=3600, secret=None, stale_threshold_minutes=30):
        self.manager = SessionContinuityManager(storage_backend, redis_url, ttl_seconds)
        self.secret = secret
        self.stale_threshold_minutes = stale_threshold_minutes

    async def resume_or_start(self, task, context, session_id) -> tuple[bool, dict]:
        """
        Loads context if present, wires it into the LLMContext, injects the correct
        bridge or greet message via queue_frames, and returns (is_resumed, pending_tool_calls).
        """
        restored_context = await self.manager.load_context(session_id)
        is_resumed = False
        pending_tool_calls = {}

        if restored_context and restored_context.get("messages"):
            context.set_messages(restored_context["messages"])
            pending_tool_calls = restored_context.get("pending_tool_calls", {})
            is_resumed = True
            time_away_seconds = restored_context.get("time_away_seconds", 0)
            logger.info(f"Resuming session {session_id} with {len(context.get_messages())} messages and {len(pending_tool_calls)} pending tools.")
            
            # Tool call hallucination mitigation
            for call_id, tool_data in pending_tool_calls.items():
                if tool_data.get("status") == "pending" and "tool_name" in tool_data:
                    tool_name = tool_data["tool_name"]
                    sys_msg = {
                        "role": "system",
                        "content": f"[System Notice: The connection dropped while executing the tool '{tool_name}'. Do NOT call this tool again for the same request. Inform the user you are resuming the previous task and ask them to confirm before proceeding.]"
                    }
                    context.add_message(sys_msg)

        else:
            logger.info(f"Starting fresh session for {session_id}")

        # Inject the appropriate system message
        if is_resumed:
            if time_away_seconds > self.stale_threshold_minutes * 60:
                msg = {
                    "role": "user",
                    "content": "[System Notice: The user was disconnected for a while and just reconnected. Welcome them back, acknowledge it's been a bit, and ask if they'd like to pick up where you left off.]"
                }
            else:
                msg = {
                    "role": "user",
                    "content": "[System Notice: The connection dropped and was just restored. Please briefly acknowledge this to the user and ask how you can continue helping.]"
                }
        else:
            msg = {
                "role": "user",
                "content": "[System Notice: A new user has just connected to the voice call. Please greet them warmly and ask how you can help.]"
            }
        
        await task.queue_frames([LLMMessagesAppendFrame([msg])])
        return is_resumed, pending_tool_calls

    async def checkpoint(self, context, session_id, pending_tool_calls=None):
        """
        Snapshots the current context messages and pending tool calls to Redis.
        """
        messages = context.get_messages()
        await self.manager.save_context(session_id, messages, pending_tool_calls)

    async def clear(self, session_id):
        """
        Clears the session from Redis.
        """
        await self.manager.clear_context(session_id)

    def new_session(self) -> tuple[str, str]:
        """Wraps generate_session_token for the /create_session endpoint."""
        return generate_session_token(self.secret)

    def verify(self, session_id, signature) -> bool:
        """Wraps verify_session_token for websocket authentication."""
        return verify_session_token(session_id, signature, self.secret)
