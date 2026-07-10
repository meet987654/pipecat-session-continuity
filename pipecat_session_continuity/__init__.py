from .manager import SessionContinuityManager
from .security import generate_session_token, verify_session_token
from pipecat.frames.frames import LLMMessagesAppendFrame
import logging

logger = logging.getLogger(__name__)

class SessionContinuity:
    def __init__(self, redis_url=None, ttl_seconds=3600, secret=None):
        self.manager = SessionContinuityManager(redis_url, ttl_seconds)
        self.secret = secret

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
            logger.info(f"Resuming session {session_id} with {len(context.get_messages())} messages and {len(pending_tool_calls)} pending tools.")
        else:
            logger.info(f"Starting fresh session for {session_id}")

        # Inject the appropriate system message
        if is_resumed:
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
