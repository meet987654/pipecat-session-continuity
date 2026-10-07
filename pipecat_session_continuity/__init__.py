from .manager import SessionContinuityManager
from .security import generate_session_token, verify_session_token
from .storage.base import BaseStorage
from .storage.redis_storage import RedisStorage
from .storage.sqlite_storage import SQLiteStorage
from .idempotency import (
    IdempotencyRegistry,
    generate_idempotency_key,
    canonicalize_arguments,
)
from pipecat.frames.frames import LLMMessagesAppendFrame
import logging

__all__ = [
    "SessionContinuity",
    "SessionContinuityManager",
    "BaseStorage",
    "RedisStorage",
    "SQLiteStorage",
    "IdempotencyRegistry",
    "generate_idempotency_key",
    "canonicalize_arguments",
]

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

        if restored_context and ("messages" in restored_context or "pending_tool_calls" in restored_context):
            messages = restored_context.get("messages") or []
            context.set_messages(messages)
            raw_tools = restored_context.get("pending_tool_calls", {})
            registry = IdempotencyRegistry(raw_tools)
            pending_tool_calls = registry.to_dict()
            is_resumed = True
            time_away_seconds = restored_context.get("time_away_seconds", 0)
            logger.info(f"Resuming session {session_id} with {len(context.get_messages())} messages and {len(pending_tool_calls)} pending tools.")
            
            # Stronger Tool call hallucination mitigation
            for idemp_key, tool_data in registry.records.items():
                tool_name = tool_data.get("tool_name", "unknown")
                args = tool_data.get("arguments")
                args_desc = f" with arguments {args}" if args is not None else ""
                status = tool_data.get("status")

                if status == "pending":
                    sys_msg = {
                        "role": "system",
                        "content": (
                            f"[System Notice: The connection dropped while executing the tool '{tool_name}'{args_desc}. "
                            f"Do NOT call this tool again for the same request or arguments. "
                            f"Inform the user you are resuming the previous task, clarify that the outcome of '{tool_name}' is unconfirmed, "
                            f"and ask them to confirm before retrying.]"
                        )
                    }
                    context.add_message(sys_msg)
                elif status == "completed":
                    result = tool_data.get("result")
                    result_desc = f" Result: {result}." if result is not None else ""
                    sys_msg = {
                        "role": "system",
                        "content": (
                            f"[System Notice: The tool '{tool_name}'{args_desc} was already successfully executed prior to reconnect.{result_desc} "
                            f"Do NOT call '{tool_name}' again with these arguments. Use the previous result to respond to the user.]"
                        )
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

    def check_tool_idempotency(
        self,
        pending_tool_calls,
        tool_name: str,
        arguments=None,
        tool_call_id=None,
        client_token=None,
    ):
        """
        Checks if a tool call was already initiated or completed.
        Works with both IdempotencyRegistry instances and raw dictionaries.
        """
        if isinstance(pending_tool_calls, IdempotencyRegistry):
            return pending_tool_calls.check(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
            )
        registry = IdempotencyRegistry(pending_tool_calls)
        return registry.check(
            tool_name=tool_name,
            arguments=arguments,
            tool_call_id=tool_call_id,
            client_token=client_token,
        )

    def record_tool_call(
        self,
        pending_tool_calls,
        tool_name: str,
        arguments=None,
        tool_call_id=None,
        client_token=None,
        status: str = "pending",
    ):
        """
        Registers an in-flight tool call in the registry or dictionary.
        """
        if isinstance(pending_tool_calls, IdempotencyRegistry):
            return pending_tool_calls.register_call(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
                status=status,
            )
        registry = IdempotencyRegistry(pending_tool_calls)
        record = registry.register_call(
            tool_name=tool_name,
            arguments=arguments,
            tool_call_id=tool_call_id,
            client_token=client_token,
            status=status,
        )
        pending_tool_calls.update(registry.to_dict())
        return record

    def complete_tool_call(
        self,
        pending_tool_calls,
        key_or_call_id: str,
        result,
    ):
        """
        Marks a tool call as completed and stores the result.
        """
        if isinstance(pending_tool_calls, IdempotencyRegistry):
            return pending_tool_calls.complete_call(key_or_call_id, result)
        registry = IdempotencyRegistry(pending_tool_calls)
        record = registry.complete_call(key_or_call_id, result)
        pending_tool_calls.update(registry.to_dict())
        return record

    async def checkpoint(self, context, session_id, pending_tool_calls=None):
        """
        Snapshots the current context messages and pending tool calls to storage.
        """
        messages = context.get_messages()
        if isinstance(pending_tool_calls, IdempotencyRegistry):
            tools_to_save = pending_tool_calls.to_dict()
        else:
            tools_to_save = pending_tool_calls
        await self.manager.save_context(session_id, messages, tools_to_save)

    async def clear(self, session_id):
        """
        Clears the session from storage.
        """
        await self.manager.clear_context(session_id)

    def new_session(self) -> tuple[str, str]:
        """Wraps generate_session_token for the /create_session endpoint."""
        return generate_session_token(self.secret)

    def verify(self, session_id, signature) -> bool:
        """Wraps verify_session_token for websocket authentication."""
        return verify_session_token(session_id, signature, self.secret)
