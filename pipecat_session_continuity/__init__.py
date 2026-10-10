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
from .metrics import (
    BaseMetricsExporter,
    InMemoryMetricsExporter,
    PrometheusMetricsExporter,
    OpenTelemetryMetricsExporter,
    CompositeMetricsExporter,
)
from .processor import SessionContinuityProcessor
from pipecat.frames.frames import LLMMessagesAppendFrame
from typing import Optional, Any, Dict, List
import logging

__all__ = [
    "SessionContinuity",
    "SessionContinuityManager",
    "SessionContinuityProcessor",
    "BaseStorage",
    "RedisStorage",
    "SQLiteStorage",
    "IdempotencyRegistry",
    "generate_idempotency_key",
    "canonicalize_arguments",
    "BaseMetricsExporter",
    "InMemoryMetricsExporter",
    "PrometheusMetricsExporter",
    "OpenTelemetryMetricsExporter",
    "CompositeMetricsExporter",
]

logger = logging.getLogger(__name__)

class SessionContinuity:
    """
    Drop-in session continuity and connection resilience manager for Pipecat agents.
    
    Defaults to SQLite storage (pipecat_sessions.db) for zero-config local development without
    requiring Redis. Pass redis_url="redis://localhost:6379" or a custom storage_backend for production.
    """
    def __init__(
        self,
        storage_backend=None,
        redis_url=None,
        db_path=None,
        ttl_seconds=3600,
        secret=None,
        stale_threshold_minutes=30,
        enable_prometheus: bool = False,
        enable_opentelemetry: bool = False,
        metrics_exporter: Optional[BaseMetricsExporter] = None,
    ):
        if metrics_exporter is not None:
            active_exporter = metrics_exporter
        else:
            exporters = []
            if enable_prometheus:
                exporters.append(PrometheusMetricsExporter())
            if enable_opentelemetry:
                exporters.append(OpenTelemetryMetricsExporter())
            if len(exporters) == 0:
                active_exporter = InMemoryMetricsExporter()
            elif len(exporters) == 1:
                active_exporter = exporters[0]
            else:
                active_exporter = CompositeMetricsExporter(exporters)

        self.manager = SessionContinuityManager(
            storage_backend=storage_backend,
            redis_url=redis_url,
            db_path=db_path,
            ttl_seconds=ttl_seconds,
            metrics_exporter=active_exporter,
        )
        self.metrics = self.manager.metrics
        self.secret = secret
        self.stale_threshold_minutes = stale_threshold_minutes

    async def resume_or_start(
        self,
        task,
        context,
        session_id: str,
        return_metadata: bool = False,
    ) -> tuple:
        """
        Loads context if present, wires it into the LLMContext, injects the correct
        bridge or greet message via queue_frames, and returns:
          - (is_resumed, pending_tool_calls) if return_metadata=False (default)
          - (is_resumed, pending_tool_calls, metadata) if return_metadata=True
        """
        import time
        start_time = time.time()
        is_resumed = False
        pending_tool_calls = {}
        metadata = {}
        try:
            restored_context = await self.manager.load_context(session_id)

            if restored_context and ("messages" in restored_context or "pending_tool_calls" in restored_context):
                messages = restored_context.get("messages") or []
                context.set_messages(messages)
                raw_tools = restored_context.get("pending_tool_calls", {})
                registry = IdempotencyRegistry(raw_tools)
                pending_tool_calls = registry.to_dict()
                metadata = restored_context.get("metadata", {})
                is_resumed = True
                time_away_seconds = restored_context.get("time_away_seconds", 0)
                logger.info(f"Resuming session {session_id} with {len(context.get_messages())} messages, {len(pending_tool_calls)} pending tools, and metadata keys: {list(metadata.keys())}.")
                
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
            elapsed_ms = (time.time() - start_time) * 1000
            self.metrics.record_resume(elapsed_ms, is_resumed=is_resumed, status="success")
            if is_resumed:
                self.metrics.record_reconnect()

            if return_metadata:
                return is_resumed, pending_tool_calls, metadata
            return is_resumed, pending_tool_calls
        except Exception:
            elapsed_ms = (time.time() - start_time) * 1000
            self.metrics.record_resume(elapsed_ms, is_resumed=is_resumed, status="error")
            raise

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
            is_dup, rec = pending_tool_calls.check(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
            )
        else:
            registry = IdempotencyRegistry(pending_tool_calls)
            is_dup, rec = registry.check(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
            )

        if is_dup:
            self.metrics.record_tool_call(tool_name, status="duplicate")
        return is_dup, rec

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
            rec = pending_tool_calls.register_call(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
                status=status,
            )
        else:
            registry = IdempotencyRegistry(pending_tool_calls)
            rec = registry.register_call(
                tool_name=tool_name,
                arguments=arguments,
                tool_call_id=tool_call_id,
                client_token=client_token,
                status=status,
            )
            pending_tool_calls.update(registry.to_dict())

        self.metrics.record_tool_call(tool_name, status=status)
        return rec

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
            rec = pending_tool_calls.complete_call(key_or_call_id, result)
        else:
            registry = IdempotencyRegistry(pending_tool_calls)
            rec = registry.complete_call(key_or_call_id, result)
            pending_tool_calls.update(registry.to_dict())

        tool_name = rec.get("tool_name", key_or_call_id) if rec else key_or_call_id
        self.metrics.record_tool_call(tool_name, status="completed")
        return rec

    async def checkpoint(
        self,
        context,
        session_id: str,
        pending_tool_calls=None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Snapshots current context messages, pending tool calls, and arbitrary metadata to storage.
        """
        messages = context.get_messages()
        if isinstance(pending_tool_calls, IdempotencyRegistry):
            tools_to_save = pending_tool_calls.to_dict()
        else:
            tools_to_save = pending_tool_calls
        await self.manager.save_context(
            session_id=session_id,
            messages=messages,
            pending_tool_calls=tools_to_save,
            metadata=metadata,
        )

    async def get_metadata(self, session_id: str) -> Dict[str, Any]:
        """
        Retrieves custom session and dialog metadata for session_id.
        """
        context_data = await self.manager.load_context(session_id)
        if context_data:
            return context_data.get("metadata", {})
        return {}

    async def set_metadata(self, session_id: str, metadata: Dict[str, Any]) -> None:
        """
        Merges or updates custom metadata for an active session without modifying existing messages.
        """
        context_data = await self.manager.load_context(session_id)
        messages = context_data.get("messages", []) if context_data else []
        tools = context_data.get("pending_tool_calls", {}) if context_data else {}
        current_meta = context_data.get("metadata", {}) if context_data else {}
        current_meta.update(metadata)
        await self.manager.save_context(
            session_id=session_id,
            messages=messages,
            pending_tool_calls=tools,
            metadata=current_meta,
        )

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

    def get_metrics(self):
        """Returns the current metrics summary."""
        return self.manager.get_metrics()

    def get_prometheus_metrics(self) -> bytes:
        """
        Exports metrics in standard Prometheus exposition format (bytes).
        Requires enable_prometheus=True or a PrometheusMetricsExporter.
        """
        if hasattr(self.metrics, "generate_latest"):
            return self.metrics.generate_latest()
        if isinstance(self.metrics, CompositeMetricsExporter):
            for exp in self.metrics.exporters:
                if hasattr(exp, "generate_latest"):
                    return exp.generate_latest()
        raise ValueError("Prometheus metrics export is not enabled. Initialize SessionContinuity(enable_prometheus=True).")

    def get_prometheus_metrics_str(self) -> str:
        """
        Exports metrics in standard Prometheus exposition format (UTF-8 string).
        """
        return self.get_prometheus_metrics().decode("utf-8")

    def create_processor(
        self,
        session_id: str,
        context: Any,
        pending_tool_calls: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
        get_metadata_fn: Optional[Any] = None,
        extra_state_fn: Optional[Any] = None,
        **kwargs,
    ) -> SessionContinuityProcessor:
        """
        Creates a native Pipecat FrameProcessor for zero-boilerplate pipeline integration.
        Automatically checkpoints conversation context and metadata on turn boundaries.
        """
        return SessionContinuityProcessor(
            continuity=self,
            session_id=session_id,
            context=context,
            pending_tool_calls=pending_tool_calls,
            metadata=metadata,
            get_metadata_fn=get_metadata_fn or extra_state_fn,
            extra_state_fn=extra_state_fn or get_metadata_fn,
            **kwargs,
        )

    # Convenience alias for pipeline definitions
    processor = create_processor

