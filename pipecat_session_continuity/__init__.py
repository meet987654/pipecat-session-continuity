from .manager import SessionContinuityManager
from .security import generate_session_token, verify_session_token
from .storage.base import BaseStorage
from .storage.redis_storage import RedisStorage
from .storage.sqlite_storage import SQLiteStorage
from .idempotency import (
    RETRY_ALLOWED,
    UNRESOLVED,
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
from typing import Optional, Any, Dict, List, Callable, Tuple
import asyncio
import inspect
import time
import logging

__all__ = [
    "SessionContinuity",
    "SessionContinuityManager",
    "SessionContinuityProcessor",
    "BaseStorage",
    "RedisStorage",
    "SQLiteStorage",
    "IdempotencyRegistry",
    "RETRY_ALLOWED",
    "UNRESOLVED",
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
        status_check_timeout_seconds: float = 1.0,
        pending_timeouts: Optional[Dict[str, float]] = None,
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
        self.status_check_timeout_seconds = status_check_timeout_seconds
        self._status_checks: Dict[str, Dict[str, Any]] = {}
        # Per-tool seconds after which a pending record stops blocking (see set_pending_timeout).
        self._pending_timeouts: Dict[str, float] = {}
        for name, seconds in (pending_timeouts or {}).items():
            self.set_pending_timeout(name, seconds)
        self._warned_unresolvable: set = set()

    def set_pending_timeout(self, tool_name: str, seconds: float) -> None:
        """
        Stop a pending record for `tool_name` from blocking forever.

        A record stays "pending" when the connection drops before the tool's result
        is recorded, including when the side effect never happened. Without a
        timeout (the default, unchanged from 0.3.0) the guard blocks every identical
        retry for the life of the session. With one, on resume, a record pending for
        longer than `seconds` (measured from its last attempt):

          - is first passed to the tool's status check, if one is registered, and
            resolved by its answer as usual;
          - otherwise (no status check, or one that could not answer) becomes
            ``UNRESOLVED``: the resume notice tells the LLM the outcome is unknown and
            to ask the caller whether to try the same request again, and the guard
            lets one identical retry through.

        Pick `seconds` to be longer than the tool can take to land on your backend.
        Note the trade-off: with no status check, if the original did go through and
        your backend ignores idempotency keys, the caller's retry can duplicate it.
        """
        if seconds is None or seconds < 0:
            raise ValueError("pending timeout must be a number of seconds >= 0")
        self._pending_timeouts[tool_name] = float(seconds)

    def _warn_if_unresolvable(self, tool_name: str) -> None:
        if tool_name in self._warned_unresolvable:
            return
        if tool_name in self._status_checks:
            return
        self._warned_unresolvable.add(tool_name)
        if tool_name in self._pending_timeouts:
            logger.warning(
                f"Tool '{tool_name}' is guarded but has no status check. After its "
                f"{self._pending_timeouts[tool_name]:g}s pending timeout, an interrupted call becomes the caller's "
                f"decision to retry; if the original did go through and the backend ignores idempotency keys, "
                f"that retry can duplicate it. Register a status check (register_status_check) to avoid guessing."
            )
        else:
            logger.warning(
                f"Tool '{tool_name}' is guarded but has no status check and no pending timeout. If the "
                f"connection drops before its result is recorded, every identical retry is blocked for the "
                f"rest of the session, even when the action never happened. Register a status check "
                f"(register_status_check) or set a pending timeout (set_pending_timeout)."
            )

    def register_status_check(
        self,
        tool_name: str,
        fn: Callable,
        retry_on_not_found: bool = False,
    ) -> None:
        """
        Registers an async status-check hook for `tool_name`.

        On resume, every record for that tool still "pending" is passed to
        ``fn(idempotency_key, args)``, which returns one of:
          - ``("completed", result)`` / ``{"status": "completed", "result": ...}``:
            the action happened. The record is marked completed and the LLM is
            told the confirmed result.
          - ``"not_found"``: by default treated like ``"unknown"`` (the LLM is told
            to ask the caller). Status APIs can lag behind the write, and retrying
            against an API that ignores idempotency keys would double-book. Only if
            you pass ``retry_on_not_found=True`` is exactly one retry allowed,
            under the SAME idempotency key.
          - ``"unknown"``, an exception, or no answer within
            ``status_check_timeout_seconds`` (default 1 s): the record stays
            pending and is marked inconclusive. The LLM is told the outcome cannot
            be confirmed and is forbidden from claiming success or failure. It is
            not told to ask the caller, because the backend has already been asked.

        Tools without a hook behave exactly as before (unconfirmed notice, ask the caller).
        """
        self._status_checks[tool_name] = {"fn": fn, "retry_on_not_found": retry_on_not_found}

    @staticmethod
    def _normalize_status_result(raw: Any) -> Tuple[str, Any]:
        status, result = raw, None
        if isinstance(raw, tuple) and raw:
            status, result = raw[0], (raw[1] if len(raw) > 1 else None)
        elif isinstance(raw, dict):
            status, result = raw.get("status"), raw.get("result")
        if status in ("completed", "not_found"):
            return status, result
        return "unknown", None

    async def _run_status_checks(self, registry: IdempotencyRegistry) -> None:
        """Runs registered hooks concurrently for unresolved records and updates the registry."""
        targets = [
            (key, rec)
            for key, rec in registry.records.items()
            if rec.get("status") in ("pending", RETRY_ALLOWED, UNRESOLVED)
            and rec.get("tool_name") in self._status_checks
        ]
        if not targets:
            return

        async def _one(key: str, rec: Dict[str, Any]) -> Tuple[str, Any]:
            async def _call():
                out = self._status_checks[rec["tool_name"]]["fn"](key, rec.get("arguments"))
                if inspect.isawaitable(out):
                    out = await out
                return out

            try:
                raw = await asyncio.wait_for(_call(), timeout=self.status_check_timeout_seconds)
                return key, self._normalize_status_result(raw)
            except asyncio.TimeoutError:
                logger.warning(f"Status check for '{rec.get('tool_name')}' ({key}) timed out; leaving unconfirmed.")
            except Exception as e:
                logger.warning(f"Status check for '{rec.get('tool_name')}' ({key}) failed: {e!r}; leaving unconfirmed.")
            return key, ("unknown", None)

        for key, (status, result) in await asyncio.gather(*[_one(k, r) for k, r in targets]):
            if status == "completed":
                rec = registry.complete_call(key, result)
                if rec is not None:
                    rec["confirmed_by"] = "status_check"
            elif status == "not_found" and self._status_checks[registry.records[key]["tool_name"]]["retry_on_not_found"]:
                registry.records[key]["status"] = RETRY_ALLOWED
                registry.records[key]["updated_at"] = time.time()
            else:
                # The backend was asked and gave no usable answer (unknown, untrusted
                # not_found, error or timeout). Neither "done" nor "not done" is
                # supported, so resume forbids the LLM from claiming either.
                registry.records[key]["status_check"] = "inconclusive"

    def _escalate_timed_out(self, registry: IdempotencyRegistry) -> None:
        """After status checks: a pending record past its tool's pending_timeout becomes UNRESOLVED."""
        now = time.time()
        for rec in registry.records.values():
            timeout = self._pending_timeouts.get(rec.get("tool_name"))
            if rec.get("status") != "pending" or timeout is None:
                continue
            last = rec.get("updated_at") or rec.get("created_at") or now
            if now - last >= timeout:
                rec["status"] = UNRESOLVED
                rec["unresolved_reason"] = (
                    "status_check_inconclusive" if rec.get("status_check") == "inconclusive" else "no_status_check"
                )
                logger.warning(
                    f"Pending '{rec.get('tool_name')}' ({rec.get('idempotency_key')}) passed its "
                    f"{timeout:g}s pending timeout unresolved ({rec['unresolved_reason']}); asking the caller."
                )

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
        unconfirmed_tools: List[str] = []
        inconclusive_tools: List[str] = []  # pending AND a status check could not resolve them
        confirmed_tools: List[str] = []  # a status check confirmed they happened
        not_done_tools: List[str] = []  # a status check confirmed they did not happen (retry allowed)
        unresolved_tools: List[str] = []  # past pending_timeout, unresolved: the caller decides
        try:
            restored_context = await self.manager.load_context(session_id)

            if restored_context and ("messages" in restored_context or "pending_tool_calls" in restored_context):
                messages = restored_context.get("messages") or []
                context.set_messages(messages)
                raw_tools = restored_context.get("pending_tool_calls", {})
                registry = IdempotencyRegistry(raw_tools)
                await self._run_status_checks(registry)
                self._escalate_timed_out(registry)
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

                    if status == UNRESOLVED:
                        unresolved_tools.append(tool_name)
                        checked = (
                            " A status check could not tell either."
                            if tool_data.get("unresolved_reason") == "status_check_inconclusive" else ""
                        )
                        sys_msg = {
                            "role": "system",
                            "content": (
                                f"[System Notice: The connection dropped while executing the tool '{tool_name}'{args_desc}. "
                                f"Its outcome is unknown: it may have gone through or it may not have.{checked} "
                                f"Whether to try again is the caller's decision. Tell the user that you cannot confirm "
                                f"whether it went through, and ask whether they want you to try the same request again "
                                f"with the same details. If they say yes, call '{tool_name}' once more with exactly the same "
                                f"arguments. Do not say it succeeded and do not say it failed, and do not offer a different "
                                f"time, day or other details instead of asking.]"
                            )
                        }
                        context.add_message(sys_msg)
                    elif status == "pending" and tool_data.get("status_check") == "inconclusive":
                        unconfirmed_tools.append(tool_name)
                        inconclusive_tools.append(tool_name)
                        sys_msg = {
                            "role": "system",
                            "content": (
                                f"[System Notice: The connection dropped while executing the tool '{tool_name}'{args_desc}. "
                                f"A status check could not determine whether it went through: it may have completed or it may not have. "
                                f"Do NOT call this tool again for the same request or arguments. For the rest of this conversation, "
                                f"do not tell the user that it succeeded or is confirmed, and do not tell them that it failed, did not go "
                                f"through, or that no record of it exists: either could be false. Say that you cannot confirm the outcome "
                                f"right now. Do not suggest repeating the request with different details to work around this, because "
                                f"the original may already have gone through.]"
                            )
                        }
                        context.add_message(sys_msg)
                    elif status == "pending":
                        unconfirmed_tools.append(tool_name)
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
                    elif status == RETRY_ALLOWED:
                        not_done_tools.append(tool_name)
                        sys_msg = {
                            "role": "system",
                            "content": (
                                f"[System Notice: The connection dropped while executing the tool '{tool_name}'{args_desc}. "
                                f"A status check confirmed that this action did NOT happen. You may call '{tool_name}' once more "
                                f"with the same arguments, passing idempotency key '{idemp_key}'. Do not retry more than once.]"
                            )
                        }
                        context.add_message(sys_msg)
                    elif status == "completed" and tool_data.get("confirmed_by") == "status_check":
                        confirmed_tools.append(tool_name)
                        result = tool_data.get("result")
                        result_desc = f" Confirmed result: {result}." if result is not None else ""
                        sys_msg = {
                            "role": "system",
                            "content": (
                                f"[System Notice: A status check confirmed that the tool '{tool_name}'{args_desc} was successfully executed "
                                f"before the connection dropped.{result_desc} "
                                f"Do NOT call '{tool_name}' again with these arguments. Use the confirmed result to respond to the user.]"
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

            if is_resumed and unresolved_tools and not unconfirmed_tools:
                # Past its pending timeout with no answer: surface it as the caller's decision,
                # not a block. Retrying the same request is allowed once; steering is not asked for.
                names = ", ".join(f"'{n}'" for n in dict.fromkeys(unresolved_tools))
                msg["content"] = msg["content"].rstrip("]") + (
                    f" Before anything else, tell the user that their request ({names}) was interrupted by the "
                    f"disconnect and that you cannot confirm whether it went through, and ask whether they would like "
                    f"you to try the same request again with the same details. Do not say it succeeded and do not say "
                    f"it failed.]"
                )
            elif is_resumed and inconclusive_tools and len(inconclusive_tools) == len(unconfirmed_tools):
                # A status check already asked the backend and it could not say. Asking the
                # caller ("did you get a confirmation?") does not resolve that either, so the
                # instruction is to state the uncertainty, not to collect a guess.
                names = ", ".join(f"'{n}'" for n in dict.fromkeys(inconclusive_tools))
                msg["content"] = msg["content"].rstrip("]") + (
                    f" Before anything else, tell the user that their request ({names}) was interrupted by the "
                    f"disconnect and that you cannot confirm whether it went through. Do not say it succeeded and do not "
                    f"say it failed. Do not call the tool again.]"
                )
            elif is_resumed and unconfirmed_tools:
                # The last message is what the LLM answers, and a generic "acknowledge and ask how
                # to continue" message was found to override the pending-tool system notice (the
                # model never mentioned the interrupted action). Put the instruction here too.
                names = ", ".join(f"'{n}'" for n in dict.fromkeys(unconfirmed_tools))
                msg["content"] = msg["content"].rstrip("]") + (
                    f" Before anything else, tell the user that their request ({names}) was interrupted by the "
                    f"disconnect and that you cannot confirm whether it went through, and ask whether they received a "
                    f"confirmation. Do not say it succeeded and do not say it failed. Do not call the tool again until they answer.]"
                )
            elif is_resumed and (confirmed_tools or not_done_tools):
                # A definite answer from a status check: state it as fact up front, or the
                # model answers the generic message and never mentions the request.
                parts = []
                if confirmed_tools:
                    names = ", ".join(f"'{n}'" for n in dict.fromkeys(confirmed_tools))
                    parts.append(f"tell the user that their request ({names}) did go through before the disconnect, "
                                 f"using the confirmed result, and do not call it again")
                if not_done_tools:
                    names = ", ".join(f"'{n}'" for n in dict.fromkeys(not_done_tools))
                    parts.append(f"tell the user that their request ({names}) did not go through, and offer to try it once more")
                msg["content"] = msg["content"].rstrip("]") + " Before anything else, " + "; and ".join(parts) + ".]"

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

        if is_dup and rec is not None and rec.get("status") == UNRESOLVED:
            # Past its pending timeout and unresolved: the caller decides, so the identical
            # retry is allowed (record_tool_call then consumes it and the record is pending again).
            is_dup = False
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
        self._warn_if_unresolvable(tool_name)
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

