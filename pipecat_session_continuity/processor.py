"""
Pipecat FrameProcessor for Session Continuity
============================================
Enables drop-in, zero-boilerplate session checkpointing directly inside
Pipecat pipelines without requiring manual event handlers.
"""

import asyncio
import logging
from typing import Any, Optional, Dict

from pipecat.frames.frames import (
    Frame,
    EndFrame,
    LLMFullResponseEndFrame,
    BotStoppedSpeakingFrame,
    FunctionCallResultFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

logger = logging.getLogger(__name__)


class SessionContinuityProcessor(FrameProcessor):
    """
    A native Pipecat FrameProcessor that monitors pipeline execution frames
    and automatically checkpoints conversation context and tool state.

    Usage:
        ```python
        pipeline = Pipeline([
            transport.input(),
            stt,
            continuity.processor(session_id=session_id, context=context),
            llm,
            tts,
            transport.output(),
        ])
        ```
    """

    def __init__(
        self,
        continuity: Any,
        session_id: str,
        context: Any,
        pending_tool_calls: Optional[Any] = None,
        checkpoint_on_llm_end: bool = True,
        checkpoint_on_bot_stopped: bool = True,
        checkpoint_on_end_frame: bool = True,
        checkpoint_on_function_result: bool = True,
        non_blocking: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.continuity = continuity
        self.session_id = session_id
        self.context = context
        self.pending_tool_calls = pending_tool_calls
        self.checkpoint_on_llm_end = checkpoint_on_llm_end
        self.checkpoint_on_bot_stopped = checkpoint_on_bot_stopped
        self.checkpoint_on_end_frame = checkpoint_on_end_frame
        self.checkpoint_on_function_result = checkpoint_on_function_result
        self.non_blocking = non_blocking

        self._active_tasks = set()
        self._checkpoint_count = 0

    def set_pending_tools(self, pending_tools: Any) -> None:
        """Dynamically updates the reference to pending tool calls."""
        self.pending_tool_calls = pending_tools

    @property
    def checkpoint_count(self) -> int:
        """Returns the number of checkpoints initiated by this processor."""
        return self._checkpoint_count

    async def _do_checkpoint(self) -> None:
        """Internal helper to execute the checkpoint against continuity storage."""
        try:
            self._checkpoint_count += 1
            await self.continuity.checkpoint(
                self.context,
                self.session_id,
                self.pending_tool_calls,
            )
            logger.debug(
                f"[SessionContinuityProcessor] Saved checkpoint #{self._checkpoint_count} "
                f"for session {self.session_id}"
            )
        except Exception as e:
            logger.error(
                f"[SessionContinuityProcessor] Error saving checkpoint for session {self.session_id}: {e}",
                exc_info=True,
            )

    def _schedule_checkpoint(self) -> None:
        """Triggers checkpoint, optionally non-blocking to protect audio streaming latency."""
        if self.non_blocking:
            task = asyncio.create_task(self._do_checkpoint())
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
        else:
            # We schedule on event loop so process_frame is not blocked
            asyncio.create_task(self._do_checkpoint())

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """
        Intercepts pipeline frames and automatically saves conversation state.
        Frames are immediately forwarded downstream to ensure zero audio latency.
        """
        await super().process_frame(frame, direction)

        # Always push the frame downstream immediately to prevent pipeline latency
        await self.push_frame(frame, direction)

        # Check if frame signals a turn transition or completion
        if self.checkpoint_on_llm_end and isinstance(frame, LLMFullResponseEndFrame):
            logger.debug(f"[SessionContinuityProcessor] Triggered by LLMFullResponseEndFrame")
            self._schedule_checkpoint()

        elif self.checkpoint_on_bot_stopped and isinstance(frame, BotStoppedSpeakingFrame):
            logger.debug(f"[SessionContinuityProcessor] Triggered by BotStoppedSpeakingFrame")
            self._schedule_checkpoint()

        elif self.checkpoint_on_function_result and isinstance(frame, FunctionCallResultFrame):
            logger.debug(f"[SessionContinuityProcessor] Triggered by FunctionCallResultFrame")
            self._schedule_checkpoint()

        elif self.checkpoint_on_end_frame and isinstance(frame, EndFrame):
            logger.debug(f"[SessionContinuityProcessor] Triggered by EndFrame (graceful teardown)")
            # On EndFrame, wait for checkpoint to guarantee final state is committed
            await self._do_checkpoint()

    async def cleanup(self):
        """Awaits any in-flight asynchronous checkpoint tasks before disposal."""
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            self._active_tasks.clear()
        await super().cleanup()

