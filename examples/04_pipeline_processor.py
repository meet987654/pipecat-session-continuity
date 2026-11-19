"""
Example 04: Zero-Boilerplate Native Pipecat FrameProcessor Integration
========================================================================
Demonstrates using SessionContinuityProcessor as a native FrameProcessor
inside a Pipecat Pipeline.

Instead of writing custom callback handlers or event listeners to capture
turns, simply drop `continuity.processor(session_id, context)` into your
pipeline:

    pipeline = Pipeline([
        transport.input(),
        stt,
        continuity.processor(session_id, context),
        llm,
        tts,
        transport.output(),
    ])

Key Features Demonstrated:
1. Zero-Boilerplate: Automatic non-blocking checkpointing on:
   - LLMFullResponseEndFrame (completion of LLM generation)
   - BotStoppedSpeakingFrame (completion of bot audio playback)
   - FunctionCallResultFrame (completion of tool/function calls)
   - EndFrame (graceful pipeline teardown)
2. Zero Audio Latency Penalty: Frame forwarding is synchronous and non-blocking;
   state persistence is handled asynchronously via managed background tasks.
3. Clean Resumption: Seamless context restoration across process restarts.
"""

import asyncio
import os
import sys

# Ensure root package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat.frames.frames import (
    EndFrame,
    LLMFullResponseEndFrame,
    BotStoppedSpeakingFrame,
    FunctionCallResultFrame,
    StartFrame,
    TextFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.processors.frame_processor import (
    FrameDirection,
    FrameProcessor,
    FrameProcessorSetup,
)
from pipecat.clocks.system_clock import SystemClock
from pipecat.utils.asyncio.task_manager import TaskManager

from pipecat_session_continuity import SessionContinuity


class MockLLMContext:
    """Mock LLM context compatible with Pipecat's OpenAILLMContext."""
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


class OutputAudioCollector(FrameProcessor):
    """Simulates transport.output() collecting downstream audio/text frames."""
    def __init__(self):
        super().__init__(enable_direct_mode=True)
        self.received_frames = []

    async def process_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
        await super().process_frame(frame, direction)
        self.received_frames.append(frame)
        await self.push_frame(frame, direction)


async def main():
    db_path = "pipeline_processor_demo.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    print("=" * 70)
    print("PIPECAT NATIVE FRAMEPROCESSOR PIPELINE INTEGRATION")
    print("=" * 70)

    # 1. Initialize SessionContinuity with local SQLite
    continuity = SessionContinuity(db_path=db_path)
    session_id = "user-sess-voice-assistant-42"

    # 2. Initialize LLM Context
    context = MockLLMContext([
        {"role": "system", "content": "You are a professional banking voice assistant."},
        {"role": "user", "content": "Hello, I want to transfer $50 to Alice."},
    ])

    # 3. Create SessionContinuityProcessor using the convenient factory
    continuity_processor = continuity.processor(
        session_id=session_id,
        context=context,
        checkpoint_on_llm_end=True,
        checkpoint_on_bot_stopped=True,
        checkpoint_on_function_result=True,
        checkpoint_on_end_frame=True,
    )

    output_sink = OutputAudioCollector()

    # 4. Assemble standard Pipecat Pipeline
    # In production: Pipeline([transport.input(), stt, continuity_processor, llm, tts, transport.output()])
    pipeline = Pipeline([continuity_processor, output_sink])

    # Setup pipeline with TaskManager & SystemClock
    setup = FrameProcessorSetup(
        clock=SystemClock(),
        task_manager=TaskManager(),
        pipeline_worker=None,
    )
    for p in pipeline.processors:
        await p.setup(setup)

    print("\n[Step 1] Initialized pipeline with SessionContinuityProcessor.")
    print(f"  Session ID : {session_id}")
    print(f"  Messages   : {len(context.get_messages())}")
    print(f"  Checkpoints: {continuity_processor.checkpoint_count}")

    # Start pipeline
    await pipeline.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    # 5. Simulate LLM generating a response and tool call
    print("\n[Step 2] Simulating LLM completion and tool invocation...")
    context.add_message({
        "role": "assistant",
        "content": "Transferring $50 to Alice now...",
        "tool_calls": [{"id": "call_transfer_1", "function": {"name": "transfer_funds"}}],
    })

    # Push LLMFullResponseEndFrame - automatically triggers non-blocking checkpoint!
    await pipeline.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(0.05)  # Allow background checkpoint task to persist
    print(f"  Checkpoints after LLM completion: {continuity_processor.checkpoint_count}")

    # 6. Simulate tool call completion
    print("\n[Step 3] Simulating tool execution result...")
    context.add_message({
        "role": "tool",
        "tool_call_id": "call_transfer_1",
        "content": '{"status": "success", "tx_id": "tx_998811"}',
    })

    func_result = FunctionCallResultFrame(
        function_name="transfer_funds",
        tool_call_id="call_transfer_1",
        arguments={"amount": 50, "recipient": "Alice"},
        result={"status": "success", "tx_id": "tx_998811"},
    )
    await pipeline.process_frame(func_result, FrameDirection.DOWNSTREAM)
    await asyncio.sleep(0.05)
    print(f"  Checkpoints after tool result   : {continuity_processor.checkpoint_count}")

    # 7. Simulate Bot audio finishing playback
    print("\n[Step 4] Simulating TTS playback completion (BotStoppedSpeakingFrame)...")
    await pipeline.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(0.05)
    print(f"  Checkpoints after speech end    : {continuity_processor.checkpoint_count}")

    # 8. Graceful shutdown with EndFrame
    print("\n[Step 5] Shutting down pipeline with EndFrame...")
    await pipeline.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)
    await continuity_processor.cleanup()
    print(f"  Total checkpoints recorded: {continuity_processor.checkpoint_count}")

    # 9. Verify zero frame loss
    print(f"  Total frames received at sink: {len(output_sink.received_frames)}")

    # 10. Simulate reconnect on a fresh worker instance
    print("\n" + "=" * 70)
    print("SIMULATING RECONNECT & CONTEXT RESTORATION")
    print("=" * 70)
    from unittest.mock import MagicMock, AsyncMock
    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()

    fresh_continuity = SessionContinuity(db_path=db_path)
    resumed_context = MockLLMContext()

    is_resumed, pending_tools = await fresh_continuity.resume_or_start(mock_task, resumed_context, session_id)
    print(f"  Resumed existing session? : {is_resumed}")
    print(f"  Restored message count   : {len(resumed_context.get_messages())}")
    for i, msg in enumerate(resumed_context.get_messages()):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        print(f"    [{i}] {role.upper()}: {content}")

    # Clean up demo database
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    print("\nPipeline FrameProcessor demonstration completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
