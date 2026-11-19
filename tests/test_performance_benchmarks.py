import asyncio
import time
import pytest
from pipecat.clocks.system_clock import SystemClock
from pipecat.frames.frames import (
    OutputAudioRawFrame,
    LLMFullResponseEndFrame,
    BotStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.processors.frame_processor import (
    FrameDirection,
    FrameProcessor,
    FrameProcessorSetup,
)
from pipecat.utils.asyncio.task_manager import TaskManager

from pipecat_session_continuity import (
    SessionContinuity,
    SessionContinuityProcessor,
    SQLiteStorage,
)


class TimestampRecorder(FrameProcessor):
    def __init__(self, **kwargs):
        super().__init__(enable_direct_mode=True, **kwargs)
        self.timestamps = []

    async def process_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
        await super().process_frame(frame, direction)
        self.timestamps.append(time.perf_counter())
        await self.push_frame(frame, direction)


async def setup_test_processors(*processors):
    setup = FrameProcessorSetup(
        clock=SystemClock(),
        task_manager=TaskManager(),
        pipeline_worker=None,
    )
    for p in processors:
        await p.setup(setup)


class MockContext:
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


@pytest.mark.asyncio
async def test_audio_frame_passthrough_latency():
    """Verify that audio frame passthrough latency through SessionContinuityProcessor

    is under 1.0 millisecond (<1000 microseconds) per frame.
    """
    storage = SQLiteStorage(":memory:")
    continuity = SessionContinuity(storage_backend=storage)
    context = MockContext()

    processor = continuity.processor(
        session_id="perf_audio_test",
        context=context,
        enable_direct_mode=True,
    )
    recorder = TimestampRecorder()
    pipeline = Pipeline([processor, recorder])
    await setup_test_processors(processor, recorder, pipeline)

    # 20ms of 16kHz 16-bit mono PCM = 640 bytes
    audio_frame = OutputAudioRawFrame(
        audio=b"\x00" * 640,
        sample_rate=16000,
        num_channels=1,
    )

    # Send StartFrame to initialize the pipeline
    from pipecat.frames.frames import StartFrame
    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)
    recorder.timestamps.clear()

    # Warmup
    for _ in range(5):
        await processor.process_frame(audio_frame, FrameDirection.DOWNSTREAM)
    recorder.timestamps.clear()

    # Measure passthrough latency
    latencies = []
    iterations = 50
    for _ in range(iterations):
        t_before = time.perf_counter()
        await processor.process_frame(audio_frame, FrameDirection.DOWNSTREAM)
        t_after = recorder.timestamps[-1]
        latencies.append((t_after - t_before) * 1000)  # ms

    avg_latency = sum(latencies) / len(latencies)
    p95_latency = sorted(latencies)[int(0.95 * len(latencies))]

    assert avg_latency < 1.0, f"Average audio frame passthrough {avg_latency:.4f}ms exceeds 1.0ms limit"
    assert p95_latency < 2.0, f"P95 audio frame passthrough {p95_latency:.4f}ms exceeds 2.0ms limit"
    storage.close()


@pytest.mark.asyncio
async def test_completion_frame_dispatch_non_blocking():
    """Verify that dispatching completion frames (LLMFullResponseEndFrame, BotStoppedSpeakingFrame)

    returns synchronously in < 5.0ms without blocking the event loop on storage I/O.
    """
    storage = SQLiteStorage(":memory:")
    continuity = SessionContinuity(storage_backend=storage)
    context = MockContext([{"role": "user", "content": "Hello"}])

    processor = continuity.processor(
        session_id="perf_nonblocking_test",
        context=context,
        checkpoint_on_full_response=True,
        enable_direct_mode=True,
    )
    recorder = TimestampRecorder()
    pipeline = Pipeline([processor, recorder])
    await setup_test_processors(processor, recorder, pipeline)

    # Frame dispatch measurement
    t_start = time.perf_counter()
    await processor.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    dispatch_duration_ms = (time.perf_counter() - t_start) * 1000

    # Dispatch must be immediate (< 5ms) because checkpoint task is offloaded
    assert dispatch_duration_ms < 5.0, (
        f"Completion frame dispatch took {dispatch_duration_ms:.2f}ms; expected < 5.0ms"
    )

    # Allow background checkpoint task to finish
    await asyncio.sleep(0.05)

    loaded = await continuity.manager.load_context("perf_nonblocking_test")
    assert loaded is not None
    assert loaded["messages"][0]["content"] == "Hello"
    storage.close()


@pytest.mark.asyncio
async def test_concurrent_audio_streaming_during_checkpoint():
    """Verify that an ongoing background checkpoint write does not starve or block

    concurrent audio frame streaming.
    """
    storage = SQLiteStorage(":memory:")
    continuity = SessionContinuity(storage_backend=storage)
    context = MockContext([{"role": "assistant", "content": "Simulated long response" * 50}])

    processor = continuity.processor(
        session_id="perf_concurrency_test",
        context=context,
        checkpoint_on_bot_stopped=True,
        enable_direct_mode=True,
    )
    recorder = TimestampRecorder()
    pipeline = Pipeline([processor, recorder])
    await setup_test_processors(processor, recorder, pipeline)

    audio_frame = OutputAudioRawFrame(
        audio=b"\x00" * 640,
        sample_rate=16000,
        num_channels=1,
    )

    # Start pipeline
    from pipecat.frames.frames import StartFrame
    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    # Trigger background checkpoint
    await processor.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    # Concurrently stream audio frames while checkpoint write is running
    stream_latencies = []
    for _ in range(20):
        t0 = time.perf_counter()
        await processor.process_frame(audio_frame, FrameDirection.DOWNSTREAM)
        stream_latencies.append((time.perf_counter() - t0) * 1000)

    # Verify no audio frame suffered starvation (> 5ms)
    max_frame_latency = max(stream_latencies)
    assert max_frame_latency < 5.0, (
        f"Audio frame suffered jitter spike of {max_frame_latency:.2f}ms during background checkpoint"
    )

    await asyncio.sleep(0.05)
    storage.close()


@pytest.mark.asyncio
async def test_ttfa_resume_latency_budget(tmp_path):
    """Verify that resume_or_start latency stays well within the real-time budget (< 50ms)

    even on a disk-based SQLite storage backend with WAL mode.
    """
    db_file = tmp_path / "bench_perf.db"
    storage = SQLiteStorage(str(db_file))
    continuity = SessionContinuity(storage_backend=storage)

    # Seed session
    session_id = "ttfa_perf_test"
    seed_context = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Book me a table for 7pm."},
        {"role": "assistant", "content": "Booking confirmed for 7pm."},
    ]
    await continuity.manager.save_context(session_id, seed_context)

    from unittest.mock import MagicMock, AsyncMock
    # Measure resume latency
    latencies = []
    for _ in range(20):
        mock_ctx = MockContext()
        mock_task = MagicMock()
        mock_task.queue_frames = AsyncMock()
        t0 = time.perf_counter()
        is_resumed, pending_tools = await continuity.resume_or_start(mock_task, mock_ctx, session_id)
        latencies.append((time.perf_counter() - t0) * 1000)
        assert is_resumed is True

    avg_resume = sum(latencies) / len(latencies)
    assert avg_resume < 50.0, f"Average resume latency {avg_resume:.2f}ms exceeded 50ms budget"
    storage.close()
