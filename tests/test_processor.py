import asyncio
import os
import pytest
from unittest.mock import MagicMock, AsyncMock

from pipecat.clocks.system_clock import SystemClock
from pipecat.frames.frames import (
    TextFrame,
    EndFrame,
    LLMFullResponseEndFrame,
    BotStoppedSpeakingFrame,
    FunctionCallResultFrame,
    StartFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.frame_processor import (
    FrameDirection,
    FrameProcessor,
    FrameProcessorSetup,
)
from pipecat.utils.asyncio.task_manager import TaskManager

from pipecat_session_continuity import (
    SessionContinuity,
    SessionContinuityProcessor,
)


class DummySinkProcessor(FrameProcessor):
    """Simple downstream processor to collect frames pushed by SessionContinuityProcessor."""
    def __init__(self):
        super().__init__(enable_direct_mode=True)
        self.received_frames = []


    async def process_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
        await super().process_frame(frame, direction)
        self.received_frames.append(frame)
        await self.push_frame(frame, direction)


class MockContext:
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


async def setup_test_processors(*processors):
    """Wires test processors with a TaskManager and SystemClock as Pipecat pipelines do."""
    setup = FrameProcessorSetup(
        clock=SystemClock(),
        task_manager=TaskManager(),
        pipeline_worker=None,
    )
    for p in processors:
        await p.setup(setup)


@pytest.mark.asyncio
async def test_processor_factory_and_initialization():
    continuity = SessionContinuity()
    context = MockContext([{"role": "user", "content": "hello"}])
    session_id = "test-processor-init"

    proc = continuity.processor(
        session_id=session_id,
        context=context,
        pending_tool_calls={"call_1": {"status": "pending"}},
    )

    assert isinstance(proc, SessionContinuityProcessor)
    assert proc.session_id == session_id
    assert proc.context == context
    assert proc.checkpoint_count == 0


@pytest.mark.asyncio
async def test_frame_passthrough_zero_loss():
    """Verify that all frames pass through the processor downstream unmodified."""
    continuity = SessionContinuity()
    context = MockContext()
    session_id = "test-passthrough"

    processor = continuity.processor(session_id=session_id, context=context)
    sink = DummySinkProcessor()

    # Link processor -> sink
    processor.link(sink)
    await setup_test_processors(processor, sink)

    # Initialize processors with StartFrame
    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    # Push frames
    f1 = TextFrame("Hello world")
    f2 = TextFrame("Second message")

    await processor.process_frame(f1, FrameDirection.DOWNSTREAM)
    await processor.process_frame(f2, FrameDirection.DOWNSTREAM)

    # Sink received StartFrame + f1 + f2
    assert len(sink.received_frames) == 3
    assert sink.received_frames[1] == f1
    assert sink.received_frames[2] == f2
    # Non-turn frames should not trigger checkpoints
    assert processor.checkpoint_count == 0

    await processor.cleanup()
    await sink.cleanup()


@pytest.mark.asyncio
async def test_checkpoint_on_llm_full_response_end():
    """Verify that LLMFullResponseEndFrame triggers context checkpointing."""
    db_path = "test_processor_llm_end.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "What is the weather?"}])
    session_id = "sess-llm-end"

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        checkpoint_on_llm_end=True,
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    # Start pipeline
    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    llm_end_frame = LLMFullResponseEndFrame()
    await processor.process_frame(llm_end_frame, FrameDirection.DOWNSTREAM)

    # Frame passed through to sink
    assert len(sink.received_frames) == 2
    assert isinstance(sink.received_frames[1], LLMFullResponseEndFrame)

    # Wait for non-blocking task to complete
    await processor.cleanup()
    await sink.cleanup()
    assert processor.checkpoint_count == 1

    # Verify context was persisted
    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert loaded["messages"] == context.get_messages()

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_checkpoint_on_bot_stopped_speaking():
    """Verify that BotStoppedSpeakingFrame triggers context checkpointing."""
    db_path = "test_processor_bot_stopped.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello there!"},
    ])
    session_id = "sess-bot-stopped"

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        checkpoint_on_bot_stopped=True,
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    bot_stopped_frame = BotStoppedSpeakingFrame()
    await processor.process_frame(bot_stopped_frame, FrameDirection.DOWNSTREAM)

    assert len(sink.received_frames) == 2
    await processor.cleanup()
    await sink.cleanup()
    assert processor.checkpoint_count == 1

    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert len(loaded["messages"]) == 2

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_checkpoint_on_function_result():
    """Verify that FunctionCallResultFrame triggers context checkpointing with updated tools."""
    db_path = "test_processor_func_result.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Book dinner"}])
    session_id = "sess-func-result"
    tools = {"call_1": {"tool_name": "book_dinner", "status": "pending"}}

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        pending_tool_calls=tools,
        checkpoint_on_function_result=True,
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    func_frame = FunctionCallResultFrame(
        function_name="book_dinner",
        tool_call_id="call_1",
        arguments={},
        result="Table confirmed",
    )
    # Update tools to completed before frame
    tools["call_1"]["status"] = "completed"
    tools["call_1"]["result"] = "Table confirmed"

    await processor.process_frame(func_frame, FrameDirection.DOWNSTREAM)
    await processor.cleanup()
    await sink.cleanup()

    assert processor.checkpoint_count == 1
    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert loaded["pending_tool_calls"]["call_1"]["status"] == "completed"

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_checkpoint_on_end_frame():
    """Verify that EndFrame forces a commit before teardown."""
    db_path = "test_processor_end_frame.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Goodbye"}])
    session_id = "sess-end-frame"

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        checkpoint_on_end_frame=True,
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)

    end_frame = EndFrame()
    await processor.process_frame(end_frame, FrameDirection.DOWNSTREAM)

    assert len(sink.received_frames) == 2
    assert processor.checkpoint_count == 1

    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert loaded["messages"][0]["content"] == "Goodbye"

    await processor.cleanup()
    await sink.cleanup()

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_pipeline_integration_with_pipeline_class():
    """Verify that SessionContinuityProcessor works inside a Pipecat Pipeline."""
    continuity = SessionContinuity()
    context = MockContext([{"role": "system", "content": "You are a helpful assistant."}])
    session_id = "sess-pipeline-class"

    processor = continuity.processor(session_id=session_id, context=context)
    sink = DummySinkProcessor()

    pipeline = Pipeline([processor, sink])
    task = PipelineTask(pipeline)
    assert task is not None
