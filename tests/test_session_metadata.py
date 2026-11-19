import asyncio
import json
import os
import pytest
from unittest.mock import MagicMock, AsyncMock

from pipecat.clocks.system_clock import SystemClock
from pipecat.frames.frames import (
    EndFrame,
    LLMFullResponseEndFrame,
    StartFrame,
    TextFrame,
)
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


class MockContext:
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


class DummySinkProcessor(FrameProcessor):
    def __init__(self):
        super().__init__(enable_direct_mode=True)
        self.received_frames = []

    async def process_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
        await super().process_frame(frame, direction)
        self.received_frames.append(frame)
        await self.push_frame(frame, direction)


async def setup_test_processors(*processors):
    setup = FrameProcessorSetup(
        clock=SystemClock(),
        task_manager=TaskManager(),
        pipeline_worker=None,
    )
    for p in processors:
        await p.setup(setup)


@pytest.mark.asyncio
async def test_checkpoint_and_load_metadata():
    """Verify storing and loading custom business metadata alongside context."""
    db_path = "test_meta_basic.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Book an appointment for 3pm"}])
    session_id = "sess-meta-test-01"

    business_meta = {
        "user_id": "usr_99812",
        "verified": True,
        "current_step": "scheduling",
        "cart_total": 49.99,
    }

    await continuity.checkpoint(
        context=context,
        session_id=session_id,
        metadata=business_meta,
    )

    # 1. Verify load_context returns metadata
    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert loaded["metadata"] == business_meta
    assert loaded["metadata"]["user_id"] == "usr_99812"
    assert loaded["metadata"]["verified"] is True

    # 2. Verify get_metadata helper
    meta = await continuity.get_metadata(session_id)
    assert meta == business_meta

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_resume_or_start_with_return_metadata():
    """Verify resume_or_start returns 3-tuple (is_resumed, tools, metadata) when return_metadata=True."""
    db_path = "test_meta_resume.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([
        {"role": "user", "content": "What is my account balance?"},
        {"role": "assistant", "content": "Your balance is $450."},
    ])
    session_id = "sess-meta-resume-02"
    initial_meta = {"account_id": "acc_3311", "tier": "gold"}

    await continuity.checkpoint(context, session_id, metadata=initial_meta)

    # Fresh session resume
    fresh_continuity = SessionContinuity(db_path=db_path)
    resumed_context = MockContext()
    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()

    is_resumed, pending_tools, metadata = await fresh_continuity.resume_or_start(
        task=mock_task,
        context=resumed_context,
        session_id=session_id,
        return_metadata=True,
    )

    assert is_resumed is True
    assert len(resumed_context.get_messages()) == 2
    assert mock_task.queue_frames.called
    assert metadata == initial_meta
    assert metadata["account_id"] == "acc_3311"
    assert metadata["tier"] == "gold"

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_resume_or_start_backward_compatibility():
    """Verify that default resume_or_start continues to return 2-tuple (is_resumed, tools)."""
    db_path = "test_meta_compat.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Hello"}])
    session_id = "sess-meta-compat-03"

    await continuity.checkpoint(context, session_id, metadata={"tag": "vip"})

    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()
    resumed_context = MockContext()

    # Traditional 2-element unpack must not fail with ValueError
    is_resumed, pending_tools = await continuity.resume_or_start(
        mock_task,
        resumed_context,
        session_id,
    )

    assert is_resumed is True
    assert isinstance(pending_tools, dict)

    # Metadata can still be retrieved via get_metadata
    meta = await continuity.get_metadata(session_id)
    assert meta == {"tag": "vip"}

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_backward_compat_legacy_session_without_metadata():
    """Verify loading older sessions stored before metadata feature was introduced."""
    db_path = "test_meta_legacy.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    session_id = "sess-legacy-no-meta"

    # Save raw JSON without 'metadata' key
    raw_payload = json.dumps({
        "messages": [{"role": "user", "content": "legacy turn"}],
        "pending_tool_calls": {},
        "updated_at": 1700000000.0,
    })
    key = continuity.manager._get_key(session_id)
    await continuity.manager.storage.save(key, raw_payload, 3600)

    # 1. load_context defaults metadata to {}
    loaded = await continuity.manager.load_context(session_id)
    assert loaded is not None
    assert loaded["metadata"] == {}

    # 2. get_metadata returns empty dict
    meta = await continuity.get_metadata(session_id)
    assert meta == {}

    # 3. resume_or_start with return_metadata=True returns empty dict
    resumed_context = MockContext()
    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()

    is_resumed, tools, meta = await continuity.resume_or_start(
        mock_task,
        resumed_context,
        session_id,
        return_metadata=True,
    )
    assert is_resumed is True
    assert meta == {}

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_set_metadata_incremental_update():
    """Verify set_metadata merges and updates keys without modifying messages."""
    db_path = "test_meta_update.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Turn 1"}])
    session_id = "sess-meta-update-04"

    await continuity.checkpoint(context, session_id, metadata={"step": "step_1", "retries": 0})

    # Update metadata
    await continuity.set_metadata(session_id, {"step": "step_2", "lead_score": 85})

    meta = await continuity.get_metadata(session_id)
    assert meta["step"] == "step_2"
    assert meta["retries"] == 0
    assert meta["lead_score"] == 85

    # Messages are untouched
    loaded = await continuity.manager.load_context(session_id)
    assert len(loaded["messages"]) == 1

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_processor_with_static_metadata():
    """Verify SessionContinuityProcessor persists static metadata on frame events."""
    db_path = "test_meta_proc_static.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Checkout now"}])
    session_id = "sess-meta-proc-static"
    processor_meta = {"caller_channel": "sip", "caller_id": "+15550199"}

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        metadata=processor_meta,
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)
    await processor.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    await processor.cleanup()
    await sink.cleanup()

    meta = await continuity.get_metadata(session_id)
    assert meta == processor_meta

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_processor_with_dynamic_metadata_callback():
    """Verify SessionContinuityProcessor invokes dynamic get_metadata_fn on every checkpoint."""
    db_path = "test_meta_proc_dynamic.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    continuity = SessionContinuity(db_path=db_path)
    context = MockContext([{"role": "user", "content": "Step 1"}])
    session_id = "sess-meta-proc-dynamic"

    bot_dialog_state = {
        "step": "collect_address",
        "collected_fields": ["name", "email"],
    }

    processor = continuity.processor(
        session_id=session_id,
        context=context,
        get_metadata_fn=lambda: dict(bot_dialog_state),
    )
    sink = DummySinkProcessor()
    processor.link(sink)
    await setup_test_processors(processor, sink)

    await processor.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)
    await processor.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    await processor.cleanup()

    # First checkpoint verified
    meta1 = await continuity.get_metadata(session_id)
    assert meta1["step"] == "collect_address"

    # Dialog state evolves in subsequent turn
    bot_dialog_state["step"] = "confirm_order"
    bot_dialog_state["collected_fields"].append("address")

    await processor.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    await processor.cleanup()
    await sink.cleanup()

    meta2 = await continuity.get_metadata(session_id)
    assert meta2["step"] == "confirm_order"
    assert "address" in meta2["collected_fields"]

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass
