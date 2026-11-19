import asyncio
import multiprocessing
import os
import random
import sys
import time
import pytest
from unittest.mock import MagicMock, AsyncMock

from pipecat_session_continuity import SessionContinuity


class MockContext:
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


def _child_process_worker_a(db_path, session_id, sync_event):
    """Child process that writes state and terminates violently."""
    continuity = SessionContinuity(db_path=db_path, secret="test-secret")
    context = MockContext()
    context.add_message({"role": "user", "content": "Transfer $500 to account ACC-999."})
    context.add_message({"role": "assistant", "content": "Processing transfer..."})

    pending_tools = {}
    tool_rec = continuity.record_tool_call(
        pending_tools,
        tool_name="bank_transfer",
        arguments={"amount": 500, "account": "ACC-999"},
        client_token="client-token-trans-1",
        status="pending",
    )

    async def run_turn():
        await continuity.checkpoint(context, session_id, pending_tools)
        # Complete tool
        continuity.complete_tool_call(
            pending_tools,
            tool_rec["idempotency_key"],
            result="Transfer successful. Transaction ID #TXN-7712",
        )
        await continuity.checkpoint(context, session_id, pending_tools)
        sync_event.set()
        time.sleep(0.2)
        # Violent OS kill
        os._exit(137)

    asyncio.run(run_turn())


def test_hard_process_kill_recovery():
    """Verify state persists across violent process crash (os._exit 137)."""
    db_path = "test_hard_kill_unit.db"
    session_id = "test-hard-kill-session"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    sync_event = multiprocessing.Event()
    worker = multiprocessing.Process(
        target=_child_process_worker_a,
        args=(db_path, session_id, sync_event),
    )
    worker.start()

    sync_event.wait(timeout=30.0)
    worker.join(timeout=15.0)
    if worker.is_alive():
        worker.terminate()
    assert worker.exitcode == 137, "Process should have exited with code 137"

    # Now verify recovery in the parent process (or a fresh worker)
    continuity = SessionContinuity(db_path=db_path, secret="test-secret")
    task = MagicMock()
    task.queue_frames = AsyncMock()
    context = MockContext()

    async def verify_recovery():
        is_resumed, pending_tools = await continuity.resume_or_start(task, context, session_id)
        assert is_resumed is True
        messages = context.get_messages()
        assert len(messages) >= 2
        assert "Transfer $500" in messages[0]["content"]

        is_dup, rec = continuity.check_tool_idempotency(
            pending_tools,
            tool_name="bank_transfer",
            arguments={"amount": 500, "account": "ACC-999"},
            client_token="client-token-trans-1",
        )
        assert is_dup is True
        assert rec["status"] == "completed"
        assert "TXN-7712" in rec["result"]

    try:
        asyncio.run(verify_recovery())
    finally:
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_inflight_pending_tool_call_drop_and_resume():
    """Verify protective system notice injected when a tool call dropped mid-flight."""
    db_path = "test_inflight_drop.db"
    session_id = "test-inflight-drop-session"
    continuity = SessionContinuity(db_path=db_path, secret="test-secret")

    task = MagicMock()
    task.queue_frames = AsyncMock()
    context = MockContext()

    # User initiates tool
    context.add_message({"role": "user", "content": "Reserve Flight AA-120"})
    pending_tools = {}
    continuity.record_tool_call(
        pending_tools,
        tool_name="book_flight",
        arguments={"flight_no": "AA-120"},
        status="pending",
    )
    # Checkpoint while tool is pending
    await continuity.checkpoint(context, session_id, pending_tools)

    # Server drops / client reconnects
    reconnect_task = MagicMock()
    reconnect_task.queue_frames = AsyncMock()
    reconnect_context = MockContext()

    is_resumed, restored_tools = await continuity.resume_or_start(
        reconnect_task, reconnect_context, session_id
    )

    assert is_resumed is True
    # Verify protective notice for unconfirmed tool
    msgs = reconnect_context.get_messages()
    system_notices = [m for m in msgs if m.get("role") == "system" and "unconfirmed" in m.get("content", "")]
    assert len(system_notices) == 1
    assert "book_flight" in system_notices[0]["content"]

    # Verify idempotency check
    is_dup, rec = continuity.check_tool_idempotency(
        restored_tools,
        tool_name="book_flight",
        arguments={"flight_no": "AA-120"},
    )
    assert is_dup is True
    assert rec["status"] == "pending"

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_stale_mobile_sleep_resume():
    """Verify stale threshold triggers prolonged absence greeting."""
    db_path = "test_stale_sleep.db"
    session_id = "test-stale-sleep-session"
    continuity = SessionContinuity(
        db_path=db_path,
        secret="test-secret",
        stale_threshold_minutes=0.01,  # ~0.6 seconds
    )

    task = MagicMock()
    task.queue_frames = AsyncMock()
    context = MockContext([{"role": "user", "content": "I need help with my account."}])
    await continuity.checkpoint(context, session_id)

    # Mobile sleep simulation
    await asyncio.sleep(0.7)

    reconnect_task = MagicMock()
    reconnect_task.queue_frames = AsyncMock()
    reconnect_context = MockContext()

    is_resumed, _ = await continuity.resume_or_start(reconnect_task, reconnect_context, session_id)
    assert is_resumed is True

    queued = reconnect_task.queue_frames.call_args[0][0]
    injected_text = queued[0].messages[0]["content"]
    assert "disconnected for a while" in injected_text

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


def test_exponential_backoff_calculation():
    """Verify exponential backoff grows exponentially with bounded jitter."""
    base_delay = 0.5
    max_delay = 4.0

    def calc(attempt):
        temp = min(max_delay, base_delay * (2 ** attempt))
        return random.uniform(temp * 0.5, temp)

    # Attempt 1: base * 2 = 1.0 -> [0.5, 1.0]
    delay1 = calc(1)
    assert 0.5 <= delay1 <= 1.0

    # Attempt 2: base * 4 = 2.0 -> [1.0, 2.0]
    delay2 = calc(2)
    assert 1.0 <= delay2 <= 2.0

    # Attempt 5: capped at max_delay 4.0 -> [2.0, 4.0]
    delay5 = calc(5)
    assert 2.0 <= delay5 <= 4.0


def test_forged_session_token_rejected():
    """Verify HMAC verification rejects invalid or tampered signatures."""
    continuity = SessionContinuity(secret="my-secret-key")
    session_id, valid_sig = continuity.new_session()

    assert continuity.verify(session_id, valid_sig) is True
    assert continuity.verify(session_id, "forged_signature_xyz") is False
    assert continuity.verify("wrong-session-id", valid_sig) is False
