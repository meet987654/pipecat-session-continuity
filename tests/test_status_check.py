import asyncio
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity, RETRY_ALLOWED


class MockContext:
    def __init__(self):
        self._messages = []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


class MockTask:
    def __init__(self):
        self.queued = []

    async def queue_frames(self, frames):
        self.queued.extend(frames)


ARGS = {"details": "Dentist at 10AM"}


async def _pending_session(tmp_path, **kwargs):
    continuity = SessionContinuity(db_path=str(tmp_path / "sc.db"), secret="s", **kwargs)
    registry = IdempotencyRegistry()
    registry.register_call("book_appointment", ARGS, tool_call_id="call_1")
    await continuity.manager.save_context("sess", [], registry.to_dict())
    key = next(iter(registry.records))
    return continuity, key


async def _resume(continuity):
    ctx = MockContext()
    resumed, tools = await continuity.resume_or_start(MockTask(), ctx, "sess")
    assert resumed
    notices = [m["content"] for m in ctx.get_messages() if m["role"] == "system"]
    return IdempotencyRegistry(tools), notices


@pytest.mark.asyncio
async def test_no_hook_behaviour_unchanged(tmp_path):
    continuity, key = await _pending_session(tmp_path)
    registry, notices = await _resume(continuity)
    assert registry.records[key]["status"] == "pending"
    assert len(notices) == 1 and "unconfirmed" in notices[0] and "Do NOT call this tool again" in notices[0]


@pytest.mark.asyncio
async def test_hook_for_other_tool_is_not_called(tmp_path):
    continuity, key = await _pending_session(tmp_path)
    called = []

    async def hook(k, a):
        called.append(k)
        return "not_found"

    continuity.register_status_check("some_other_tool", hook)
    registry, notices = await _resume(continuity)
    assert called == []
    assert registry.records[key]["status"] == "pending"
    assert "unconfirmed" in notices[0]


@pytest.mark.asyncio
async def test_completed_marks_completed_and_states_result(tmp_path):
    continuity, key = await _pending_session(tmp_path)
    seen = {}

    async def hook(k, a):
        seen["key"], seen["args"] = k, a
        return ("completed", "apt_42")

    continuity.register_status_check("book_appointment", hook)
    registry, notices = await _resume(continuity)

    assert seen == {"key": key, "args": ARGS}, "hook gets the idempotency key and args"
    rec = registry.records[key]
    assert rec["status"] == "completed" and rec["result"] == "apt_42"
    assert len(notices) == 1
    assert "apt_42" in notices[0] and "confirmed" in notices[0].lower()
    assert "unconfirmed" not in notices[0]


@pytest.mark.asyncio
async def test_completed_dict_form(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        return {"status": "completed", "result": {"id": "apt_7"}}

    continuity.register_status_check("book_appointment", hook)
    registry, _ = await _resume(continuity)
    assert registry.records[key]["result"] == {"id": "apt_7"}


@pytest.mark.asyncio
async def test_not_found_allows_exactly_one_retry_with_same_key(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        return "not_found"

    continuity.register_status_check("book_appointment", hook, retry_on_not_found=True)
    registry, notices = await _resume(continuity)

    assert registry.records[key]["status"] == RETRY_ALLOWED
    assert "did NOT happen" in notices[0] and key in notices[0]

    # First re-call (new tool_call_id): allowed, same key, now pending again.
    is_dup, entry = continuity.check_tool_idempotency(registry, "book_appointment", ARGS, tool_call_id="call_2")
    assert is_dup and entry["status"] == RETRY_ALLOWED
    rec = continuity.record_tool_call(registry, "book_appointment", ARGS, tool_call_id="call_2")
    assert rec["idempotency_key"] == key and rec["status"] == "pending" and rec["attempts"] == 2

    # Second re-call: blocked, no second retry.
    is_dup, entry = continuity.check_tool_idempotency(registry, "book_appointment", ARGS, tool_call_id="call_3")
    assert is_dup and entry["status"] == "pending" and entry["attempts"] == 2


@pytest.mark.asyncio
async def test_not_found_without_opt_in_is_treated_as_unknown(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        return "not_found"

    continuity.register_status_check("book_appointment", hook)  # retry_on_not_found defaults to False
    registry, notices = await _resume(continuity)

    assert registry.records[key]["status"] == "pending"
    assert len(notices) == 1 and "could not determine" in notices[0] and "did NOT happen" not in notices[0]
    # A re-call is blocked: no retry was granted.
    is_dup, entry = continuity.check_tool_idempotency(registry, "book_appointment", ARGS, tool_call_id="call_2")
    assert is_dup and entry["status"] == "pending"


@pytest.mark.asyncio
async def test_unknown_gives_inconclusive_notice(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        return "unknown"

    continuity.register_status_check("book_appointment", hook)
    registry, notices = await _resume(continuity)
    assert registry.records[key]["status"] == "pending"
    assert "could not determine" in notices[0]


@pytest.mark.asyncio
async def test_hook_timeout_gives_inconclusive_notice(tmp_path):
    continuity, key = await _pending_session(tmp_path, status_check_timeout_seconds=0.05)

    async def hook(k, a):
        await asyncio.sleep(5)
        return ("completed", "too_late")

    continuity.register_status_check("book_appointment", hook)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    registry, notices = await _resume(continuity)
    assert loop.time() - t0 < 1.0, "must not wait for the slow hook"
    assert registry.records[key]["status"] == "pending"
    assert "could not determine" in notices[0] and "too_late" not in notices[0]


@pytest.mark.asyncio
async def test_default_timeout_is_one_second(tmp_path):
    continuity = SessionContinuity(db_path=str(tmp_path / "d.db"), secret="s")
    assert continuity.status_check_timeout_seconds == 1.0


@pytest.mark.asyncio
async def test_hook_raises_gives_inconclusive_notice(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        raise RuntimeError("booking API is down")

    continuity.register_status_check("book_appointment", hook)
    registry, notices = await _resume(continuity)
    assert registry.records[key]["status"] == "pending"
    assert "could not determine" in notices[0]


@pytest.mark.asyncio
async def test_garbage_return_value_is_unknown(tmp_path):
    continuity, key = await _pending_session(tmp_path)

    async def hook(k, a):
        return 12345

    continuity.register_status_check("book_appointment", hook)
    registry, notices = await _resume(continuity)
    assert registry.records[key]["status"] == "pending"
    assert "could not determine" in notices[0]
