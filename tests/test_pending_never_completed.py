"""
Drop BEFORE the commit: the side effect never happened and the record stays "pending".

Default (0.3.0 behaviour, kept for compatibility): nothing resolves it. The guard blocks
every identical retry, which is why the Phase 1b `--first-booking not_committed` arm
loses the booking. A warning is logged the first time such a tool is guarded.

With a per-tool pending timeout: once it passes, the record is first given to the
tool's status check if one is registered; otherwise (or if the check cannot answer) it
becomes UNRESOLVED, the resume notice asks the caller whether to retry the same request,
and the guard lets one identical retry through.
"""
import logging
import asyncio
import os
import sys
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity, RETRY_ALLOWED, UNRESOLVED


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
    async def queue_frames(self, frames):
        pass


class Ledger:
    """The backend. Counts bookings that actually committed."""

    def __init__(self):
        self.bookings = []

    def book(self, args):
        self.bookings.append(args)
        return {"status": "confirmed", **args}


ARGS = {"name": "Priya Shah", "day": "Thursday", "time": "15:00"}


def call_tool(continuity, pending, ledger, args, call_id, drop_before_commit=False):
    """The app-side handler pattern from the Phase 1b spike and example 06.
    Returns "blocked", "dropped" or "booked"."""
    is_dup, rec = continuity.check_tool_idempotency(pending, "book_appointment", arguments=args)
    if is_dup and rec.get("status") == RETRY_ALLOWED:
        is_dup = False  # the one permitted retry
    if is_dup:
        return "blocked"
    continuity.record_tool_call(pending, "book_appointment", arguments=args, tool_call_id=call_id)
    if drop_before_commit:
        return "dropped"  # connection lost before the backend wrote anything
    result = ledger.book(args)
    continuity.complete_tool_call(pending, call_id, result)
    return "booked"


async def drop_then_resume(tmp_path, ttl_seconds=3600, hook=None, retry_on_not_found=False, pending_timeouts=None):
    continuity = SessionContinuity(db_path=str(tmp_path / "sc.db"), secret="s", ttl_seconds=ttl_seconds,
                                   pending_timeouts=pending_timeouts)
    if hook:
        continuity.register_status_check("book_appointment", hook, retry_on_not_found=retry_on_not_found)
    ledger, pending = Ledger(), {}

    assert call_tool(continuity, pending, ledger, ARGS, "call_1", drop_before_commit=True) == "dropped"
    await continuity.checkpoint(MockContext(), "sess", pending)
    return continuity, ledger


async def age_records(continuity, seconds):
    """created_at/updated_at are what the pending timeout is measured from."""
    data = await continuity.manager.load_context("sess")
    tools = data["pending_tool_calls"]
    for r in tools.values():
        r["created_at"] = r["updated_at"] = time.time() - seconds
    await continuity.manager.save_context("sess", data["messages"], tools)


def notices(ctx):
    return [m["content"] for m in ctx.get_messages() if m["role"] == "system"]


async def resume(continuity):
    ctx = MockContext()
    resumed, restored = await continuity.resume_or_start(MockTask(), ctx, "sess")
    pending = {}
    pending.update(restored)
    return resumed, pending, ctx


@pytest.mark.asyncio
async def test_identical_retry_after_drop_before_commit_is_blocked_and_booking_is_lost(tmp_path):
    continuity, ledger = await drop_then_resume(tmp_path)
    resumed, pending, _ = await resume(continuity)
    assert resumed

    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == "pending"
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "blocked"
    assert call_tool(continuity, pending, ledger, ARGS, "call_3") == "blocked"
    assert ledger.bookings == []  # loss: the caller asked for it, the backend never got it


@pytest.mark.asyncio
async def test_default_still_blocks_a_30_day_old_record_and_warns(tmp_path, caplog):
    """Backward compatible: with no pending timeout, nothing changes, but it is no longer silent."""
    with caplog.at_level(logging.WARNING, logger="pipecat_session_continuity"):
        continuity, ledger = await drop_then_resume(tmp_path)
    assert any("no status check and no pending timeout" in m for m in caplog.messages)
    await age_records(continuity, 30 * 86400)

    for attempt in range(5):  # five more reconnects
        resumed, pending, _ = await resume(continuity)
        assert resumed
        (rec,) = IdempotencyRegistry(pending).records.values()
        assert rec["status"] == "pending"
        assert call_tool(continuity, pending, ledger, ARGS, f"call_r{attempt}") == "blocked"
        await continuity.checkpoint(MockContext(), "sess", pending)
    assert ledger.bookings == []


@pytest.mark.asyncio
async def test_30_day_old_record_without_status_check_escalates_to_unresolved(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="pipecat_session_continuity"):
        continuity, ledger = await drop_then_resume(tmp_path, pending_timeouts={"book_appointment": 30})
    assert not any("no status check and no pending timeout" in m for m in caplog.messages)
    assert any("has no status check" in m and "caller's decision" in m for m in caplog.messages)
    await age_records(continuity, 30 * 86400)

    _, pending, ctx = await resume(continuity)
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == UNRESOLVED and rec["unresolved_reason"] == "no_status_check"

    # A decision for the caller, not a block: the identical retry goes through once.
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "booked"
    assert call_tool(continuity, pending, ledger, ARGS, "call_3") == "blocked"
    assert ledger.bookings == [ARGS]
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == "completed" and rec["attempts"] == 2


@pytest.mark.asyncio
async def test_unresolved_notice_asks_the_caller_and_does_not_forbid_the_retry(tmp_path):
    continuity, _ = await drop_then_resume(tmp_path, pending_timeouts={"book_appointment": 0})
    resumed, _, ctx = await resume(continuity)
    (notice,) = notices(ctx)
    assert "Do NOT call this tool again" not in notice
    assert "outcome is unknown" in notice
    assert "try the same request again with the same details" in notice
    assert "do not offer a different time" in notice
    # The final bridge message is queued as a frame, not added to the context: capture it.
    class Capture:
        frames = []

        async def queue_frames(self, frames):
            Capture.frames.extend(frames)

    ctx2 = MockContext()
    await continuity.resume_or_start(Capture(), ctx2, "sess")
    final = Capture.frames[-1].messages[-1]["content"]
    assert "try the same request again" in final
    assert "Do not call the tool again" not in final


@pytest.mark.asyncio
async def test_young_pending_record_is_not_escalated(tmp_path):
    """The timeout is measured from the last attempt: a write that may still land is left alone."""
    continuity, ledger = await drop_then_resume(tmp_path, pending_timeouts={"book_appointment": 3600})
    _, pending, _ = await resume(continuity)
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == "pending"
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "blocked"


@pytest.mark.asyncio
async def test_timed_out_record_is_given_to_the_status_check_first(tmp_path):
    calls = []

    async def lookup(key, args):
        calls.append(key)
        return ("completed", {"status": "confirmed"})

    continuity, ledger = await drop_then_resume(tmp_path, hook=lookup, pending_timeouts={"book_appointment": 0})
    _, pending, _ = await resume(continuity)
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert calls and rec["status"] == "completed"
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "blocked"  # done: no retry
    assert ledger.bookings == []


@pytest.mark.asyncio
async def test_timed_out_record_with_inconclusive_status_check_escalates(tmp_path):
    async def lookup(key, args):
        return "unknown"

    continuity, ledger = await drop_then_resume(tmp_path, hook=lookup, pending_timeouts={"book_appointment": 0})
    _, pending, ctx = await resume(continuity)
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == UNRESOLVED and rec["unresolved_reason"] == "status_check_inconclusive"
    assert "A status check could not tell either" in notices(ctx)[0]
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "booked"


@pytest.mark.asyncio
async def test_retry_with_different_arguments_is_not_caught(tmp_path):
    """Open scope question, not a bug fix here: another slot gets a new key."""
    continuity, ledger = await drop_then_resume(tmp_path)
    _, pending, _ = await resume(continuity)
    other = {**ARGS, "time": "16:00"}
    assert call_tool(continuity, pending, ledger, other, "call_2") == "booked"
    assert ledger.bookings == [other]


@pytest.mark.asyncio
async def test_not_found_hook_with_retry_allows_exactly_one_identical_retry(tmp_path):
    async def lookup(key, args):
        return "not_found"

    continuity, ledger = await drop_then_resume(tmp_path, hook=lookup, retry_on_not_found=True)
    _, pending, _ = await resume(continuity)
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "booked"
    assert call_tool(continuity, pending, ledger, ARGS, "call_3") == "blocked"
    assert ledger.bookings == [ARGS]


@pytest.mark.asyncio
async def test_not_found_hook_without_retry_flag_still_blocks(tmp_path):
    async def lookup(key, args):
        return "not_found"

    continuity, ledger = await drop_then_resume(tmp_path, hook=lookup, retry_on_not_found=False)
    _, pending, _ = await resume(continuity)
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "blocked"
    assert ledger.bookings == []


@pytest.mark.asyncio
async def test_only_whole_session_expiry_unblocks_and_it_drops_the_conversation(tmp_path):
    continuity, ledger = await drop_then_resume(tmp_path, ttl_seconds=1)
    await asyncio.sleep(1.2)
    resumed, pending, _ = await resume(continuity)
    assert not resumed and pending == {}  # registry AND conversation are gone
    assert call_tool(continuity, pending, ledger, ARGS, "call_2") == "booked"


@pytest.mark.asyncio
async def test_retry_allowed_notice_does_not_ask_the_llm_to_pass_a_key(tmp_path):
    """Found live: told to 'pass idempotency key X', the model added an idempotency_key argument the
    tool does not take, and the call crashed. The app passes the key from the record, not the LLM."""
    async def lookup(key, args):
        return "not_found"

    continuity, _ = await drop_then_resume(tmp_path, hook=lookup, retry_on_not_found=True)
    _, _, ctx = await resume(continuity)
    (notice,) = notices(ctx)
    assert "idemp:" not in notice and "passing idempotency key" not in notice
    assert "Do not add any extra argument" in notice
