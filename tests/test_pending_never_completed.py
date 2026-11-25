"""
Drop BEFORE the commit: the side effect never happened, the record stays "pending",
and nothing in the library ever resolves it.

These tests pin down, deterministically and with no LLM, why the Phase 1b
`--first-booking not_committed` arm is predicted to lose the booking: the guard
blocks the identical retry, and a pending record never expires or resolves on its
own. The only things that unblock it are a status-check hook that proves the
action did not happen (retry_on_not_found=True), or the whole session expiring
from storage (which also throws away the conversation).
"""
import asyncio
import os
import sys
import time

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


async def drop_then_resume(tmp_path, ttl_seconds=3600, hook=None, retry_on_not_found=False):
    continuity = SessionContinuity(db_path=str(tmp_path / "sc.db"), secret="s", ttl_seconds=ttl_seconds)
    if hook:
        continuity.register_status_check("book_appointment", hook, retry_on_not_found=retry_on_not_found)
    ledger, pending = Ledger(), {}

    assert call_tool(continuity, pending, ledger, ARGS, "call_1", drop_before_commit=True) == "dropped"
    await continuity.checkpoint(MockContext(), "sess", pending)
    return continuity, ledger


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
async def test_pending_record_never_expires_or_resolves_on_its_own(tmp_path):
    continuity, ledger = await drop_then_resume(tmp_path)

    # Age the record by 30 days. created_at/updated_at are stored but nothing reads them.
    data = await continuity.manager.load_context("sess")
    tools = data["pending_tool_calls"]
    for r in tools.values():
        r["created_at"] = r["updated_at"] = time.time() - 30 * 86400
    await continuity.manager.save_context("sess", data["messages"], tools)

    for attempt in range(5):  # five more reconnects
        resumed, pending, _ = await resume(continuity)
        assert resumed
        (rec,) = IdempotencyRegistry(pending).records.values()
        assert rec["status"] == "pending"
        assert call_tool(continuity, pending, ledger, ARGS, f"call_r{attempt}") == "blocked"
        await continuity.checkpoint(MockContext(), "sess", pending)  # also refreshes the session TTL

    assert ledger.bookings == []


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
