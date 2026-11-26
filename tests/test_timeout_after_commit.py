"""
The risk side of the pending timeout: the drop happens AFTER the backend committed.

With a timeout and no status check, the record becomes UNRESOLVED and the guard lets one
identical retry through. Whether that retry is harmless depends on the backend:

- it ignores the idempotency key: the retry books a second time (a duplicate). This is
  what the Phase 1b `--first-booking committed --pending-timeout 0` arm measured live.
- it honours the key: the retry returns the original booking and nothing new is written.

The retry must carry the SAME key as the first attempt, or no backend can de-duplicate it.
"""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity, UNRESOLVED


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


class Backend:
    """A booking API. With honour_keys it de-duplicates on the idempotency key, as Stripe-style APIs do."""

    def __init__(self, honour_keys):
        self.honour_keys = honour_keys
        self.ledger = []
        self.by_key = {}
        self.keys_seen = []

    def book(self, args, key):
        self.keys_seen.append(key)
        if self.honour_keys and key in self.by_key:
            return self.by_key[key]  # replay: same result, no new write
        self.ledger.append(dict(args))
        result = {"status": "booked", "confirmation": f"CONF-{len(self.ledger):03d}"}
        self.by_key[key] = result
        return result


ARGS = {"name": "Priya Shah", "day": "Thursday", "time": "15:00"}


def call_tool(continuity, pending, backend, call_id, drop_after_commit=False):
    """The app-side handler: guard, record, call the backend with the record's key, complete."""
    is_dup, rec = continuity.check_tool_idempotency(pending, "book_appointment", arguments=ARGS)
    if is_dup:
        return "blocked", None
    rec = continuity.record_tool_call(pending, "book_appointment", arguments=ARGS, tool_call_id=call_id)
    result = backend.book(ARGS, rec["idempotency_key"])
    if drop_after_commit:
        return "dropped", None  # committed, but the reply never reached the agent
    continuity.complete_tool_call(pending, call_id, result)
    return "booked", result


async def drop_after_commit_then_resume(tmp_path, backend):
    continuity = SessionContinuity(db_path=str(tmp_path / "sc.db"), secret="s",
                                   pending_timeouts={"book_appointment": 0})
    pending = {}
    assert call_tool(continuity, pending, backend, "call_1", drop_after_commit=True)[0] == "dropped"
    await continuity.checkpoint(MockContext(), "sess", pending)
    resumed, restored = await continuity.resume_or_start(MockTask(), MockContext(), "sess")
    assert resumed
    pending = dict(restored)
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == UNRESOLVED  # no status check: the caller decides
    return continuity, pending


@pytest.mark.asyncio
async def test_retry_after_commit_duplicates_when_backend_ignores_the_key(tmp_path):
    backend = Backend(honour_keys=False)
    continuity, pending = await drop_after_commit_then_resume(tmp_path, backend)

    status, result = call_tool(continuity, pending, backend, "call_2")
    assert status == "booked" and result["confirmation"] == "CONF-002"
    assert len(backend.ledger) == 2  # the duplicate the timeout fix trades for no loss
    assert call_tool(continuity, pending, backend, "call_3")[0] == "blocked"  # only one retry
    assert len(backend.ledger) == 2


@pytest.mark.asyncio
async def test_retry_after_commit_is_harmless_when_backend_honours_the_key(tmp_path):
    backend = Backend(honour_keys=True)
    continuity, pending = await drop_after_commit_then_resume(tmp_path, backend)

    status, result = call_tool(continuity, pending, backend, "call_2")
    assert status == "booked" and result["confirmation"] == "CONF-001"  # the original booking
    assert len(backend.ledger) == 1
    assert backend.keys_seen[0] == backend.keys_seen[1]  # the retry reused the first key
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == "completed" and rec["result"]["confirmation"] == "CONF-001"
