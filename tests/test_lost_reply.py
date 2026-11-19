"""
Lost-reply scenario: the booking reaches the server and is recorded, but the
reply is dropped before complete_call() runs. After save -> restore the record
must still be pending, a re-call with a NEW tool_call_id must match the SAME
record, and the server must still hold exactly one booking.
"""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity


class FakeBookingServer:
    """Counts bookings. Does NOT dedupe: the library alone must prevent the second one."""

    def __init__(self):
        self.bookings = []

    async def book(self, details: str) -> str:
        self.bookings.append(details)
        return f"apt_{len(self.bookings)}"


class ReplyDropped(ConnectionError):
    pass


@pytest.mark.asyncio
async def test_lost_reply_does_not_double_book(tmp_path):
    continuity = SessionContinuity(db_path=str(tmp_path / "lost_reply.db"), secret="s")
    session_id = "lost_reply_session"
    server = FakeBookingServer()
    registry = IdempotencyRegistry()
    resumed = False

    async def book_appointment(tool_call_id: str, details: str):
        args = {"details": details}
        is_dup, entry = continuity.check_tool_idempotency(
            registry, "book_appointment", args, tool_call_id=tool_call_id
        )
        if is_dup:
            return entry, None  # blocked before touching the server

        continuity.record_tool_call(
            registry, "book_appointment", args, tool_call_id=tool_call_id, status="pending"
        )
        await continuity.manager.save_context(session_id, [], registry.to_dict())

        booking_id = await server.book(details)  # server side effect happens here
        if not resumed:
            raise ReplyDropped("connection dropped before the reply was received")
        continuity.complete_tool_call(registry, tool_call_id, booking_id)
        return None, booking_id

    # 1. First attempt: server books, reply is lost, complete_call never runs.
    with pytest.raises(ReplyDropped):
        await book_appointment("call_original", "Dentist at 10AM")
    assert len(server.bookings) == 1
    original_key = next(iter(registry.records))

    # 2. Restore from storage, as after a reconnect.
    restored = await continuity.manager.load_context(session_id)
    registry = IdempotencyRegistry(restored["pending_tool_calls"])
    resumed = True

    # 3. LLM re-calls the same action with a brand new tool_call_id.
    entry, booking_id = await book_appointment("call_brand_new_999", "Dentist at 10AM")

    assert booking_id is None, "tool must not have executed again"
    assert entry["status"] == "pending"
    assert entry["idempotency_key"] == original_key, "must match the same record"
    assert entry["tool_call_id"] == "call_original"
    assert len(registry) == 1
    assert len(server.bookings) == 1
