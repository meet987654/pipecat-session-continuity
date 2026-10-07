import asyncio
import os
import sys
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from pipecat_session_continuity.manager import SessionContinuityManager
from pipecat_session_continuity import (
    SessionContinuity,
    IdempotencyRegistry,
)


class MockFunctionCallParams:
    def __init__(self, tool_call_id: str):
        self.tool_call_id = tool_call_id


class MockContext:
    def __init__(self, messages=None):
        self._messages = messages or []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


class MockPipelineTask:
    def __init__(self):
        self.queued_frames = []

    async def queue_frames(self, frames):
        self.queued_frames.extend(frames)


@pytest.mark.asyncio
async def test_idempotency():
    redis_url = os.getenv("REDIS_URL", "redis://invalid-host-so-it-falls-back:6379")
    continuity = SessionContinuity(redis_url=redis_url, ttl_seconds=60)
    manager = continuity.manager
    session_id = "test_idempotency_real_race"

    registry = IdempotencyRegistry()
    is_resumed = False
    mock_appointment_counter = 0

    async def book_appointment(params, details: str, client_token: str = None):
        nonlocal mock_appointment_counter
        call_id = params.tool_call_id
        args = {"details": details}

        # Check idempotency via deterministic key + call_id + client_token
        is_dup, entry = continuity.check_tool_idempotency(
            pending_tool_calls=registry,
            tool_name="book_appointment",
            arguments=args,
            tool_call_id=call_id,
            client_token=client_token,
        )

        if is_dup and entry:
            if entry["status"] == "pending" and is_resumed:
                return "SYSTEM_NOTE: The previous attempt to book this appointment was interrupted by a connection drop. The outcome is unknown. Please ask the user if they received a confirmation before retrying."
            elif entry["status"] == "completed":
                return f"This was already done: {entry['result']}"

        continuity.record_tool_call(
            pending_tool_calls=registry,
            tool_name="book_appointment",
            arguments=args,
            tool_call_id=call_id,
            client_token=client_token,
            status="pending",
        )
        await manager.save_context(session_id, [], registry.to_dict())

        mock_appointment_counter += 1
        # SLEEP to allow cancellation mid-flight
        await asyncio.sleep(0.5)

        result_str = f"Appointment booked! ID: apt_{mock_appointment_counter}"
        continuity.complete_tool_call(
            pending_tool_calls=registry,
            key_or_call_id=call_id,
            result=result_str,
        )
        await manager.save_context(session_id, [], registry.to_dict())

        return result_str

    print("=== CASE 1: Mid-Flight Cancellation (True Race) ===")
    params1 = MockFunctionCallParams(tool_call_id="call_111")

    task = asyncio.create_task(book_appointment(params1, "Dentist at 10AM"))
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Task was genuinely cancelled mid-flight!")

    assert mock_appointment_counter == 1

    # Simulate Resume
    restored = await manager.load_context(session_id)
    registry = IdempotencyRegistry(restored.get("pending_tool_calls", {}) if restored else {})
    is_resumed = True

    # Call again with SAME tool_call_id (legacy behavior)
    res_resume_same_id = await book_appointment(params1, "Dentist at 10AM")
    assert mock_appointment_counter == 1
    assert "outcome is unknown" in res_resume_same_id

    print("\n=== CASE 2: Issue #1 Fix - Mid-Flight Resumed with BRAND NEW tool_call_id ===")
    # LLM re-calls the same logical action with a NEW tool_call_id
    params_new_id = MockFunctionCallParams(tool_call_id="call_brand_new_999")
    res_resume_new_id = await book_appointment(params_new_id, "Dentist at 10AM")
    # Must NOT increment counter, must identify the pending action via deterministic args hash
    assert mock_appointment_counter == 1
    assert "outcome is unknown" in res_resume_new_id

    print("\n=== CASE 3: Resumed Duplicate After Full Completion ===")
    is_resumed = False
    params2 = MockFunctionCallParams(tool_call_id="call_222")

    res_complete = await book_appointment(params2, "Doctor at 2PM")
    assert mock_appointment_counter == 2
    assert "Appointment booked! ID: apt_2" in res_complete

    # Simulate Resume and LLM re-calls with DIFFERENT tool_call_id
    is_resumed = True
    params_dup_new_id = MockFunctionCallParams(tool_call_id="call_doctor_retry_333")
    res_duplicate = await book_appointment(params_dup_new_id, "Doctor at 2PM")
    assert mock_appointment_counter == 2
    assert "This was already done: Appointment booked! ID: apt_2" in res_duplicate

    print("\n=== CASE 4: Client-Side Idempotency Token ===")
    is_resumed = False
    params_client = MockFunctionCallParams(tool_call_id="call_client_444")
    token = "client-req-uuid-abc"

    res_client = await book_appointment(params_client, "Optometrist at 4PM", client_token=token)
    assert mock_appointment_counter == 3

    # Client retries with same token but different call ID and even slightly altered text
    params_client_retry = MockFunctionCallParams(tool_call_id="call_client_555")
    res_client_retry = await book_appointment(params_client_retry, "Optometrist at 4PM (retry)", client_token=token)
    assert mock_appointment_counter == 3
    assert "This was already done:" in res_client_retry

    print("\n=== CASE 5: Enhanced System Prompt Injection Verification ===")
    context = MockContext()
    task_mock = MockPipelineTask()
    is_res, tools = await continuity.resume_or_start(task_mock, context, session_id)

    assert is_res is True
    injected_messages = context.get_messages()
    system_notices = [m["content"] for m in injected_messages if m["role"] == "system"]

    # Verify that the injected system prompt includes the tool name and argument details
    has_enhanced_pending_notice = any(
        "Dentist at 10AM" in notice and "Do NOT call this tool again" in notice
        for notice in system_notices
    )
    has_enhanced_completed_notice = any(
        "Doctor at 2PM" in notice and "was already successfully executed" in notice
        for notice in system_notices
    )

    assert has_enhanced_pending_notice, f"Expected pending notice with arguments in {system_notices}"
    assert has_enhanced_completed_notice, f"Expected completed notice with arguments in {system_notices}"

    print("[SUCCESS] All test cases in test_idempotency.py passed!")


if __name__ == "__main__":
    asyncio.run(test_idempotency())
