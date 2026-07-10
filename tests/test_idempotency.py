import sys
import os
import asyncio
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from pipecat_session_continuity.manager import SessionContinuityManager

class MockFunctionCallParams:
    def __init__(self, tool_call_id: str):
        self.tool_call_id = tool_call_id

import pytest

@pytest.mark.asyncio
async def test_idempotency():
    redis_url = os.getenv("REDIS_URL", "redis://invalid-host-so-it-falls-back:6379")
    manager = SessionContinuityManager(redis_url=redis_url, ttl_seconds=60)
    session_id = "test_idempotency_real_race"
    
    session_pending_tool_calls = {}
    is_resumed = False
    mock_appointment_counter = 0
    
    async def book_appointment(params, details: str):
        nonlocal mock_appointment_counter
        call_id = params.tool_call_id
        
        entry = session_pending_tool_calls.get(call_id)
        if entry:
            if entry["status"] == "pending" and is_resumed:
                return "SYSTEM_NOTE: The previous attempt to book this appointment was interrupted by a connection drop. The outcome is unknown. Please ask the user if they received a confirmation before retrying."
            elif entry["status"] == "completed":
                return f"This was already done: {entry['result']}"
        
        session_pending_tool_calls[call_id] = {"status": "pending", "result": None}
        await manager.save_context(session_id, [], session_pending_tool_calls)
        
        mock_appointment_counter += 1
        # SLEEP to allow cancellation mid-flight
        await asyncio.sleep(0.5)
        
        result_str = f"Appointment booked! ID: apt_{mock_appointment_counter}"
        session_pending_tool_calls[call_id] = {"status": "completed", "result": result_str}
        await manager.save_context(session_id, [], session_pending_tool_calls)
        
        return result_str

    print("=== CASE 1: Mid-Flight Cancellation (True Race) ===")
    params1 = MockFunctionCallParams(tool_call_id="call_111")
    
    # 1. Start the task
    task = asyncio.create_task(book_appointment(params1, "Dentist at 10AM"))
    
    # 2. Wait just a tiny bit so the counter increments but the sleep doesn't finish
    await asyncio.sleep(0.1)
    
    # 3. Cancel it mid-flight!
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Task was genuinely cancelled mid-flight!")
        
    print(f"Counter after cancellation: {mock_appointment_counter}")
    assert mock_appointment_counter == 1 # Counter incremented before the sleep
    
    # Verify the registry still shows pending because the second save never happened
    assert session_pending_tool_calls["call_111"]["status"] == "pending"
    print(f"Registry status: {session_pending_tool_calls['call_111']}")
    
    # 4. Simulate Resume
    print("\n--- Reconnecting (Resumed) ---")
    restored = await manager.load_context(session_id)
    session_pending_tool_calls = restored.get("pending_tool_calls", {}) if restored else {}
    is_resumed = True
    
    # 5. Call again with SAME tool_call_id
    res_resume = await book_appointment(params1, "Dentist at 10AM")
    print(f"Result: {res_resume}")
    print(f"Counter: {mock_appointment_counter}")
    
    assert mock_appointment_counter == 1 # Should NOT increment again
    assert "outcome is unknown" in res_resume
    
    print("\n=== CASE 2: Resumed Duplicate After Full Completion ===")
    is_resumed = False # Clean slate for the new call
    params2 = MockFunctionCallParams(tool_call_id="call_222")
    
    print("--- First Run (Complete) ---")
    res_complete = await book_appointment(params2, "Doctor at 2PM")
    print(f"Result: {res_complete}")
    print(f"Counter: {mock_appointment_counter}")
    assert mock_appointment_counter == 2
    
    print("\n--- Simulating Resume with Duplicate tool_call_id ---")
    is_resumed = True
    res_duplicate = await book_appointment(params2, "Doctor at 2PM")
    print(f"Result: {res_duplicate}")
    print(f"Counter: {mock_appointment_counter}")
    
    assert mock_appointment_counter == 2 # Should NOT increment again
    assert "This was already done:" in res_duplicate
    
    print("\n[SUCCESS] test_idempotency.py passed!")

if __name__ == "__main__":
    asyncio.run(test_idempotency())
