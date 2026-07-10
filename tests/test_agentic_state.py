import pytest
import asyncio
import time
from pipecat_session_continuity.manager import SessionContinuityManager
from pipecat_session_continuity import SessionContinuity

class MockContext:
    def __init__(self):
        self.messages = []
    def set_messages(self, msgs):
        self.messages = msgs
    def get_messages(self):
        return self.messages
    def add_message(self, msg):
        self.messages.append(msg)

class MockTask:
    def __init__(self):
        self.queued_frames = []
    async def queue_frames(self, frames):
        self.queued_frames.extend(frames)

@pytest.mark.asyncio
async def test_tool_call_hallucination_mitigation():
    continuity = SessionContinuity(redis_url="redis://invalid-host-so-it-falls-back:6379", ttl_seconds=60)
    session_id = "test_tool_hallucination"
    
    # 1. Save context with a pending tool call
    pending_calls = {
        "call_abc123": {
            "status": "pending",
            "result": None,
            "tool_name": "book_flight"
        }
    }
    await continuity.manager.save_context(session_id, [{"role": "user", "content": "Book a flight"}], pending_calls)
    
    # 2. Resume session
    task = MockTask()
    context = MockContext()
    is_resumed, pending = await continuity.resume_or_start(task, context, session_id)
    
    assert is_resumed is True
    assert "call_abc123" in pending
    
    # 3. Verify the system message was explicitly injected to mitigate hallucination
    messages = context.get_messages()
    system_msgs = [m for m in messages if m.get("role") == "system"]
    
    assert len(system_msgs) == 1
    sys_content = system_msgs[0]["content"]
    assert "book_flight" in sys_content
    assert "Do NOT call this tool again for the same request" in sys_content
    print("[SUCCESS] Tool-call hallucination mitigation string injected correctly.")

@pytest.mark.asyncio
async def test_staleness_boundary():
    # Set a 30-minute threshold for stale sessions
    continuity = SessionContinuity(redis_url="redis://invalid-host-so-it-falls-back:6379", ttl_seconds=3600, stale_threshold_minutes=30)
    
    # Test Case 1: UNDER the threshold (Recent Drop)
    session_id_recent = "test_recent"
    await continuity.manager.save_context(session_id_recent, [{"role": "user", "content": "Hello"}], {})
    
    task1 = MockTask()
    context1 = MockContext()
    await continuity.resume_or_start(task1, context1, session_id_recent)
    
    # The appended frame contains the user message
    frame1 = task1.queued_frames[0]
    msg_recent = frame1.messages[0]["content"]
    assert "connection dropped and was just restored" in msg_recent
    print("[SUCCESS] Recent drop triggered standard bridge message.")
    
    # Test Case 2: OVER the threshold (Stale Drop)
    session_id_stale = "test_stale"
    await continuity.manager.save_context(session_id_stale, [{"role": "user", "content": "Hello"}], {})
    
    # Hack the updated_at time backwards to simulate a 45 minute drop
    data = await continuity.manager.storage.load(f"pipecat:session:{session_id_stale}")
    import json
    parsed = json.loads(data)
    parsed["updated_at"] = time.time() - (45 * 60) # 45 mins ago
    await continuity.manager.storage.save(f"pipecat:session:{session_id_stale}", json.dumps(parsed), 3600)
    
    task2 = MockTask()
    context2 = MockContext()
    await continuity.resume_or_start(task2, context2, session_id_stale)
    
    frame2 = task2.queued_frames[0]
    msg_stale = frame2.messages[0]["content"]
    assert "disconnected for a while" in msg_stale
    assert "connection dropped and was just restored" not in msg_stale
    print("[SUCCESS] Stale drop triggered the staleness generic pick-up message.")

if __name__ == "__main__":
    asyncio.run(test_tool_call_hallucination_mitigation())
    asyncio.run(test_staleness_boundary())
