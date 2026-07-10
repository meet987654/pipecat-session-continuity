import pytest
import asyncio
from pipecat_session_continuity.manager import SessionContinuityManager

@pytest.mark.asyncio
async def test_session_manager_save_load():
    # Use the in-memory fallback for unit testing without redis dependencies
    manager = SessionContinuityManager(redis_url="redis://invalid-host-so-it-falls-back:6379", ttl_seconds=60)
    
    session_id = "test-session-123"
    messages = [{"role": "user", "content": "Hello world!"}]
    
    # Initial load should be None
    assert await manager.load_context(session_id) is None
    
    # Save context
    await manager.save_context(session_id, messages, pending_tool_calls={})
    
    # Load context should return the messages
    loaded = await manager.load_context(session_id)
    assert loaded is not None
    assert loaded["messages"] == messages
    
    # Clear context
    await manager.clear_context(session_id)
    assert await manager.load_context(session_id) is None
