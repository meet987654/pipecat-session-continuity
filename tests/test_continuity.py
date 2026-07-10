import pytest
import asyncio
import os
from pipecat_session_continuity.manager import SessionContinuityManager
from pipecat_session_continuity.storage.redis_storage import RedisStorage
from pipecat_session_continuity.storage.sqlite_storage import SQLiteStorage

@pytest.fixture(params=["redis", "sqlite"])
def manager(request):
    """Returns a SessionContinuityManager configured with either Redis or SQLite."""
    if request.param == "redis":
        redis_url = os.getenv("REDIS_URL", "redis://invalid-host-so-it-falls-back:6379")
        storage = RedisStorage(redis_url=redis_url)
    else:
        # SQLite storage (use a temporary DB for tests)
        storage = SQLiteStorage(db_path="test_sessions.db")
        
    manager_obj = SessionContinuityManager(storage_backend=storage, ttl_seconds=60)
    
    yield manager_obj
    
    # Teardown: cleanup SQLite file if it exists
    if request.param == "sqlite" and os.path.exists("test_sessions.db"):
        try:
            os.remove("test_sessions.db")
        except OSError:
            pass

@pytest.mark.asyncio
async def test_session_manager_save_load(manager):
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
