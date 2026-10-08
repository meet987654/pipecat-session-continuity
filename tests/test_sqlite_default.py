import asyncio
import os
import pytest
import time

from pipecat_session_continuity import SessionContinuity, SessionContinuityManager
from pipecat_session_continuity.storage.sqlite_storage import SQLiteStorage
from pipecat_session_continuity.storage.redis_storage import RedisStorage


@pytest.mark.asyncio
async def test_default_backend_is_sqlite():
    """Verify that both SessionContinuityManager and SessionContinuity default to SQLite."""
    manager = SessionContinuityManager()
    assert isinstance(manager.storage, SQLiteStorage)
    assert manager.storage.db_path == "pipecat_sessions.db"

    continuity = SessionContinuity()
    assert isinstance(continuity.manager.storage, SQLiteStorage)
    assert continuity.manager.storage.db_path == "pipecat_sessions.db"


@pytest.mark.asyncio
async def test_custom_sqlite_db_path():
    """Verify that custom db_path parameter is honored."""
    test_db = "test_custom_path.db"
    try:
        continuity = SessionContinuity(db_path=test_db)
        assert isinstance(continuity.manager.storage, SQLiteStorage)
        assert continuity.manager.storage.db_path == test_db

        session_id = "test-custom-session"
        msgs = [{"role": "user", "content": "testing custom db"}]
        await continuity.manager.save_context(session_id, msgs)

        loaded = await continuity.manager.load_context(session_id)
        assert loaded is not None
        assert loaded["messages"] == msgs
    finally:
        if os.path.exists(test_db):
            try:
                os.remove(test_db)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_redis_url_explicit_override():
    """Verify that passing redis_url uses RedisStorage instead of the default SQLite."""
    manager = SessionContinuityManager(redis_url="redis://invalid-test-host:6379")
    assert isinstance(manager.storage, RedisStorage)

    continuity = SessionContinuity(redis_url="redis://invalid-test-host:6379")
    assert isinstance(continuity.manager.storage, RedisStorage)


@pytest.mark.asyncio
async def test_in_memory_sqlite_continuity():
    """Verify in-memory SQLite (:memory:) retains state across async save/load calls."""
    storage = SQLiteStorage(db_path=":memory:")
    manager = SessionContinuityManager(storage_backend=storage, ttl_seconds=60)

    session_id = "mem-session-123"
    messages = [
        {"role": "system", "content": "You are a test assistant."},
        {"role": "user", "content": "Remember this in memory."},
    ]

    await manager.save_context(session_id, messages, {"tool1": {"status": "completed"}})

    loaded = await manager.load_context(session_id)
    assert loaded is not None
    assert loaded["messages"] == messages
    assert "tool1" in loaded["pending_tool_calls"]

    # Delete
    await manager.clear_context(session_id)
    assert await manager.load_context(session_id) is None
    storage.close()


@pytest.mark.asyncio
async def test_sqlite_purge_expired():
    """Verify purge_expired removes records that exceeded ttl_seconds."""
    storage = SQLiteStorage(db_path=":memory:")
    
    # Save a record with ttl = 1 second
    await storage.save("pipecat:session:expiring", '{"msg": "bye"}', ttl_seconds=1)
    # Save a record with ttl = 300 seconds
    await storage.save("pipecat:session:active", '{"msg": "alive"}', ttl_seconds=300)

    # Wait for the first record to expire
    await asyncio.sleep(1.2)

    pruned = storage.purge_expired()
    assert pruned >= 1

    # Expired should be gone
    assert await storage.load("pipecat:session:expiring") is None
    # Active should still be present
    assert await storage.load("pipecat:session:active") is not None
    storage.close()
