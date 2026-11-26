"""
Fail closed: a storage outage must raise, not look like a fresh call.

Before 0.4.0 every load error was logged and turned into "no session", so resume_or_start
greeted the caller as new, dropped the conversation and, worse, dropped the idempotency
records that block a duplicate of a tool call that was in flight when the line dropped.
"""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import (
    BaseStorage,
    IdempotencyRegistry,
    RedisStorage,
    SessionContinuity,
    SQLiteStorage,
    StorageUnavailableError,
)


class Ctx:
    def __init__(self):
        self.m = []

    def get_messages(self):
        return self.m

    def set_messages(self, m):
        self.m = list(m)

    def add_message(self, x):
        self.m.append(x)


class Task:
    def __init__(self):
        self.frames = []

    async def queue_frames(self, frames):
        self.frames.extend(frames)


class FlakyStorage(BaseStorage):
    """In-memory store that can be switched into an outage. Raises its own error type,
    as a third-party backend would."""

    def __init__(self):
        self.data = {}
        self.down = False

    def _check(self):
        if self.down:
            raise ConnectionError("store unreachable")

    async def save(self, key, value, ttl_seconds):
        self._check()
        self.data[key] = value

    async def load(self, key):
        self._check()
        return self.data.get(key)

    async def delete(self, key):
        self._check()
        self.data.pop(key, None)


async def session_with_pending_booking(storage):
    sc = SessionContinuity(storage_backend=storage, secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"time": "15:00"}, tool_call_id="c1")
    await sc.manager.save_context("s", [{"role": "user", "content": "book 3pm"}], reg.to_dict())
    return sc


@pytest.mark.asyncio
async def test_outage_on_resume_raises_and_does_not_greet_as_new():
    storage = FlakyStorage()
    sc = await session_with_pending_booking(storage)
    storage.down = True

    task = Task()
    with pytest.raises(StorageUnavailableError):
        await sc.resume_or_start(task, Ctx(), "s")
    assert task.frames == []  # no fresh-call greeting was queued
    assert sc.metrics.get_summary()["resumes"]["error_count"] == 1

    # When the store comes back, the session and its guard are still there.
    storage.down = False
    resumed, pending = await sc.resume_or_start(Task(), Ctx(), "s")
    assert resumed
    (rec,) = IdempotencyRegistry(pending).records.values()
    assert rec["status"] == "pending"


@pytest.mark.asyncio
async def test_missing_session_still_starts_fresh():
    sc = SessionContinuity(storage_backend=FlakyStorage(), secret="x")
    resumed, pending = await sc.resume_or_start(Task(), Ctx(), "never-seen")
    assert not resumed and pending == {}


@pytest.mark.asyncio
async def test_corrupt_record_raises_instead_of_starting_fresh():
    storage = FlakyStorage()
    sc = SessionContinuity(storage_backend=storage, secret="x")
    storage.data[sc.manager._get_key("s")] = "{not json"
    with pytest.raises(StorageUnavailableError):
        await sc.resume_or_start(Task(), Ctx(), "s")


@pytest.mark.asyncio
async def test_set_metadata_during_outage_does_not_overwrite_the_session():
    """It used to read None, then save empty messages and tools over the real session."""
    storage = FlakyStorage()
    sc = await session_with_pending_booking(storage)
    before = dict(storage.data)
    storage.down = True
    with pytest.raises(StorageUnavailableError):
        await sc.set_metadata("s", {"caller": "known"})
    assert storage.data == before


@pytest.mark.asyncio
async def test_failed_checkpoint_does_not_crash_the_call_and_is_counted_as_an_error():
    storage = FlakyStorage()
    sc = SessionContinuity(storage_backend=storage, secret="x")
    storage.down = True
    await sc.checkpoint(Ctx(), "s", {})  # logged, not raised: a live call keeps going
    assert sc.manager.checkpoint_times == []  # not recorded as a successful save
    summary = sc.metrics.get_summary()["checkpoints"]
    assert summary["error_count"] == 1 and summary["success_count"] == 0


@pytest.mark.asyncio
async def test_sqlite_read_error_raises(tmp_path, monkeypatch):
    storage = SQLiteStorage(db_path=str(tmp_path / "s.db"))

    def broken(key):
        raise OSError("disk I/O error")

    monkeypatch.setattr(storage, "_load_sync", broken)
    with pytest.raises(StorageUnavailableError):
        await storage.load("k")
    sc = SessionContinuity(storage_backend=storage, secret="x")
    with pytest.raises(StorageUnavailableError):
        await sc.resume_or_start(Task(), Ctx(), "s")


def test_unreachable_redis_raises_by_default():
    with pytest.raises(StorageUnavailableError, match="allow_memory_fallback"):
        RedisStorage(redis_url="redis://invalid-host-fail-closed:6379")
    with pytest.raises(StorageUnavailableError):
        SessionContinuity(redis_url="redis://invalid-host-fail-closed:6379")


def test_memory_fallback_is_opt_in():
    storage = RedisStorage(redis_url="redis://invalid-host-fail-closed:6379", allow_memory_fallback=True)
    assert storage.use_memory
