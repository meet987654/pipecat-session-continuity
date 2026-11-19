"""The final resume message (the one the LLM answers) must carry the pending-tool instruction."""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity

PLAIN = "[System Notice: The connection dropped and was just restored. Please briefly acknowledge this to the user and ask how you can continue helping.]"


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


async def _resume(sc, task):
    return await sc.resume_or_start(task, Ctx(), "s")


@pytest.mark.asyncio
async def test_pending_tool_is_named_in_final_message(tmp_path):
    sc = SessionContinuity(db_path=str(tmp_path / "a.db"), secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())
    task = Task()
    await _resume(sc, task)
    final = task.frames[0].messages[0]["content"]
    assert "'book_appointment'" in final
    assert "cannot confirm" in final and "Do not say it succeeded" in final


@pytest.mark.asyncio
async def test_no_pending_tool_message_unchanged(tmp_path):
    sc = SessionContinuity(db_path=str(tmp_path / "b.db"), secret="x")
    await sc.manager.save_context("s", [{"role": "user", "content": "hi"}], {})
    task = Task()
    await _resume(sc, task)
    assert task.frames[0].messages[0]["content"] == PLAIN


@pytest.mark.asyncio
async def test_status_check_resolved_tool_does_not_add_instruction(tmp_path):
    sc = SessionContinuity(db_path=str(tmp_path / "c.db"), secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())

    async def hook(k, a):
        return ("completed", "apt_1")

    sc.register_status_check("book_appointment", hook)
    task = Task()
    await _resume(sc, task)
    assert task.frames[0].messages[0]["content"] == PLAIN
