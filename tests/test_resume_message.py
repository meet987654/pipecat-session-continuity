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
async def test_status_check_confirmed_tool_is_stated_as_fact(tmp_path):
    sc = SessionContinuity(db_path=str(tmp_path / "c.db"), secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())

    async def hook(k, a):
        return ("completed", "apt_1")

    sc.register_status_check("book_appointment", hook)
    task = Task()
    await _resume(sc, task)
    final = task.frames[0].messages[0]["content"]
    assert "'book_appointment'" in final and "did go through" in final
    assert "cannot confirm" not in final


@pytest.mark.asyncio
async def test_status_check_not_found_with_retry_is_stated_as_not_done(tmp_path):
    async def hook(k, a):
        return "not_found"

    sc = SessionContinuity(db_path=str(tmp_path / "r.db"), secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())
    sc.register_status_check("book_appointment", hook, retry_on_not_found=True)
    task = Task()
    await _resume(sc, task)
    final = task.frames[0].messages[0]["content"]
    assert "did not go through" in final and "cannot confirm" not in final


async def _pending_with_hook(tmp_path, name, hook, **kw):
    sc = SessionContinuity(db_path=str(tmp_path / name), secret="x", **kw)
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())
    sc.register_status_check("book_appointment", hook)
    task, ctx = Task(), Ctx()
    _, pending = await sc.resume_or_start(task, ctx, "s")
    return task, ctx, pending


def _notice(ctx):
    return [m["content"] for m in ctx.get_messages() if m["role"] == "system"][-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["unknown", "not_found", "garbage"])
async def test_inconclusive_status_check_forbids_claims_both_ways(tmp_path, answer):
    async def hook(k, a):
        return answer

    task, ctx, pending = await _pending_with_hook(tmp_path, f"{answer}.db", hook)
    notice = _notice(ctx)
    assert "could not determine" in notice
    assert "do not tell the user that it succeeded" in notice and "do not tell them that it failed" in notice
    final = task.frames[0].messages[0]["content"]
    assert "cannot confirm" in final and "Do not say it succeeded and do not say it failed" in final
    assert "received a confirmation" not in final  # the backend was asked; do not ask the caller to guess
    assert all(r["status"] == "pending" for r in pending.values())  # still guarded against a retry


@pytest.mark.asyncio
async def test_status_check_error_and_timeout_are_inconclusive(tmp_path):
    import asyncio

    async def boom(k, a):
        raise RuntimeError("status API down")

    async def slow(k, a):
        await asyncio.sleep(1)

    _, ctx, _ = await _pending_with_hook(tmp_path, "e.db", boom)
    assert "could not determine" in _notice(ctx)
    _, ctx, _ = await _pending_with_hook(tmp_path, "t.db", slow, status_check_timeout_seconds=0.05)
    assert "could not determine" in _notice(ctx)


@pytest.mark.asyncio
async def test_no_hook_keeps_ask_the_caller_wording(tmp_path):
    sc = SessionContinuity(db_path=str(tmp_path / "n.db"), secret="x")
    reg = IdempotencyRegistry()
    reg.register_call("book_appointment", {"details": "10AM"}, tool_call_id="c1")
    await sc.manager.save_context("s", [], reg.to_dict())
    task, ctx = Task(), Ctx()
    await sc.resume_or_start(task, ctx, "s")
    assert "could not determine" not in _notice(ctx)
    assert "received a confirmation" in task.frames[0].messages[0]["content"]


@pytest.mark.asyncio
@pytest.mark.parametrize("resumed", [True, False])
async def test_bridge_or_greet_message_triggers_the_llm(tmp_path, resumed):
    """Regression: the frame was queued without run_llm=True, so after a reconnect the agent
    said nothing until the caller spoke. Both the resume bridge and the fresh greeting must run."""
    sc = SessionContinuity(db_path=str(tmp_path / "r.db"), secret="x")
    if resumed:
        await sc.manager.save_context("s", [{"role": "user", "content": "hi"}], {})
    task = Task()
    is_resumed, _ = await _resume(sc, task)
    assert is_resumed is resumed
    (frame,) = task.frames
    assert frame.run_llm is True
