"""
Example 01: Hard Process Termination and Recovery
=================================================
Demonstrates how pipecat-session-continuity survives catastrophic, violent process
termination (e.g. SIGKILL, OOM killer, Kubernetes pod eviction, power failure).

In this example:
1. Process A starts, initialises session storage (SQLite/Redis), and handles a conversation.
2. A user converses and a tool call ('process_payment') is registered and executed.
3. Process A is VIOLENTLY KILLED using os._exit / kill -9 (no graceful teardown).
4. Process B starts as an entirely new OS process with zero in-memory state.
5. Client reconnects with its signed session token.
6. Process B seamlessly resumes conversation context and tool idempotency state.
"""

import asyncio
import multiprocessing
import os
import signal
import sys
import time
from unittest.mock import MagicMock, AsyncMock

# Add root directory to sys.path so pipecat_session_continuity is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import SessionContinuity


DB_PATH = "example_hard_kill_sessions.db"
SECRET = "production-secret-key-42"
SESSION_ID = "user-sess-hard-kill-demo"


def run_worker_process_turn1(sync_event):
    """Worker Process A: Runs conversation Turn 1, then violently crashes."""
    print("\n[Worker A (PID: {})] Starting up fresh...".format(os.getpid()))
    
    continuity = SessionContinuity(
        db_path=DB_PATH,
        secret=SECRET,
    )

    class MockContext:
        def __init__(self, msgs):
            self._messages = msgs
        def get_messages(self):
            return self._messages
        def set_messages(self, msgs):
            self._messages = msgs
        def add_message(self, msg):
            self._messages.append(msg)

    async def turn1():
        # User connects for the first time
        mock_task = MagicMock()
        mock_task.queue_frames = AsyncMock()
        context = MockContext([])

        is_resumed, pending_tools = await continuity.resume_or_start(mock_task, context, SESSION_ID)
        print("[Worker A] Session initialized. is_resumed={}".format(is_resumed))

        # User gives critical information
        context.add_message({"role": "user", "content": "Book appointment for Dr. Smith at 3 PM, confirmation code REF-9921."})
        context.add_message({"role": "assistant", "content": "I have that noted. Processing appointment for Dr. Smith at 3 PM..."})

        # Tool call initiated and completed
        pending_tools = {}
        tool_rec = continuity.record_tool_call(
            pending_tools,
            tool_name="schedule_appointment",
            arguments={"doctor": "Dr. Smith", "time": "15:00", "code": "REF-9921"},
            client_token="client-token-9921",
            status="pending"
        )
        print("[Worker A] Tool call registered: schedule_appointment (status: pending)")

        # Save checkpoint before execution
        await continuity.checkpoint(context, SESSION_ID, pending_tools)

        # Tool finishes execution
        key = tool_rec.get("idempotency_key") or tool_rec.get("tool_call_id")
        continuity.complete_tool_call(pending_tools, key, result="Appointment confirmed for 3:00 PM (ID #8812)")
        print("[Worker A] Tool call completed: schedule_appointment (status: completed)")

        # End of turn checkpoint
        await continuity.checkpoint(context, SESSION_ID, pending_tools)
        print("[Worker A] Turn 1 checkpoint saved to persistent storage ({}).".format(DB_PATH))

        # Notify parent process that turn is completed
        sync_event.set()
        print("[Worker A] Simulating catastrophic crash in 0.5s (SIGKILL / os._exit)...")
        time.sleep(0.5)

        # VIOLENT EXIT: bypasses Python atexit, finally blocks, and normal cleanup!
        print("[Worker A] *** CRASH! os._exit(137) ***\n")
        os._exit(137)

    asyncio.run(turn1())


def run_worker_process_turn2():
    """Worker Process B: Fresh process, simulates reconnect and resume after crash."""
    print("[Worker B (PID: {})] Starting up after crash of Worker A...".format(os.getpid()))
    
    continuity = SessionContinuity(
        db_path=DB_PATH,
        secret=SECRET,
    )

    class MockContext:
        def __init__(self, msgs=None):
            self._messages = msgs or []
        def get_messages(self):
            return self._messages
        def set_messages(self, msgs):
            self._messages = msgs
        def add_message(self, msg):
            self._messages.append(msg)

    async def turn2():
        mock_task = MagicMock()
        mock_task.queue_frames = AsyncMock()
        context = MockContext()

        print("[Worker B] Client reconnects with session token. Invoking resume_or_start()...")
        is_resumed, pending_tools = await continuity.resume_or_start(mock_task, context, SESSION_ID)

        print("[Worker B] Resume complete: is_resumed={}".format(is_resumed))
        messages = context.get_messages()
        print("[Worker B] Restored {} conversation messages:".format(len(messages)))
        for i, m in enumerate(messages, 1):
            content = m.get("content", "")
            preview = (content[:80] + "...") if len(content) > 80 else content
            print("   {}. [{}] {}".format(i, m.get("role"), preview))

        # Verify idempotency of the tool call that was executed prior to crash
        is_dup, record = continuity.check_tool_idempotency(
            pending_tools,
            tool_name="schedule_appointment",
            arguments={"doctor": "Dr. Smith", "time": "15:00", "code": "REF-9921"},
            client_token="client-token-9921",
        )
        print("\n[Worker B] Checking idempotency for duplicate schedule_appointment call:")
        print("   is_duplicate: {}".format(is_dup))
        print("   tool_status:  {}".format(record.get("status") if record else None))
        print("   prior_result: {}".format(record.get("result") if record else None))

        assert is_resumed is True, "Session should have resumed!"
        assert is_dup is True, "Tool call should be identified as duplicate!"
        assert record["status"] == "completed", "Tool call status should be completed!"
        assert "8812" in record["result"], "Tool result must be preserved!"
        print("\nSUCCESS: All context and idempotency state survived violent process death!\n")

    asyncio.run(turn2())


def main():
    print("=" * 70)
    print("  PipeCat Session Continuity: Hard Process Kill & Recovery Demo")
    print("=" * 70)

    # Clean previous run db
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError:
            pass

    sync_event = multiprocessing.Event()
    worker_a = multiprocessing.Process(target=run_worker_process_turn1, args=(sync_event,))
    worker_a.start()

    # Wait for Worker A to save context and crash
    sync_event.wait(timeout=10.0)
    worker_a.join(timeout=5.0)

    exit_code = worker_a.exitcode
    print("[Main Orchestrator] Worker A terminated with exit code: {} (Crash confirmed)".format(exit_code))

    # Now launch Worker B (completely separate process)
    worker_b = multiprocessing.Process(target=run_worker_process_turn2)
    worker_b.start()
    worker_b.join(timeout=10.0)

    # Cleanup test db
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError:
            pass


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
