"""
Example 02: Simulated Network Failures & Edge Cases
===================================================
Demonstrates how pipecat-session-continuity handles the three most common
real-world network failure modes encountered by production voice agents:

1. Transient Socket Severing (TCP RST / WiFi Drops)
   - Connection drops abruptly mid-conversation.
   - Client reconnects with valid HMAC signature and resumes conversation context.

2. In-Flight Tool Drop (Network Drop During External API Call)
   - Connection drops while a critical external action ('transfer_funds') is pending.
   - On reconnect, resume_or_start detects the pending action and injects an
     explicit System Notice to prevent duplicate charges and solicit user clarification.

3. Mobile Sleep / App Backgrounding (Stale Resume Boundary)
   - User locks screen or backgrounds mobile app for extended period (> stale_threshold).
   - On reconnect, system detects long elapsed absence and injects a gentle re-engagement
     prompt rather than an abrupt inline continuation.
"""

import asyncio
import os
import sys
import time
from unittest.mock import MagicMock, AsyncMock

# Add root directory to sys.path so pipecat_session_continuity is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import SessionContinuity

DB_PATH = "example_network_failures.db"
SECRET = "test-network-failures-secret"


class MockContext:
    """Mock Pipecat LLMContext for realistic message tracking."""
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


def create_mock_task():
    task = MagicMock()
    task.queue_frames = AsyncMock()
    return task


async def scenario_1_socket_severing():
    print("\n" + "=" * 65)
    print("SCENARIO 1: Transient Socket Severing (TCP RST / WiFi Drop)")
    print("=" * 65)

    continuity = SessionContinuity(db_path=DB_PATH, secret=SECRET)
    session_id, signature = continuity.new_session()
    print("[Client] Generated secure session token:")
    print("   session_id: {}".format(session_id))
    print("   signature:  {}...".format(signature[:16]))

    # Step 1: Initial user connection
    task = create_mock_task()
    context = MockContext()
    is_resumed, pending_tools = await continuity.resume_or_start(task, context, session_id)
    assert not is_resumed, "First connection must not be resumed"

    # Step 2: Conversation happens
    context.add_message({"role": "user", "content": "I'd like to check my account balance for account #1042."})
    context.add_message({"role": "assistant", "content": "Your current checking balance for account #1042 is $4,120.50."})
    await continuity.checkpoint(context, session_id, pending_tools)
    print("[Server] User checked balance. Turn 1 checkpointed.")

    # Step 3: Network Drop! Client socket abruptly severs without clean close handshake
    print("[Network] *** WiFi disconnect / TCP Reset detected! Connection severed ***")
    await asyncio.sleep(0.5)

    # Step 4: Client reconnects using stored session_id & signature
    print("[Client] Reconnecting to /ws?session_id={}&signature={}&reconnect=true".format(session_id, signature[:8]))
    is_valid = continuity.verify(session_id, signature)
    assert is_valid, "HMAC signature verification failed"
    print("[Server] Token signature verified successfully.")

    reconnect_task = create_mock_task()
    reconnect_context = MockContext()
    is_resumed, pending_tools = await continuity.resume_or_start(reconnect_task, reconnect_context, session_id)

    assert is_resumed is True, "Session should have resumed"
    print("[Server] Context restored! Messages in context: {}".format(len(reconnect_context.get_messages())))
    last_msg = reconnect_context.get_messages()[-1]
    print("[Server] Injected resume notice: [{}] {}".format(last_msg["role"], last_msg["content"]))
    print(">>> Scenario 1 PASSED: Seamless context recovery after sudden socket drop.")


async def scenario_2_inflight_tool_drop():
    print("\n" + "=" * 65)
    print("SCENARIO 2: In-Flight Tool Drop (Network severed mid-action)")
    print("=" * 65)

    continuity = SessionContinuity(db_path=DB_PATH, secret=SECRET)
    session_id, signature = continuity.new_session()

    task = create_mock_task()
    context = MockContext()
    await continuity.resume_or_start(task, context, session_id)

    # Conversation leads to tool call
    context.add_message({"role": "user", "content": "Please charge $75.00 for order #INV-882 to my saved card."})
    pending_tools = {}
    tool_rec = continuity.record_tool_call(
        pending_tools,
        tool_name="charge_card",
        arguments={"amount": 75.00, "order_id": "INV-882"},
        client_token="tok-card-882",
        status="pending",
    )
    print("[Server] Tool 'charge_card' recorded as pending with client token.")

    # Checkpoint pending state before invoking external payment gateway
    await continuity.checkpoint(context, session_id, pending_tools)
    print("[Server] Checkpoint saved with pending tool call.")

    # DISASTER: Connection dies BEFORE payment gateway response returns or completes
    print("[Network] *** Network drop mid-payment! Server killed before completion ***")
    await asyncio.sleep(0.5)

    # Reconnect
    reconnect_task = create_mock_task()
    reconnect_context = MockContext()
    is_resumed, restored_tools = await continuity.resume_or_start(reconnect_task, reconnect_context, session_id)

    # Verify that continuity injected protective system notice for the unconfirmed tool
    msgs = reconnect_context.get_messages()
    system_notices = [m for m in msgs if m.get("role") == "system" and "unconfirmed" in m.get("content", "")]
    assert len(system_notices) > 0, "A protective system notice must be injected for pending tool calls"
    print("[Server] Protective system notice injected into LLM context:")
    print("   \"{}\"".format(system_notices[0]["content"]))

    # Verify idempotency check rejects re-executing this pending call
    is_dup, dup_entry = continuity.check_tool_idempotency(
        restored_tools,
        tool_name="charge_card",
        arguments={"amount": 75.00, "order_id": "INV-882"},
        client_token="tok-card-882",
    )
    assert is_dup is True, "Tool call must be marked as duplicate"
    assert dup_entry["status"] == "pending", "Status must remain pending until confirmed"
    print("[Server] check_tool_idempotency returned duplicate=True (status: pending). Double-charging PREVENTED.")
    print(">>> Scenario 2 PASSED: Double execution prevented on interrupted action.")


async def scenario_3_mobile_backgrounding_stale_resume():
    print("\n" + "=" * 65)
    print("SCENARIO 3: Mobile Backgrounding / Sleep (Stale Resume)")
    print("=" * 65)

    # Use a small threshold (0.01 minutes ~ 0.6 seconds) for demonstration
    continuity = SessionContinuity(
        db_path=DB_PATH,
        secret=SECRET,
        stale_threshold_minutes=0.01,
    )
    session_id, signature = continuity.new_session()

    task = create_mock_task()
    context = MockContext()
    await continuity.resume_or_start(task, context, session_id)

    context.add_message({"role": "user", "content": "I need help with my insurance policy claim."})
    context.add_message({"role": "assistant", "content": "Certainly, what is your policy number?"})
    await continuity.checkpoint(context, session_id)
    print("[Server] User paused call. Active context saved.")

    print("[Mobile Client] User backgrounds app / locks screen for a while...")
    # Sleep to exceed stale threshold
    await asyncio.sleep(1.0)

    # User re-opens app and reconnects
    reconnect_task = create_mock_task()
    reconnect_context = MockContext()
    is_resumed, _ = await continuity.resume_or_start(reconnect_task, reconnect_context, session_id)

    assert is_resumed is True
    # Verify the welcome-back frame injected via queue_frames has the stale message
    queued_frames = reconnect_task.queue_frames.call_args[0][0]
    injected_msg = queued_frames[0].messages[0]["content"]
    assert "disconnected for a while" in injected_msg, "Stale disconnect notice should be used"
    print("[Server] Detected prolonged absence! Injected re-engagement greeting:")
    print("   \"{}\"".format(injected_msg))
    print(">>> Scenario 3 PASSED: Contextual re-engagement greeting injected after prolonged idle.")


async def main():
    print("=" * 70)
    print("  PipeCat Session Continuity: Network Failure & Edge Cases Demo")
    print("=" * 70)

    # Clean previous run db
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError:
            pass

    try:
        await scenario_1_socket_severing()
        await scenario_2_inflight_tool_drop()
        await scenario_3_mobile_backgrounding_stale_resume()
        print("\n" + "=" * 70)
        print("  ALL 3 REALISTIC FAILURE SCENARIOS COMPLETED SUCCESSFULLY!")
        print("=" * 70 + "\n")
    finally:
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except OSError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
