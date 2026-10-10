"""
Example 05: Dialog State & User Metadata Persistence Across Reconnects
========================================================================
Demonstrates persisting and restoring arbitrary business metadata
(e.g., user_id, verification status, dialog state machine step, shopping cart)
alongside conversation messages and tool idempotency state.

In real-world voice applications:
1. Turn 1: Caller authenticates (`verified: True`, `user_id: "usr_4402"`).
2. Turn 2: Caller selects items into a shopping cart (`cart: [...]`, `step: "checkout"`).
3. Network Drop: Client connection terminates abruptly.
4. Reconnect: Fresh process / container resumes session:
   - Restores LLM messages array.
   - Restores tool execution records.
   - Restores business metadata directly without requiring a secondary database!
"""

import asyncio
import os
import sys
from unittest.mock import MagicMock, AsyncMock

# Add root directory to sys.path so pipecat_session_continuity is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import SessionContinuity


class MockLLMContext:
    def __init__(self, messages=None):
        self._messages = list(messages) if messages else []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages = list(messages)

    def add_message(self, message):
        self._messages.append(message)


async def main():
    db_path = "example_metadata_sessions.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    session_id = "call-sess-enterprise-991"
    continuity = SessionContinuity(db_path=db_path)

    print("=" * 70)
    print("DIALOG STATE & USER METADATA PERSISTENCE DEMO")
    print("=" * 70)

    # ---------------------------------------------------------
    # STEP 1: Turn 1 - Authentication & Initial Business State
    # ---------------------------------------------------------
    print("\n[Turn 1] User verifies identity and starts checkout...")
    context = MockLLMContext([
        {"role": "system", "content": "You are an automated e-commerce voice assistant."},
        {"role": "user", "content": "Hi, this is Alice Smith. I'd like to check out."},
        {"role": "assistant", "content": "Hello Alice! I have verified your account."},
    ])

    # Custom business metadata
    initial_metadata = {
        "user_id": "usr_alice_8892",
        "verified": True,
        "caller_tier": "platinum",
        "dialog_step": "item_selection",
        "cart_items": [{"id": "item_1", "name": "Wireless Headset", "price": 89.99}],
        "cart_total": 89.99,
    }

    # Save checkpoint with metadata
    await continuity.checkpoint(
        context=context,
        session_id=session_id,
        metadata=initial_metadata,
    )
    print(f"  Saved Turn 1 checkpoint with {len(context.get_messages())} messages.")
    print(f"  Persisted metadata: {initial_metadata}")

    # ---------------------------------------------------------
    # STEP 2: Turn 2 - Dialog State Machine Progression
    # ---------------------------------------------------------
    print("\n[Turn 2] User applies promo code and advances dialog step...")
    context.add_message({"role": "user", "content": "Can I apply discount code SAVE20?"})
    context.add_message({"role": "assistant", "content": "Applied! Your new total is $71.99."})

    # Update dialog state metadata incrementally
    updated_metadata = dict(initial_metadata)
    updated_metadata["dialog_step"] = "payment_confirmation"
    updated_metadata["promo_code"] = "SAVE20"
    updated_metadata["cart_total"] = 71.99

    await continuity.checkpoint(
        context=context,
        session_id=session_id,
        metadata=updated_metadata,
    )
    print(f"  Updated metadata dialog_step -> '{updated_metadata['dialog_step']}', total -> ${updated_metadata['cart_total']}")

    # ---------------------------------------------------------
    # STEP 3: Simulated Connection Drop & Worker Crash
    # ---------------------------------------------------------
    print("\n[Simulated Failure] Network drops mid-call! Process terminating...")
    del context
    del continuity

    # ---------------------------------------------------------
    # STEP 4: Fresh Worker Spawns & Client Reconnects
    # ---------------------------------------------------------
    print("\n[Reconnect] Fresh worker spawns. Client reconnects with session_id...")
    fresh_continuity = SessionContinuity(db_path=db_path)
    resumed_context = MockLLMContext()
    mock_task = MagicMock()
    mock_task.queue_frames = AsyncMock()

    # Pass return_metadata=True to receive restored metadata directly
    is_resumed, pending_tools, restored_meta = await fresh_continuity.resume_or_start(
        task=mock_task,
        context=resumed_context,
        session_id=session_id,
        return_metadata=True,
    )

    print(f"  Session Resumed?      : {is_resumed}")
    print(f"  Restored Messages     : {len(resumed_context.get_messages())}")
    print(f"  Restored User ID      : {restored_meta.get('user_id')}")
    print(f"  Account Verified?     : {restored_meta.get('verified')}")
    print(f"  Current Dialog Step   : {restored_meta.get('dialog_step')}")
    print(f"  Promo Code Applied    : {restored_meta.get('promo_code')}")
    print(f"  Cart Total            : ${restored_meta.get('cart_total')}")
    print(f"  Cart Items Count      : {len(restored_meta.get('cart_items', []))}")

    # ---------------------------------------------------------
    # STEP 5: Incremental State Updates via set_metadata
    # ---------------------------------------------------------
    print("\n[Direct Update] Updating dialog metadata via set_metadata()...")
    await fresh_continuity.set_metadata(session_id, {
        "dialog_step": "completed",
        "order_id": "ord_998811",
        "payment_method": "apple_pay",
    })

    final_meta = await fresh_continuity.get_metadata(session_id)
    print(f"  Final Dialog Step     : {final_meta.get('dialog_step')}")
    print(f"  Order ID              : {final_meta.get('order_id')}")
    print(f"  All keys in metadata  : {list(final_meta.keys())}")

    # Clean up demo database
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    print("\nDialog state metadata demonstration completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
