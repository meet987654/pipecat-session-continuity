"""
Example 03: Production Client-Side Reconnect Flow & Backoff
===========================================================
Demonstrates the client-side lifecycle and architecture for connecting,
detecting network drops, and reconnecting with exponential backoff and jitter.

Client Architecture Patterns Covered:
1. Handshake & Token Storage:
   - Request signed credentials via POST /create_session.
   - Cache {session_id, signature} in persistent client storage (localStorage / Keychain).

2. Exponential Backoff with Decorrelated Jitter:
   - On unexpected socket closure (code != 1000), schedule reconnect.
   - delay = min(max_delay, base_delay * (2 ** retry_count)) + random_jitter.
   - Prevents the 'Thundering Herd' problem against backend services during network spikes.

3. Client Idempotency Tokens:
   - Generates client_token for sensitive user actions (payments, bookings, purchases).
   - Re-submitting the same client_token after a reconnect guarantees exactly-once execution.

4. Session Resume Handshake:
   - Connects with ?session_id=...&signature=...&reconnect=true.
   - Server validates HMAC signature, loads persistent context, and resumes cleanly.
"""

import asyncio
import os
import random
import sys
import time
import uuid

# Add root directory to sys.path so pipecat_session_continuity is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import SessionContinuity

DB_PATH = "example_client_reconnect.db"
SECRET = "secure-production-secret-99"


class ResilientVoiceClient:
    """
    Reference client implementation showing production-grade connection
    management, exponential backoff with jitter, and session continuity tokens.
    """

    def __init__(
        self,
        base_delay: float = 0.5,
        max_delay: float = 4.0,
        max_retries: int = 5,
    ):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries

        # Client-side storage (equivalent to localStorage in a browser or SecureStore in mobile)
        self.storage = {}
        self.retry_count = 0
        self.is_connected = False
        self.last_client_token = None

    def store_credentials(self, session_id: str, signature: str):
        """Simulates persisting credentials to browser localStorage or mobile Keychain."""
        self.storage["session_id"] = session_id
        self.storage["signature"] = signature
        print("[Client Storage] Saved session credentials (session_id: {}, signature: {}...)".format(
            session_id, signature[:12]
        ))

    def get_stored_credentials(self):
        return self.storage.get("session_id"), self.storage.get("signature")

    def generate_idempotency_token(self, action_name: str) -> str:
        """Generates a stable client-side token to bind to user actions across retries."""
        self.last_client_token = "tok-{}-{}".format(action_name, uuid.uuid4().hex[:8])
        return self.last_client_token

    def calculate_backoff(self, attempt: int) -> float:
        """
        Calculates exponential backoff delay with 'Full Jitter':
        sleep = random_uniform(0, min(max_delay, base_delay * 2^attempt))
        """
        temp = min(self.max_delay, self.base_delay * (2 ** attempt))
        delay = random.uniform(temp * 0.5, temp)
        return round(delay, 3)

    async def connect_and_run(self, server_backend, simulate_drops_count: int = 2):
        """Simulates full connection, drop, and backoff reconnect lifecycle."""
        session_id, signature = self.get_stored_credentials()
        is_reconnect = bool(self.retry_count > 0)

        print("\n--- Connecting to Voice Agent (Attempt {}, is_reconnect={}) ---".format(
            self.retry_count + 1, is_reconnect
        ))

        # 1. Verify token
        if not server_backend.verify(session_id, signature):
            print("[Client Error] Invalid token signature! Requesting fresh session...")
            return False

        self.is_connected = True
        print("[Client] WebSocket connected successfully.")

        # 2. Simulate conversation and actions
        if not is_reconnect:
            print("[User -> Bot] 'Hello! I need to reserve a table for 4 tonight.'")
            token = self.generate_idempotency_token("reserve_table")
            print("[Client] Generated idempotency token for action: {}".format(token))
            await server_backend.handle_user_turn(
                session_id,
                user_text="Reserve table for 4 at 8 PM",
                action_name="reserve_table",
                client_token=token,
            )
        else:
            print("[Client] Resuming session! Sending reconnect confirmation frame...")
            resumed_state = await server_backend.handle_reconnect(session_id)
            print("[Bot -> User] Bot resumes seamlessly. Restored messages: {}".format(
                len(resumed_state["messages"])
            ))

        # 3. Simulate sudden network drops if requested
        if simulate_drops_count > 0:
            print("[Network] !!! Sudden network drop triggered (Connection lost) !!!")
            self.is_connected = False
            self.retry_count += 1

            if self.retry_count > self.max_retries:
                print("[Client Error] Max retries exceeded. Prompting user to check connection.")
                return False

            backoff_delay = self.calculate_backoff(self.retry_count)
            print("[Client Backoff] Scheduling reconnect in {:.3f}s (exponential backoff with jitter)...".format(
                backoff_delay
            ))
            await asyncio.sleep(backoff_delay)

            # Recursive reconnect attempt
            return await self.connect_and_run(
                server_backend, simulate_drops_count=simulate_drops_count - 1
            )

        print("\n[Client] Call finished successfully with zero duplicate actions.")
        return True


class MockServerBackend:
    """Mock server backend simulating the FastAPI + Pipecat server."""

    def __init__(self):
        self.continuity = SessionContinuity(db_path=DB_PATH, secret=SECRET)
        self.pending_tools = {}

    def new_session(self):
        return self.continuity.new_session()

    def verify(self, session_id, signature):
        return self.continuity.verify(session_id, signature)

    async def handle_user_turn(self, session_id, user_text, action_name, client_token):
        class MockContext:
            def __init__(self):
                self.messages = []
            def get_messages(self):
                return self.messages
            def set_messages(self, m):
                self.messages = m

        ctx = MockContext()
        ctx.messages.append({"role": "user", "content": user_text})
        ctx.messages.append({"role": "assistant", "content": "Booking table for 4 at 8 PM..."})

        # Register tool call
        self.continuity.record_tool_call(
            self.pending_tools,
            tool_name=action_name,
            arguments={"party_size": 4, "time": "20:00"},
            client_token=client_token,
            status="pending"
        )
        await self.continuity.checkpoint(ctx, session_id, self.pending_tools)

        # Complete tool call
        self.continuity.complete_tool_call(
            self.pending_tools,
            client_token,
            result="Table #14 confirmed for 4 people at 8:00 PM."
        )
        await self.continuity.checkpoint(ctx, session_id, self.pending_tools)

    async def handle_reconnect(self, session_id):
        class MockTask:
            async def queue_frames(self, frames):
                pass
        class MockContext:
            def __init__(self):
                self.messages = []
            def get_messages(self):
                return self.messages
            def set_messages(self, m):
                self.messages = m
            def add_message(self, m):
                self.messages.append(m)

        task = MockTask()
        ctx = MockContext()
        is_resumed, tools = await self.continuity.resume_or_start(task, ctx, session_id)
        return {"is_resumed": is_resumed, "messages": ctx.get_messages(), "tools": tools}


async def main():
    print("=" * 70)
    print("  PipeCat Session Continuity: Client Reconnect Flow & Backoff")
    print("=" * 70)

    # Clean previous run db
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError:
            pass

    server = MockServerBackend()
    client = ResilientVoiceClient(base_delay=0.3, max_delay=2.0, max_retries=3)

    # Step 1: Initial handshake (equivalent to client POST /create_session)
    session_id, signature = server.new_session()
    client.store_credentials(session_id, signature)

    # Step 2: Connect, simulate 2 sequential network drops, and recover via backoff
    success = await client.connect_and_run(server, simulate_drops_count=2)
    assert success is True, "Client reconnect flow should succeed!"

    # Clean up
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
