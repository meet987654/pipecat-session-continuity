"""
Example 06: Resolving an unconfirmed tool call with a status-check hook
=======================================================================
A booking reaches the server, but the reply is lost when the connection drops.
Without a hook the library can only say "outcome unconfirmed, ask the caller".
With a hook, resume asks the booking backend what actually happened:

  completed  -> record marked completed, the LLM is told the confirmed result
  not_found  -> only with retry_on_not_found=True: exactly ONE retry, under the SAME
                idempotency key. By default it is treated like "unknown".
  unknown / error / timeout (1 s default) -> "inconclusive" notice: no claim either way

Tools with no registered hook behave exactly as before.
"""
import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipecat_session_continuity import IdempotencyRegistry, SessionContinuity


class BookingServer:
    """Fake backend that honours idempotency keys (a repeated key returns the original booking)."""

    def __init__(self):
        self.by_key = {}

    def book(self, key, details):
        self.by_key.setdefault(key, f"apt_{len(self.by_key) + 1}:{details}")
        return self.by_key[key]


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
    async def queue_frames(self, frames):
        pass


async def run(label, server_executed):
    # retry_on_not_found=True is safe here ONLY because this fake backend honours idempotency keys.
    print(f"\n--- {label} ---")
    server = BookingServer()
    with tempfile.TemporaryDirectory() as d:
        sc = SessionContinuity(db_path=os.path.join(d, "ex.db"), secret="s")
        args = {"details": "Dentist at 10AM"}

        registry = IdempotencyRegistry()
        rec = registry.register_call("book_appointment", args, tool_call_id="call_1")
        key = rec["idempotency_key"]
        if server_executed:
            server.book(key, args["details"])  # booking lands, reply is lost
        await sc.manager.save_context("s", [], registry.to_dict())

        # The hook asks the backend; here the backend is looked up by idempotency key.
        async def check_booking(idempotency_key, arguments):
            if idempotency_key in server.by_key:
                return ("completed", server.by_key[idempotency_key])
            return "not_found"

        sc.register_status_check("book_appointment", check_booking, retry_on_not_found=True)

        ctx = Ctx()
        _, tools = await sc.resume_or_start(Task(), ctx, "s")
        print("LLM notice:", [m["content"] for m in ctx.get_messages() if m["role"] == "system"][0])

        registry = IdempotencyRegistry(tools)
        entry = registry.get_record(key)
        if entry["status"] == "retry_allowed":
            # The one permitted retry: same key, so an idempotent API can dedupe server-side.
            sc.record_tool_call(registry, "book_appointment", args, tool_call_id="call_2")
            print("retry result:", server.book(entry["idempotency_key"], args["details"]))
        print("bookings on server:", len(server.by_key))


async def main():
    await run("Booking landed, reply lost -> hook says completed", server_executed=True)
    await run("Booking never reached server -> hook says not_found", server_executed=False)


if __name__ == "__main__":
    asyncio.run(main())
