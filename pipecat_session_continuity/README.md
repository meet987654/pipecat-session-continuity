# Pipecat Session Continuity

A drop-in library to add connection-resilience and idempotency to your Pipecat 1.5 voice agents.
It snapshots your LLM's conversation history to Redis and securely restores it if the user drops and reconnects, using HMAC tokens to prevent session forgery.

## Quickstart (FastAPI + Pipecat)

*Verified against FastAPIWebsocketTransport (live audio/hard-kill test) and SmallWebRTCTransport (API-level state recovery/hard-kill test).*

```python
from pipecat_session_continuity import SessionContinuity

# 1. Initialize (defaults to SQLite for zero-config local dev)
continuity = SessionContinuity()

# Or configure Redis for production:
# continuity = SessionContinuity(redis_url="redis://localhost:6379", ttl_seconds=3600)

@app.post("/create_session")
async def create_session():
    # 2. Issue secure session tokens
    session_id, signature = continuity.new_session()
    return {"session_id": session_id, "signature": signature}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, session_id: str, signature: str):
    await websocket.accept()

    # 3. Verify on connect
    if not continuity.verify(session_id, signature):
        await websocket.close()
        return

    # ... Setup your PipelineTask and LLMContext ...

    # 4. Resume or Start!
    is_resumed, pending_tool_calls = await continuity.resume_or_start(task, context, session_id)

    # 5. Checkpoint during the call (e.g. on_turn_stopped) and on disconnect
    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(t, c):
        await continuity.checkpoint(context, session_id, pending_tool_calls)

    await runner.run(task)
```
## Limitations & Known Gaps

Before using this library in production, please be aware of the following architectural constraints:

### 1. Deterministic Tool-Call Idempotency & Client Tokens
Earlier versions keyed idempotency strictly to the LLM-generated `tool_call_id`, which was vulnerable to the LLM generating a brand new ID for the same logical action on reconnect (#1). 

`pipecat-session-continuity` now solves this with **deterministic idempotency keys**:
- Keys are derived from `tool_name` + canonical SHA-256 hash of sorted arguments (`idemp:det:{tool}:{args_hash}`).
- Optional `client_token` support binds actions directly to client-supplied tokens (`idemp:token:{tool}:{token}`).
- The `IdempotencyRegistry` supports dual lookup (by deterministic key or legacy `tool_call_id`), and `SessionContinuity.resume_or_start()` injects rich contextual system notices containing arguments and strict non-replay directives.
- *Best practice*: If your tool takes dynamic arguments like "current timestamp", pass a client-side idempotency token or normalize the parameters before checking idempotency.

### 2. In-Session Duplicate Delivery (At-Least-Once Delivery)
Checkpoints happen sequentially at the end of each turn. If a server dies *while* the TTS audio is streaming to the user but *before* the checkpoint runs, the LLM state is rolled back to the previous turn. Upon reconnect, the LLM will re-generate the answer. This is an inherent trait of optimistic, asynchronous checkpointing (At-Least-Once delivery).

### 3. Global, In-Process Metrics
The `/metrics` example tracks connection attempts and checkpoint timings using simple in-memory, process-global dictionaries. 
- **Ephemeral**: These counters reset every time the server restarts. 
- **Global**: They aggregate stats across *all* sessions connected to that worker process. 
- **Stateless environments**: In a multi-worker or Serverless environment, these metrics are not synchronized. 
For production telemetry, you should push these events to a dedicated metrics backend (e.g., Prometheus, Datadog) rather than relying on an in-process dictionary.

### 4. Localhost vs Network Testing
When testing on `localhost`, manipulating the WiFi/Network adapter will not sever the WebSocket, because loopback traffic bypasses the network stack. Real drop testing requires forcefully terminating the server process (e.g. `Ctrl+C`) or using a tool like Toxiproxy to simulate network failure.
