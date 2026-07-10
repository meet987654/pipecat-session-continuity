# pipecat-session-continuity

A lightweight, drop-in library to add connection-resilience and state idempotency to Pipecat 1.5 voice agents.

## Quickstart

```python
from pipecat_session_continuity import SessionContinuity

continuity = SessionContinuity()  # Defaults to localhost:6379 Redis

@transport.event_handler("on_client_connected")
async def on_client_connected(transport, client):
    # Resume existing session context or start fresh
    is_resumed, pending_tools = await continuity.resume_or_start(task, context, session_id)
    
    if is_resumed:
        print(f"Welcome back! Restored {len(context.messages)} messages.")

# Hook into the pipeline to securely save state when the LLM finishes speaking
turn_observer = task.turn_tracking_observer
if turn_observer:
    @turn_observer.event_handler("on_turn_ended")
    async def on_turn_ended(observer, *args, **kwargs):
        await continuity.checkpoint(context, session_id, session_pending_tool_calls)
```

For full installation details, API documentation, and configuration options, see the **[Full Documentation](pipecat_session_continuity/README.md)**.

## Current Limitations
This library currently has a few intentional boundaries:
- It only persists the `LLMContext` (messages array) and pending tool calls. It does not attempt to serialize the state of other pipeline processors (like VAD state or STT buffers).
- It relies entirely on Redis for the store.
- **Tool-Call Re-initiation gap**: Idempotency is keyed strictly on Pipecat's `tool_call_id`. After a session resumes, the LLM has no reason to reuse that specific ID for interrupted work, and will often generate a brand-new tool call with a new ID for the exact same logical action. This library restores the pending state perfectly, but solving the LLM re-initiation hallucination requires surfacing pending state directly into the system prompt, which this library doesn't currently handle out-of-the-box.
