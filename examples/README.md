# Real-World Failure & Resilience Examples

This directory contains standalone, executable scripts and reference patterns demonstrating how `pipecat-session-continuity` handles severe, production failure modes beyond simple "happy path" reconnects.

---

## Example 1: Hard Process Termination (`01_hard_process_kill.py`)

### The Problem
Most libraries only test graceful disconnects where cleanup hooks (like Python's `try...finally` or `atexit`) can execute. In real cloud environments, backend workers terminate violently due to:
- Kubernetes Pod evictions / OOM Killer (`SIGKILL` / signal 9)
- Auto-scaling node terminations (AWS Spot / GCP Preemptible)
- Server power loss or container crashes (`taskkill /F` / `kill -9`)

### How It Works
1. **Worker Process A** spawns and processes a conversation turn, initiating a `schedule_appointment` tool call.
2. Context and tool execution are checkpointed to disk (`example_hard_kill_sessions.db`).
3. **Violent Termination**: Worker A abruptly aborts via `os._exit(137)`—completely bypassing normal Python teardown handlers.
4. **Worker Process B** launches as a completely fresh OS process with zero in-memory state.
5. The client reconnects with its signed HMAC session token.
6. Worker B restores the messages and verifies the tool call is identified as `completed`, completely preventing duplicate execution.

### Running the Example
```bash
python examples/01_hard_process_kill.py
```

---

## Example 2: Simulated Network Failures (`02_simulated_network_failures.py`)

### Scenarios Demonstrated

#### 1. Transient Socket Severing (TCP RST / WiFi Drops)
Simulates an abrupt network severance mid-conversation (e.g. driving through a tunnel or switching Wi-Fi access points). Demonstrates HMAC session token verification and instant conversation resumption.

#### 2. In-Flight Tool Drops (Network Drop During External API Call)
Simulates a connection failure occurring **while** an external action (e.g. credit card payment or database write) is in-flight:
- The action is recorded as `pending` before external invocation.
- When the network dies before completion, `resume_or_start()` detects the interrupted call upon reconnect.
- Automatically injects an explicit protective **System Notice** instructing the LLM:
  > *"[System Notice: The connection dropped while executing the tool 'charge_card'... Do NOT call this tool again... clarify outcome with user]"*
- Prevents double-charging or repeated executions.

#### 3. Mobile Sleep & App Backgrounding (Stale Resume Boundary)
Simulates a mobile user locking their phone screen or backgrounding the voice assistant app for longer than `stale_threshold_minutes`:
- Normal reconnects use a seamless, brief continuity acknowledgement.
- Reconnects after prolonged absence inject a warm, contextual re-engagement greeting:
  > *"[System Notice: The user was disconnected for a while and just reconnected. Welcome them back, acknowledge it's been a bit, and ask if they'd like to pick up where you left off.]"*

### Running the Example
```bash
python examples/02_simulated_network_failures.py
```

---

## Example 3: Client-Side Reconnect Flow & Backoff (`03_client_reconnect_flow.py`)

### Key Client Architecture Patterns

#### 1. Token Handshake & Persistent Client Storage
- Requests a cryptographically signed `{session_id, signature}` pair from `/create_session`.
- Stores credentials in browser `localStorage` or native mobile Keychain.

#### 2. Exponential Backoff with Decorrelated Jitter
When connections drop unexpectedly (close code != 1000), immediate reconnect storms can overwhelm servers ("thundering herd"). The client calculates:
```python
temp = min(max_delay, base_delay * (2 ** attempt))
delay = random.uniform(temp * 0.5, temp)
```

#### 3. Client Idempotency Tokens
Demonstrates client-supplied UUID tokens (`client_token = uuid4()`) bound to user actions. Even if network drops occur across retries, passing the same `client_token` guarantees exactly-once execution.

### Running the Example
```bash
python examples/03_client_reconnect_flow.py
```

---

## Production Testing Checklist

| Failure Scenario | How to Simulate in Production | Expected Library Behavior |
| :--- | :--- | :--- |
| **Worker SIGKILL** | `kubectl delete pod -n voice <pod_name> --now` | Context restored on next pod; 0 duplicate actions |
| **Silent Packet Loss** | Use [Toxiproxy](https://github.com/Shopify/toxiproxy) (`toxic: timeout` / `down`) | Client triggers backoff; resumes once path clears |
| **Mobile Backgrounding**| Background iOS/Android app for > 5 mins | Injects stale welcome-back bridge prompt |
| **Tampered Token** | Alter session query string in WebSocket URL | Server closes connection; rejects unauthorized resume |
| **Mid-Flight Payment Drop** | Kill connection between `record_tool_call` and `complete_tool_call` | Protective unconfirmed system notice injected |
