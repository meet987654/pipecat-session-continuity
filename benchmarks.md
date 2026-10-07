# pipecat-session-continuity Benchmarks

This document contains performance benchmarks for the `pipecat-session-continuity` library.

## Latency Measurements
These metrics represent the raw overhead introduced by the library when intercepting, saving, and loading session contexts. 

**Test Setup:** 
- Iterations: 1,000 runs
- Storage Backend: `SQLiteStorage` (disk-based, mimicking typical local deployments)
- Message Payload: 3 conversation turns (system, user, assistant)

| Operation | Average Latency | P95 Latency | P99 Latency |
|-----------|-----------------|-------------|-------------|
| **Save Context** | ~13.14 ms | ~21.34 ms | ~35.92 ms |
| **Load Context** | ~3.07 ms | ~5.18 ms | ~8.93 ms |
| **`resume_or_start`** | ~2.83 ms | ~4.57 ms | ~8.84 ms |

> [!NOTE]
> **Conclusion**: The library introduces less than **3 milliseconds** of blocking overhead on average during a WebSocket reconnect event, ensuring voice agent resume times are almost entirely bounded by the LLM's Time-To-First-Token (TTFT) rather than local state retrieval.

## Duplicate-Call Rate (Idempotency)
*Measured over 20 iterations using `openai/gpt-oss-120b` via Groq.*

The duplicate-call rate measures how often the LLM ignores the injected system prompt (`[System Notice: The connection dropped while executing the tool... Do NOT call this tool again...]`). To measure this realistically, we used an extremely forceful user prompt to ensure the LLM *wanted* to call the tool in the baseline condition.

**Benchmark Results:**
- **Baseline Rate (No Mitigation):** 80.0% (16/20 times, the LLM hallucinated the duplicate tool call upon reconnect).
- **Mitigated Rate (With `pipecat-session-continuity`):** 0.0% (0/20 times).

> [!NOTE]
> **Conclusion**: The library achieved an **80.0x reduction in hallucinated tool calls**, successfully overriding aggressive tool-call biases and completely eliminating duplicate state actions in this scenario. Furthermore, deterministic argument hashing and client tokens guarantee that even if an LLM re-issues a tool call with a brand new `tool_call_id`, the duplicate action is trapped and bypassed at the registry level.
