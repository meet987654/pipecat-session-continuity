# @pipecat-ai/session-continuity-client

A lightweight, dependency-free TypeScript/JavaScript client helper for [pipecat-session-continuity](https://github.com/meet987654/pipecat-session-continuity).

Handles secure session token bootstrapping, automatic credential caching, exponential backoff with decorrelated full jitter, audio streaming, and client-side tool idempotency tokens.

---

## Features

- 🔄 **Automatic Reconnection with Jittered Backoff**: Recovers seamlessly from WiFi drops, cellular handoffs, and process kills using exponential backoff with decorrelated full jitter to prevent thundering herds.
- 🔐 **HMAC Session Handshake**: Bootstraps secure tokens via `POST /create_session` and transparently caches session ID, signature, and timestamps in `localStorage` (or memory).
- 🏷️ **Client-Side Tool Idempotency**: Automatically creates high-entropy client tokens (`cli_tok_...`) and canonicalizes tool arguments to guarantee duplicate tool calls are trapped at the agent registry.
- ⚡ **Zero External Dependencies**: Implemented in clean TypeScript with zero runtime dependencies. Works in browsers, Node.js, Next.js, and React Native.
- 🎙️ **Audio-First API**: Helpers for streaming raw PCM audio chunks directly to Pipecat voice pipelines (`sendAudio`).

---

## Installation

```bash
# From within the repository or as an npm dependency
npm install ./client-sdk
```

---

## Quickstart (Vanilla TypeScript / JavaScript)

```typescript
import { ResilientVoiceSession } from "@pipecat-ai/session-continuity-client";

const session = new ResilientVoiceSession({
  wsUrl: "ws://localhost:8000/ws",
  createSessionUrl: "/create_session",
  initialBackoffMs: 500,
  maxBackoffMs: 10000,
  jitter: true,

  onConnected: (isReconnect) => {
    console.log(isReconnect ? "Reconnected to voice agent!" : "Connected to new voice agent!");
  },

  onResumed: (creds) => {
    console.log(`Resumed session ${creds.sessionId} without conversation context loss!`);
  },

  onStatusChange: (status) => {
    console.log("Session status:", status); // "connecting" | "connected" | "reconnecting" | etc.
  },

  onMessage: (data) => {
    if (data instanceof ArrayBuffer) {
      // Incoming raw PCM audio chunk from Pipecat TTS
      playPcmAudio(data);
    } else {
      console.log("Server message:", data);
    }
  },
});

// Start session
await session.connect();

// Stream microphone PCM audio
navigator.mediaDevices.getUserMedia({ audio: true }).then((stream) => {
  // Capture 16kHz PCM Int16 audio chunks and stream
  session.sendAudio(pcmChunk);
});

// Trigger a client tool call with idempotency protection
session.sendToolResult("check_order_status", { order_id: "ORD-9912" });

// End call cleanly when user hangs up
await session.endCall();
```

---

## React Integration

You can easily integrate `ResilientVoiceSession` with a custom React hook:

```tsx
import { useEffect, useState, useRef } from "react";
import { ResilientVoiceSession, ConnectionStatus } from "@pipecat-ai/session-continuity-client";

export function useResilientVoiceSession(wsUrl: string) {
  const [status, setStatus] = useState<ConnectionStatus>("idle");
  const [isResumed, setIsResumed] = useState(false);
  const sessionRef = useRef<ResilientVoiceSession | null>(null);

  useEffect(() => {
    const session = new ResilientVoiceSession({
      wsUrl,
      createSessionUrl: "/create_session",
      onStatusChange: setStatus,
      onResumed: () => setIsResumed(true),
    });

    sessionRef.current = session;

    return () => {
      session.disconnect("Component unmounted", 1000, false);
    };
  }, [wsUrl]);

  const connect = () => sessionRef.current?.connect();
  const endCall = () => sessionRef.current?.endCall();
  const sendAudio = (buffer: ArrayBuffer) => sessionRef.current?.sendAudio(buffer);

  return { status, isResumed, connect, endCall, sendAudio };
}
```

---

## API Reference

### `ResilientVoiceSession(options)`

#### Options
| Option | Type | Default | Description |
|---|---|---|---|
| `wsUrl` | `string` | *(required)* | WebSocket endpoint URL. |
| `createSessionUrl` | `string` | `undefined` | Endpoint for `POST /create_session` token generation. |
| `credentials` | `SessionCredentials` | `undefined` | Pre-minted session credentials (bypasses HTTP creation). |
| `storage` | `SessionStorageAdapter` | `LocalStorageAdapter` | Storage adapter (`localStorage` or `MemoryStorageAdapter`). |
| `ttlSeconds` | `number` | `3600` | Session validity window in seconds. |
| `maxReconnectAttempts` | `number` | `10` | Max reconnection retries before failing. |
| `initialBackoffMs` | `number` | `500` | Initial retry backoff in ms. |
| `maxBackoffMs` | `number` | `10000` | Maximum retry backoff cap in ms. |
| `jitter` | `boolean` | `true` | Apply decorrelated full jitter to backoff intervals. |

#### Methods
- `connect(): Promise<void>`: Connects to the voice agent (creating credentials if necessary).
- `sendAudio(pcmChunk: ArrayBuffer | Int16Array): void`: Sends raw audio bytes.
- `sendToolResult(toolName: string, result: any, clientToken?: string): void`: Dispatches an idempotent tool execution payload.
- `generateToolIdempotencyToken(prefix?: string): string`: Mint a unique `cli_tok_...` token.
- `simulateDisconnect(code?: number, reason?: string): void`: Simulate sudden connection drop for automated testing.
- `disconnect(reason?: string, code?: number, purgeCredentials?: boolean): void`: Pause or drop socket.
- `endCall(): Promise<void>`: Hang up cleanly and remove cached credentials from local storage.

---

## Testing

Run unit tests via Node's native test runner:

```bash
npm test
```
