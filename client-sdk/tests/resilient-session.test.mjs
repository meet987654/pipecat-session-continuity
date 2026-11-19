import test from "node:test";
import assert from "node:assert/strict";
import { ResilientVoiceSession, MemoryStorageAdapter } from "../dist/index.js";

class MockWebSocket {
  constructor(url) {
    this.url = url;
    this.readyState = 0; // 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
    this.binaryType = "arraybuffer";
    this.sentMessages = [];
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;

    // Auto-open on next tick
    setTimeout(() => {
      this.readyState = 1;
      if (this.onopen) this.onopen();
    }, 5);
  }

  send(data) {
    this.sentMessages.push(data);
  }

  close(code = 1000, reason = "") {
    this.readyState = 3;
    if (this.onclose) {
      this.onclose({ code, reason, wasClean: code === 1000 });
    }
  }

  simulateServerMessage(data) {
    if (this.onmessage) {
      this.onmessage({ data });
    }
  }
}

test("ResilientVoiceSession creates session and establishes WebSocket connection", async () => {
  const storage = new MemoryStorageAdapter();
  const createdSockets = [];

  const mockFetch = async (url, options) => {
    assert.equal(url, "/api/create_session");
    assert.equal(options.method, "POST");
    return {
      ok: true,
      status: 200,
      json: async () => ({
        session_id: "sess_mock_1",
        signature: "sig_mock_1",
      }),
    };
  };

  const session = new ResilientVoiceSession({
    wsUrl: "ws://localhost:8000/ws",
    createSessionUrl: "/api/create_session",
    storage,
    customFetch: mockFetch,
    webSocketFactory: (url) => {
      const sock = new MockWebSocket(url);
      createdSockets.push(sock);
      return sock;
    },
  });

  const statuses = [];
  session.on("status", (s) => statuses.push(s));

  await session.connect();

  assert.equal(session.getStatus(), "connected");
  assert.equal(createdSockets.length, 1);
  assert.ok(createdSockets[0].url.includes("session_id=sess_mock_1"));
  assert.ok(createdSockets[0].url.includes("signature=sig_mock_1"));
  assert.ok(!createdSockets[0].url.includes("reconnect=true"));

  // Check credentials stored
  assert.equal(storage.getItem("pipecat_session_id"), "sess_mock_1");
  assert.equal(storage.getItem("pipecat_session_sig"), "sig_mock_1");

  await session.endCall();
});

test("ResilientVoiceSession resumes session on abrupt drop with backoff", async () => {
  const storage = new MemoryStorageAdapter();
  const createdSockets = [];
  let resumedFired = false;
  let resumedCreds = null;

  const session = new ResilientVoiceSession({
    wsUrl: "ws://localhost:8000/ws",
    credentials: {
      sessionId: "existing_session_42",
      signature: "existing_sig_42",
      createdAt: Date.now(),
    },
    storage,
    initialBackoffMs: 20,
    maxBackoffMs: 50,
    jitter: false,
    webSocketFactory: (url) => {
      const sock = new MockWebSocket(url);
      createdSockets.push(sock);
      return sock;
    },
    onResumed: (creds) => {
      resumedFired = true;
      resumedCreds = creds;
    },
  });

  await session.connect();
  assert.equal(createdSockets.length, 1);
  assert.equal(session.getStatus(), "connected");

  // Simulate abrupt drop
  session.simulateDisconnect(1006, "Abrupt TCP Reset");

  // Wait for backoff timer to reconnect
  await new Promise((resolve) => setTimeout(resolve, 80));

  assert.equal(createdSockets.length, 2);
  assert.equal(session.getStatus(), "connected");
  assert.ok(createdSockets[1].url.includes("session_id=existing_session_42"));
  assert.ok(createdSockets[1].url.includes("reconnect=true"));
  assert.equal(resumedFired, true);
  assert.equal(resumedCreds.sessionId, "existing_session_42");

  await session.endCall();
});

test("ResilientVoiceSession sends audio buffers and tool results with idempotency tokens", async () => {
  const storage = new MemoryStorageAdapter();
  let currentSocket = null;

  const session = new ResilientVoiceSession({
    wsUrl: "ws://localhost:8000/ws",
    credentials: {
      sessionId: "audio_sess",
      signature: "audio_sig",
      createdAt: Date.now(),
    },
    storage,
    webSocketFactory: (url) => {
      currentSocket = new MockWebSocket(url);
      return currentSocket;
    },
  });

  await session.connect();

  // Test sendAudio
  const pcmData = new Int16Array([0, 100, -100, 200]);
  session.sendAudio(pcmData);

  assert.equal(currentSocket.sentMessages.length, 1);
  assert.ok(currentSocket.sentMessages[0] instanceof ArrayBuffer);

  // Test sendToolResult
  session.sendToolResult("check_balance", { balance: 1200 }, "cli_token_custom_1");

  assert.equal(currentSocket.sentMessages.length, 2);
  const payload = JSON.parse(currentSocket.sentMessages[1]);
  assert.equal(payload.type, "tool_result");
  assert.equal(payload.tool_name, "check_balance");
  assert.equal(payload.result.balance, 1200);
  assert.equal(payload.client_token, "cli_token_custom_1");

  await session.endCall();
});

test("ResilientVoiceSession clean endCall removes stored credentials", async () => {
  const storage = new MemoryStorageAdapter();

  const session = new ResilientVoiceSession({
    wsUrl: "ws://localhost:8000/ws",
    credentials: {
      sessionId: "end_call_sess",
      signature: "end_call_sig",
      createdAt: Date.now(),
    },
    storage,
    webSocketFactory: (url) => new MockWebSocket(url),
  });

  await session.connect();
  assert.ok(storage.getItem("pipecat_session_id"));

  await session.endCall();

  assert.equal(session.getStatus(), "disconnected");
  assert.equal(storage.getItem("pipecat_session_id"), null);
  assert.equal(storage.getItem("pipecat_session_sig"), null);
});
