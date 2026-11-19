import {
  ConnectionStatus,
  ResilientSessionEventMap,
  ResilientVoiceSessionOptions,
  SessionCredentials,
  SessionStorageAdapter,
} from "./types";
import { getDefaultStorage } from "./storage";
import { calculateBackoff } from "./backoff";
import { generateClientToken } from "./idempotency";

/**
 * ResilientVoiceSession provides production-grade WebSocket connection resilience,
 * HMAC session authentication, automated reconnection with decorrelated jittered backoff,
 * and client-side tool idempotency for Pipecat voice agents.
 */
export class ResilientVoiceSession {
  private options: ResilientVoiceSessionOptions;
  private storage: SessionStorageAdapter;
  private prefix: string;
  private credentials: SessionCredentials | null = null;
  private status: ConnectionStatus = "idle";
  private socket: any = null;
  private reconnectAttempt: number = 0;
  private reconnectTimer: any = null;
  private userInitiatedClose: boolean = false;
  private listeners: Map<keyof ResilientSessionEventMap, Set<Function>> = new Map();

  constructor(options: ResilientVoiceSessionOptions) {
    this.options = {
      ttlSeconds: 3600,
      maxReconnectAttempts: 10,
      initialBackoffMs: 500,
      maxBackoffMs: 10000,
      backoffFactor: 2.0,
      jitter: true,
      binaryType: "arraybuffer",
      ...options,
    };

    this.prefix = this.options.storageKeyPrefix || "pipecat_session_";
    this.storage = this.options.storage || getDefaultStorage();

    if (this.options.credentials) {
      this.credentials = { ...this.options.credentials };
      this.persistCredentials(this.credentials);
    }
  }

  // --- Event Handling ---

  public on<K extends keyof ResilientSessionEventMap>(
    event: K,
    listener: ResilientSessionEventMap[K]
  ): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
    return this;
  }

  public off<K extends keyof ResilientSessionEventMap>(
    event: K,
    listener: ResilientSessionEventMap[K]
  ): this {
    const set = this.listeners.get(event);
    if (set) {
      set.delete(listener);
    }
    return this;
  }

  private emit<K extends keyof ResilientSessionEventMap>(
    event: K,
    ...args: Parameters<ResilientSessionEventMap[K]>
  ): void {
    const set = this.listeners.get(event);
    if (set) {
      for (const fn of set) {
        try {
          (fn as any)(...args);
        } catch (err) {
          console.error(`Error in ResilientVoiceSession event listener for '${String(event)}':`, err);
        }
      }
    }
  }

  private setStatus(newStatus: ConnectionStatus, detail?: any): void {
    this.status = newStatus;
    if (this.options.onStatusChange) {
      this.options.onStatusChange(newStatus, detail);
    }
    this.emit("status", newStatus, detail);
  }

  // --- Storage & Credentials ---

  private async loadStoredCredentials(): Promise<SessionCredentials | null> {
    try {
      const id = await this.storage.getItem(`${this.prefix}id`);
      const sig = await this.storage.getItem(`${this.prefix}sig`);
      const createdStr = await this.storage.getItem(`${this.prefix}start`);

      if (id && sig) {
        const createdAt = createdStr ? parseInt(createdStr, 10) : Date.now();
        return { sessionId: id, signature: sig, createdAt, ttlSeconds: this.options.ttlSeconds };
      }
    } catch (err) {
      console.warn("Failed to retrieve cached credentials from storage", err);
    }
    return null;
  }

  private async persistCredentials(creds: SessionCredentials): Promise<void> {
    try {
      await this.storage.setItem(`${this.prefix}id`, creds.sessionId);
      await this.storage.setItem(`${this.prefix}sig`, creds.signature);
      await this.storage.setItem(`${this.prefix}start`, creds.createdAt.toString());
    } catch (err) {
      console.warn("Failed to persist session credentials to storage", err);
    }
  }

  public async clearStoredCredentials(): Promise<void> {
    try {
      await this.storage.removeItem(`${this.prefix}id`);
      await this.storage.removeItem(`${this.prefix}sig`);
      await this.storage.removeItem(`${this.prefix}start`);
      this.credentials = null;
    } catch (err) {
      console.warn("Failed to clear session credentials", err);
    }
  }

  public getSessionInfo(): SessionCredentials | null {
    return this.credentials ? { ...this.credentials } : null;
  }

  public getStatus(): ConnectionStatus {
    return this.status;
  }

  public isConnected(): boolean {
    return this.status === "connected" && this.socket?.readyState === 1; // 1 = OPEN
  }

  // --- Session Initiation ---

  private isSessionExpired(creds: SessionCredentials): boolean {
    const ttl = (creds.ttlSeconds || this.options.ttlSeconds || 3600) * 1000;
    return Date.now() - creds.createdAt > ttl;
  }

  public async createSession(): Promise<SessionCredentials> {
    if (!this.options.createSessionUrl) {
      throw new Error(
        "Cannot create new session: neither 'createSessionUrl' nor initial 'credentials' were provided."
      );
    }

    this.setStatus("creating_session");
    const fetchFn = this.options.customFetch || fetch;

    try {
      const res = await fetchFn(this.options.createSessionUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      if (!res.ok) {
        throw new Error(`Failed to create session HTTP status ${res.status}: ${res.statusText}`);
      }

      const data = await res.json();
      const credentials: SessionCredentials = {
        sessionId: data.session_id,
        signature: data.signature,
        createdAt: Date.now(),
        ttlSeconds: this.options.ttlSeconds,
      };

      this.credentials = credentials;
      await this.persistCredentials(credentials);
      return credentials;
    } catch (err) {
      this.setStatus("error", err);
      if (this.options.onError) this.options.onError(err);
      this.emit("error", err);
      throw err;
    }
  }

  // --- Connection & Lifecycle ---

  public async connect(): Promise<void> {
    this.userInitiatedClose = false;
    this.clearReconnectTimer();

    // Check credentials in memory or storage
    if (!this.credentials) {
      this.credentials = await this.loadStoredCredentials();
    }

    if (this.credentials && this.isSessionExpired(this.credentials)) {
      await this.clearStoredCredentials();
      this.setStatus("expired");
    }

    if (!this.credentials) {
      if (this.options.createSessionUrl) {
        await this.createSession();
      } else {
        throw new Error("No credentials available and createSessionUrl not specified.");
      }
    }

    await this.establishWebSocket(false);
  }

  private buildWebSocketUrl(isReconnect: boolean): string {
    const creds = this.credentials!;
    const url = new URL(this.options.wsUrl, typeof window !== "undefined" ? window.location.href : "http://localhost");
    url.searchParams.set("session_id", creds.sessionId);
    url.searchParams.set("signature", creds.signature);

    if (isReconnect || this.reconnectAttempt > 0) {
      url.searchParams.set("reconnect", "true");
    }

    return url.toString();
  }

  private establishWebSocket(isReconnect: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      this.setStatus(isReconnect ? "reconnecting" : "connecting");
      const fullUrl = this.buildWebSocketUrl(isReconnect);

      let ws: any;
      if (this.options.webSocketFactory) {
        ws = this.options.webSocketFactory(fullUrl);
      } else if (typeof WebSocket !== "undefined") {
        ws = new WebSocket(fullUrl);
      } else {
        const error = new Error("WebSocket is not defined in this environment. Provide options.webSocketFactory.");
        this.setStatus("error", error);
        reject(error);
        return;
      }

      if (this.options.binaryType && "binaryType" in ws) {
        ws.binaryType = this.options.binaryType;
      }

      let connectionResolved = false;

      ws.onopen = () => {
        connectionResolved = true;
        this.socket = ws;
        const wasResume = isReconnect || this.reconnectAttempt > 0;
        this.reconnectAttempt = 0;
        this.setStatus("connected");

        if (this.options.onConnected) this.options.onConnected(wasResume);
        this.emit("connected", wasResume);

        if (wasResume) {
          if (this.options.onResumed) this.options.onResumed(this.credentials!);
          this.emit("resumed", this.credentials!);
        }

        resolve();
      };

      ws.onmessage = (event: any) => {
        const data = event.data;
        if (this.options.onMessage) this.options.onMessage(data);
        this.emit("message", data);
      };

      ws.onerror = (err: any) => {
        if (!connectionResolved && !isReconnect) {
          reject(err);
        }
        if (this.options.onError) this.options.onError(err);
        this.emit("error", err);
      };

      ws.onclose = (event: any) => {
        this.socket = null;
        const code = event?.code ?? 1000;
        const reason = event?.reason ?? "";
        const wasClean = event?.wasClean ?? (code === 1000 || this.userInitiatedClose);

        const closeInfo = { code, reason, wasClean };
        if (this.options.onClose) this.options.onClose(closeInfo);
        this.emit("close", closeInfo);

        if (this.userInitiatedClose || code === 1000 || reason === "Clean Disconnect") {
          this.setStatus("disconnected", closeInfo);
          return;
        }

        // Abnormal closure: check expiration then trigger auto-reconnect backoff
        if (this.credentials && this.isSessionExpired(this.credentials)) {
          this.clearStoredCredentials();
          this.setStatus("expired");
          return;
        }

        this.handleAutoReconnect();
      };
    });
  }

  private handleAutoReconnect(): void {
    const maxAttempts = this.options.maxReconnectAttempts ?? 10;
    if (this.reconnectAttempt >= maxAttempts) {
      this.setStatus("disconnected", { reason: "Max reconnection attempts reached" });
      return;
    }

    this.reconnectAttempt++;
    const delay = calculateBackoff(
      this.reconnectAttempt,
      this.options.initialBackoffMs,
      this.options.maxBackoffMs,
      this.options.backoffFactor,
      this.options.jitter
    );

    this.setStatus("reconnecting", { attempt: this.reconnectAttempt, delayMs: delay });

    if (this.options.onReconnecting) {
      this.options.onReconnecting(this.reconnectAttempt, delay);
    }
    this.emit("reconnecting", this.reconnectAttempt, delay);

    this.clearReconnectTimer();
    this.reconnectTimer = setTimeout(async () => {
      if (!this.userInitiatedClose) {
        try {
          await this.establishWebSocket(true);
        } catch {
          // Failure in establishWebSocket will trigger onclose which schedules next retry
        }
      }
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  // --- Public Operations ---

  public send(data: string | ArrayBuffer | Blob): void {
    if (!this.socket || this.socket.readyState !== 1) {
      throw new Error("Cannot send: WebSocket is not open.");
    }
    this.socket.send(data);
  }

  public sendAudio(pcmChunk: ArrayBuffer | Int16Array): void {
    const buffer = pcmChunk instanceof Int16Array ? pcmChunk.buffer : pcmChunk;
    this.send(buffer as ArrayBuffer);
  }

  public sendToolResult(
    toolName: string,
    result: any,
    clientToken?: string
  ): void {
    const payload = JSON.stringify({
      type: "tool_result",
      tool_name: toolName,
      result,
      client_token: clientToken || this.generateToolIdempotencyToken(),
    });
    this.send(payload);
  }

  public generateToolIdempotencyToken(prefix?: string): string {
    return generateClientToken(prefix);
  }

  public disconnect(reason: string = "User Disconnect", code: number = 1000, purgeCredentials: boolean = false): void {
    this.userInitiatedClose = true;
    this.clearReconnectTimer();

    if (purgeCredentials) {
      this.clearStoredCredentials();
    }

    if (this.socket) {
      try {
        this.socket.close(code, reason);
      } catch {
        // Ignored
      }
      this.socket = null;
    }

    this.setStatus("disconnected", { code, reason });
  }

  /**
   * Concludes the call cleanly, closing the socket and removing cached session credentials.
   */
  public async endCall(): Promise<void> {
    this.disconnect("Clean Disconnect", 1000, true);
  }

  /**
   * Simulates an abrupt network failure (e.g. WiFi drop or TCP RST) to validate
   * auto-reconnect and session restoration behavior.
   */
  public simulateDisconnect(code: number = 1006, reason: string = "Abrupt Network Drop"): void {
    this.userInitiatedClose = false;
    if (this.socket) {
      try {
        this.socket.close(code, reason);
      } catch {
        // Ignored
      }
    }
  }
}
