/**
 * Connection states for ResilientVoiceSession.
 */
export type ConnectionStatus =
  | "idle"
  | "creating_session"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "disconnected"
  | "expired"
  | "error";

/**
 * Session authentication credentials returned by Pipecat continuity backend.
 */
export interface SessionCredentials {
  sessionId: string;
  signature: string;
  createdAt: number;
  ttlSeconds?: number;
}

/**
 * Interface for credential storage (supports localStorage, sessionStorage, or in-memory).
 */
export interface SessionStorageAdapter {
  getItem(key: string): string | null | Promise<string | null>;
  setItem(key: string, value: string): void | Promise<void>;
  removeItem(key: string): void | Promise<void>;
  clear?(): void | Promise<void>;
}

/**
 * Configuration options for ResilientVoiceSession.
 */
export interface ResilientVoiceSessionOptions {
  /**
   * Target WebSocket URL (e.g. "ws://localhost:8000/ws" or "wss://api.example.com/voice").
   * Query parameters for session_id and signature will be appended automatically.
   */
  wsUrl: string;

  /**
   * Optional HTTP endpoint for initial session creation (e.g. "/create_session").
   * If provided and no cached credentials exist, a POST request is made to obtain credentials.
   */
  createSessionUrl?: string;

  /**
   * Direct credentials if already known or minted server-side.
   */
  credentials?: SessionCredentials;

  /**
   * Storage backend for credentials caching. Defaults to localStorage (in browser) or MemoryStorageAdapter.
   */
  storage?: SessionStorageAdapter;

  /**
   * Prefix for storage keys. Default: "pipecat_session_"
   */
  storageKeyPrefix?: string;

  /**
   * Maximum session validity window before considering it expired. Default: 3600 (1 hour).
   */
  ttlSeconds?: number;

  /**
   * Maximum reconnection attempts before giving up and transitioning to "disconnected".
   * Default: 10 (use Infinity for endless retries).
   */
  maxReconnectAttempts?: number;

  /**
   * Initial backoff delay in milliseconds. Default: 500 ms.
   */
  initialBackoffMs?: number;

  /**
   * Maximum backoff delay in milliseconds. Default: 10000 ms (10 seconds).
   */
  maxBackoffMs?: number;

  /**
   * Multiplier for exponential backoff. Default: 2.0.
   */
  backoffFactor?: number;

  /**
   * Whether to apply decorrelated full jitter to backoff intervals to prevent thundering herds. Default: true.
   */
  jitter?: boolean;

  /**
   * Custom fetch function (useful for Node/SSR environments or custom headers).
   */
  customFetch?: typeof fetch;

  /**
   * Custom WebSocket factory (useful for Node.js environments or testing).
   */
  webSocketFactory?: (url: string) => any;

  /**
   * WebSocket binaryType ("arraybuffer" or "blob"). Default: "arraybuffer".
   */
  binaryType?: BinaryType;

  /**
   * Callback fired on any status change.
   */
  onStatusChange?: (status: ConnectionStatus, detail?: any) => void;

  /**
   * Callback fired when a dropped session is successfully resumed.
   */
  onResumed?: (credentials: SessionCredentials) => void;

  /**
   * Callback fired when connection opens (first connect or reconnect).
   */
  onConnected?: (isReconnect: boolean) => void;

  /**
   * Callback fired when data (audio buffer or JSON message) arrives from server.
   */
  onMessage?: (data: ArrayBuffer | string | Blob) => void;

  /**
   * Callback fired on error.
   */
  onError?: (err: any) => void;

  /**
   * Callback fired on socket closure.
   */
  onClose?: (event: { code: number; reason: string; wasClean: boolean }) => void;

  /**
   * Callback fired before a reconnection attempt is scheduled.
   */
  onReconnecting?: (attempt: number, delayMs: number) => void;
}

/**
 * Event map for typed event emitter.
 */
export interface ResilientSessionEventMap {
  status: (status: ConnectionStatus, detail?: any) => void;
  connected: (isReconnect: boolean) => void;
  resumed: (credentials: SessionCredentials) => void;
  message: (data: ArrayBuffer | string | Blob) => void;
  error: (err: any) => void;
  close: (event: { code: number; reason: string; wasClean: boolean }) => void;
  reconnecting: (attempt: number, delayMs: number) => void;
}
