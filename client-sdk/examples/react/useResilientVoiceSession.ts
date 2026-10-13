import { useEffect, useState, useRef, useCallback } from "react";
import {
  ResilientVoiceSession,
  ConnectionStatus,
  SessionCredentials,
  ResilientVoiceSessionOptions,
} from "../../src";

export interface UseResilientVoiceSessionResult {
  status: ConnectionStatus;
  isResumed: boolean;
  credentials: SessionCredentials | null;
  reconnectAttempt: number;
  connect: () => Promise<void>;
  disconnect: () => void;
  endCall: () => Promise<void>;
  sendAudio: (pcmChunk: ArrayBuffer | Int16Array) => void;
  sendToolResult: (toolName: string, result: any, clientToken?: string) => void;
  simulateDrop: () => void;
}

/**
 * React hook wrapping ResilientVoiceSession for voice applications.
 */
export function useResilientVoiceSession(
  options: ResilientVoiceSessionOptions
): UseResilientVoiceSessionResult {
  const [status, setStatus] = useState<ConnectionStatus>("idle");
  const [isResumed, setIsResumed] = useState<boolean>(false);
  const [credentials, setCredentials] = useState<SessionCredentials | null>(null);
  const [reconnectAttempt, setReconnectAttempt] = useState<number>(0);
  const sessionRef = useRef<ResilientVoiceSession | null>(null);

  useEffect(() => {
    const session = new ResilientVoiceSession({
      ...options,
      onStatusChange: (newStatus) => {
        setStatus(newStatus);
        options.onStatusChange?.(newStatus);
      },
      onConnected: (reconnected) => {
        setIsResumed(reconnected);
        setCredentials(session.getSessionInfo());
        setReconnectAttempt(0);
        options.onConnected?.(reconnected);
      },
      onResumed: (creds) => {
        setIsResumed(true);
        setCredentials(creds);
        options.onResumed?.(creds);
      },
      onReconnecting: (attempt, delayMs) => {
        setReconnectAttempt(attempt);
        options.onReconnecting?.(attempt, delayMs);
      },
    });

    sessionRef.current = session;

    return () => {
      session.disconnect("Component unmounted", 1000, false);
      sessionRef.current = null;
    };
  }, [options.wsUrl, options.createSessionUrl]);

  const connect = useCallback(async () => {
    if (sessionRef.current) {
      await sessionRef.current.connect();
    }
  }, []);

  const disconnect = useCallback(() => {
    sessionRef.current?.disconnect();
  }, []);

  const endCall = useCallback(async () => {
    if (sessionRef.current) {
      await sessionRef.current.endCall();
      setCredentials(null);
      setIsResumed(false);
    }
  }, []);

  const sendAudio = useCallback((pcmChunk: ArrayBuffer | Int16Array) => {
    sessionRef.current?.sendAudio(pcmChunk);
  }, []);

  const sendToolResult = useCallback((toolName: string, result: any, clientToken?: string) => {
    sessionRef.current?.sendToolResult(toolName, result, clientToken);
  }, []);

  const simulateDrop = useCallback(() => {
    sessionRef.current?.simulateDisconnect();
  }, []);

  return {
    status,
    isResumed,
    credentials,
    reconnectAttempt,
    connect,
    disconnect,
    endCall,
    sendAudio,
    sendToolResult,
    simulateDrop,
  };
}
