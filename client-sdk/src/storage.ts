import { SessionStorageAdapter } from "./types";

/**
 * In-memory storage adapter for non-browser or test environments.
 */
export class MemoryStorageAdapter implements SessionStorageAdapter {
  private map: Map<string, string> = new Map();

  getItem(key: string): string | null {
    return this.map.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.map.set(key, value);
  }

  removeItem(key: string): void {
    this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }
}

/**
 * LocalStorage adapter that safely detects browser environment.
 */
export class LocalStorageAdapter implements SessionStorageAdapter {
  private isAvailable: boolean;
  private fallback: MemoryStorageAdapter;

  constructor() {
    this.fallback = new MemoryStorageAdapter();
    this.isAvailable = typeof window !== "undefined" && typeof window.localStorage !== "undefined";
  }

  getItem(key: string): string | null {
    if (this.isAvailable) {
      try {
        return window.localStorage.getItem(key);
      } catch {
        return this.fallback.getItem(key);
      }
    }
    return this.fallback.getItem(key);
  }

  setItem(key: string, value: string): void {
    if (this.isAvailable) {
      try {
        window.localStorage.setItem(key, value);
        return;
      } catch {
        this.fallback.setItem(key, value);
        return;
      }
    }
    this.fallback.setItem(key, value);
  }

  removeItem(key: string): void {
    if (this.isAvailable) {
      try {
        window.localStorage.removeItem(key);
        return;
      } catch {
        this.fallback.removeItem(key);
        return;
      }
    }
    this.fallback.removeItem(key);
  }

  clear(): void {
    if (this.isAvailable) {
      try {
        window.localStorage.clear();
        return;
      } catch {
        this.fallback.clear();
        return;
      }
    }
    this.fallback.clear();
  }
}

/**
 * Helper to obtain the best default storage adapter.
 */
export function getDefaultStorage(): SessionStorageAdapter {
  return new LocalStorageAdapter();
}
