import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorageAdapter, LocalStorageAdapter } from "../dist/index.js";

test("MemoryStorageAdapter sets, gets, and removes values correctly", () => {
  const storage = new MemoryStorageAdapter();

  assert.equal(storage.getItem("missing"), null);

  storage.setItem("key1", "val1");
  assert.equal(storage.getItem("key1"), "val1");

  storage.removeItem("key1");
  assert.equal(storage.getItem("key1"), null);

  storage.setItem("a", "1");
  storage.setItem("b", "2");
  storage.clear();
  assert.equal(storage.getItem("a"), null);
  assert.equal(storage.getItem("b"), null);
});

test("LocalStorageAdapter operates cleanly with fallback in Node environment", () => {
  const storage = new LocalStorageAdapter();

  storage.setItem("test_key", "test_val");
  assert.equal(storage.getItem("test_key"), "test_val");

  storage.removeItem("test_key");
  assert.equal(storage.getItem("test_key"), null);
});
