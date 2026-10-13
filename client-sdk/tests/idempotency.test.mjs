import test from "node:test";
import assert from "node:assert/strict";
import { generateClientToken, canonicalizeArguments } from "../dist/index.js";

test("generateClientToken produces prefixed unique non-empty string", () => {
  const token1 = generateClientToken("custom_");
  const token2 = generateClientToken("custom_");

  assert.ok(token1.startsWith("custom_"));
  assert.ok(token2.startsWith("custom_"));
  assert.notEqual(token1, token2);
});

test("canonicalizeArguments handles null and primitives", () => {
  assert.equal(canonicalizeArguments(null), "{}");
  assert.equal(canonicalizeArguments(undefined), "{}");
  assert.equal(canonicalizeArguments("hello"), '"hello"');
  assert.equal(canonicalizeArguments(42), "42");
});

test("canonicalizeArguments produces deterministic sorted JSON regardless of key order", () => {
  const objA = { z: 1, a: "foo", m: true };
  const objB = { a: "foo", m: true, z: 1 };

  const canonicalA = canonicalizeArguments(objA);
  const canonicalB = canonicalizeArguments(objB);

  assert.equal(canonicalA, canonicalB);
  assert.equal(canonicalA, '{"a":"foo","m":true,"z":1}');
});

test("canonicalizeArguments handles nested objects deterministically", () => {
  const nestedA = { outer: { b: 2, a: 1 } };
  const nestedB = { outer: { a: 1, b: 2 } };

  assert.equal(canonicalizeArguments(nestedA), canonicalizeArguments(nestedB));
});
