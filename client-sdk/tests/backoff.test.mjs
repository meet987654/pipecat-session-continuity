import test from "node:test";
import assert from "node:assert/strict";
import { calculateBackoff } from "../dist/index.js";

test("calculateBackoff returns 0 for non-positive attempts", () => {
  assert.equal(calculateBackoff(0), 0);
  assert.equal(calculateBackoff(-1), 0);
});

test("calculateBackoff computes exponential progression without jitter", () => {
  const base = 500;
  const max = 10000;
  const factor = 2.0;

  assert.equal(calculateBackoff(1, base, max, factor, false), 500);
  assert.equal(calculateBackoff(2, base, max, factor, false), 1000);
  assert.equal(calculateBackoff(3, base, max, factor, false), 2000);
  assert.equal(calculateBackoff(4, base, max, factor, false), 4000);
  assert.equal(calculateBackoff(5, base, max, factor, false), 8000);
  assert.equal(calculateBackoff(6, base, max, factor, false), 10000); // capped at max
  assert.equal(calculateBackoff(10, base, max, factor, false), 10000); // capped at max
});

test("calculateBackoff applies decorrelated jitter within 50% to 100% window", () => {
  const base = 1000;
  const max = 8000;
  const factor = 2.0;

  // With random = 0.0 -> minimum (50% of 1000 = 500)
  const minVal = calculateBackoff(1, base, max, factor, true, () => 0.0);
  assert.equal(minVal, 500);

  // With random = 1.0 -> maximum (100% of 1000 = 1000)
  const maxVal = calculateBackoff(1, base, max, factor, true, () => 1.0);
  assert.equal(maxVal, 1000);

  // With random = 0.5 -> midpoint (75% of 1000 = 750)
  const midVal = calculateBackoff(1, base, max, factor, true, () => 0.5);
  assert.equal(midVal, 750);
});
