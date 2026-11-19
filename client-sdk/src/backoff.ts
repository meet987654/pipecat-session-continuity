/**
 * Calculates exponential backoff with optional decorrelated full jitter.
 *
 * Full jitter spreads reconnect attempts across the time interval, preventing
 * the "thundering herd" problem when an edge proxy or media server restarts.
 *
 * @param attempt 1-indexed retry attempt number (1, 2, 3...)
 * @param initialBackoffMs Initial base backoff (e.g. 500ms)
 * @param maxBackoffMs Maximum cap on backoff (e.g. 10000ms)
 * @param backoffFactor Multiplier (e.g. 2.0)
 * @param jitter Whether to randomize between 50% and 100% of the computed window
 * @param randomFn Optional custom random function for unit testing
 */
export function calculateBackoff(
  attempt: number,
  initialBackoffMs: number = 500,
  maxBackoffMs: number = 10000,
  backoffFactor: number = 2.0,
  jitter: boolean = true,
  randomFn: () => number = Math.random
): number {
  if (attempt <= 0) return 0;

  const raw = initialBackoffMs * Math.pow(backoffFactor, attempt - 1);
  const capped = Math.min(maxBackoffMs, raw);

  if (!jitter) {
    return Math.round(capped);
  }

  // Decorrelated full jitter between 50% and 100% of computed cap
  const minInterval = capped * 0.5;
  const spread = capped * 0.5;
  const delay = minInterval + randomFn() * spread;

  return Math.round(delay);
}
