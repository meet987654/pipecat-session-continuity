/**
 * Generates a unique, high-entropy client-side idempotency token for tool execution.
 *
 * Attaching this token to client-triggered actions prevents double execution
 * across sudden WebSocket disconnects or retry loops.
 */
export function generateClientToken(prefix: string = "cli_tok_"): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}${crypto.randomUUID()}`;
  }

  // Fallback random string
  const rnd = Math.random().toString(36).substring(2, 12);
  const ts = Date.now().toString(36);
  return `${prefix}${ts}_${rnd}`;
}

/**
 * Recursively canonicalizes arguments into deterministic JSON representation.
 */
export function canonicalizeArguments(args: any): string {
  if (args === null || args === undefined) {
    return "{}";
  }

  if (typeof args !== "object") {
    return JSON.stringify(args);
  }

  if (Array.isArray(args)) {
    return JSON.stringify(args.map((item) => JSON.parse(canonicalizeArguments(item))));
  }

  const sortedKeys = Object.keys(args).sort();
  const sortedObj: Record<string, any> = {};
  for (const k of sortedKeys) {
    sortedObj[k] = JSON.parse(canonicalizeArguments(args[k]));
  }
  return JSON.stringify(sortedObj);
}
