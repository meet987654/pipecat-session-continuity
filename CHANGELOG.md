# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Known issue in 0.3.0
- **A drop before the commit can permanently block the retry when no status check is registered.** If the connection drops after a guarded call is recorded but before the backend performs it, the record stays `pending` with no expiry, and every identical retry is blocked for the rest of the session: the action never happens and the caller's request is lost. The 0.3.0 resume notice ("Do NOT call this tool again") also pushes the agent towards offering different details, which the guard cannot catch. Workaround on 0.3.0: register a status check for every guarded tool.
### Fixed
- **The `retry_allowed` notice no longer tells the LLM to pass the idempotency key.** It said "call it once more ... passing idempotency key '...'"; in a live run `openai/gpt-oss-120b` added an `idempotency_key` argument the tool did not declare, and the tool call crashed on every trial. The app already reuses the key from the record. The notice now says to call with exactly the same arguments and add nothing.
### Added
- **Per-tool pending timeouts** (`SessionContinuity(pending_timeouts={...})`, `set_pending_timeout(tool, seconds)`). On resume, a record pending longer than its timeout goes to the tool's status check if one is registered; otherwise, or if the check cannot answer, it becomes the new `UNRESOLVED` status. Its notice says the outcome is unknown and tells the LLM to ask the caller whether to retry the same request (no "do not call again", no offering other details), and `check_tool_idempotency` allows one identical retry under the same idempotency key. Default (no timeout) behaviour is unchanged.
- A warning is logged the first time a tool with no status check is guarded (`record_tool_call`). Status checks are registered after the constructor, so this is checked at first use rather than at construction.
- `tests/test_pending_never_completed.py`: drop before commit, default still blocks (and warns), timed-out records escalate or go to the status check.
### Changed
- **A status check that cannot answer is no longer treated like having no status check.** When a registered hook returns `unknown`, an untrusted `not_found`, raises, or times out, the record stays pending (still guarded) but gets a stricter notice: the LLM must not say the action succeeded or failed for the rest of the call, must say it cannot confirm, and must not suggest repeating it with different details. It is no longer told to ask the caller whether they got a confirmation, since the backend has already been asked. Tools without a hook keep the existing "unconfirmed, ask the caller" notice.
- When a status check gives a definite answer (`completed`, or `not_found` with `retry_on_not_found=True`), the final resume message now tells the LLM to state that outcome first. Previously it got the generic "ask how you can continue" message and the first reply often didn't mention the request.
### Fixed
- When a resumed session has unconfirmed (pending) tool calls, the final "connection restored" message the LLM answers now names the interrupted tool and tells it to say the outcome can't be confirmed and ask the caller. In a 20-trial check with `openai/gpt-oss-120b`, the generic message made the model ignore the pending-tool system notice (0/20 mentioned the booking). Behaviour is unchanged when nothing is pending or a status check resolved it.
### Changed
- `benchmarks/measure_pending_notice_claims.py` now classifies replies as `CLAIMS_CONFIRMED`, `CLAIMS_NOT_DONE`, `FLAGS_UNCERTAIN` or `SILENT`, and retries on Groq rate limits.

## [0.3.0] - 2026-11-19
### Added
- **Status-check hooks**: `SessionContinuity.register_status_check(tool_name, fn)`. On resume, pending records for that tool are checked via `fn(idempotency_key, args)`: `completed` marks the record done and tells the LLM the confirmed result; `not_found` is treated like `unknown` unless the hook is registered with `retry_on_not_found=True`, in which case exactly one retry is allowed under the same idempotency key (new `retry_allowed` status); `unknown`, an exception, or a timeout (`status_check_timeout_seconds`, default 1 s) keeps the existing "unconfirmed" notice.
- `tests/test_lost_reply.py` (booking lands, reply dropped, no double booking) and `tests/test_status_check.py`.
- `examples/06_status_check_hook.py`, `benchmarks/measure_status_check_latency.py`, `benchmarks/measure_pending_notice_claims.py`.
### Changed
- Behaviour is unchanged for tools without a registered hook.

## [0.2.1] - 2026-07-10
### Fixed
- 0.2.0 was missing the `storage` subpackage in the published wheel; fixed setuptools package discovery in `pyproject.toml`.
## [0.2.0] - 2026-07-10
### Added
- **Storage Abstraction**: Added `BaseStorage` interface with `RedisStorage` and `SQLiteStorage` backends. SQLite is a zero-setup persistent option leveraging local files.
### Changed
- **BREAKING**: `SessionContinuityManager.__init__` now accepts a `storage_backend` instance instead of `redis_url` directly (though `redis_url` is still supported via fallback for backward compatibility).

## [0.1.0] - 2026-07-10
### Added
- Initial release of `pipecat-session-continuity`.
- Redis-backed session continuity for Pipecat.
- Graceful recovery from hard process kills.
- HMAC security tokens for session validation.
