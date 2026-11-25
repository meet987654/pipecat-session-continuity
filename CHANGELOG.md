# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
