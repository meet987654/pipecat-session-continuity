# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - Unreleased
### Breaking
- **Storage fails closed.** Storage backends no longer turn errors into "no session". If the store cannot be read on resume (outage, I/O error, corrupt record), `resume_or_start`, `load_context`, `get_metadata` and `set_metadata` raise the new `StorageUnavailableError` instead of silently starting a fresh call. Starting fresh dropped the conversation and the idempotency records that block duplicate tool calls, and `set_metadata` during an outage saved empty messages and tools over the real session. A session that does not exist or has expired still starts fresh. Custom `BaseStorage` backends must raise on errors: any exception from `load` is wrapped in `StorageUnavailableError`, but a backend that returns `None` on an error still looks like a fresh session.
- **`RedisStorage` no longer falls back to in-process memory when Redis is unreachable at startup.** It raises `StorageUnavailableError`. Pass `allow_memory_fallback=True` (to `RedisStorage`, `SessionContinuityManager` or `SessionContinuity`) to restore the old behaviour for development and tests.
### Fixed
- **The resume bridge message now triggers the LLM** (`LLMMessagesAppendFrame(..., run_llm=True)`). Before, `resume_or_start` only appended it to the context, so after a reconnect the agent said nothing until the caller spoke. Same for the fresh-call greeting.
- **A drop before the commit no longer has to block the retry for good** (known issue in 0.3.0). If the connection dropped after a guarded call was recorded but before the backend performed it, the record stayed `pending` with no expiry, every identical retry was blocked, and the request was lost (5/5 in a live trial). Set a per-tool pending timeout or a status check (see Added). The default is unchanged.
- **The `retry_allowed` notice no longer tells the LLM to pass the idempotency key.** In a live run `openai/gpt-oss-120b` added an `idempotency_key` argument the tool did not declare, and the call crashed on every trial. The notice now says to call with exactly the same arguments and add nothing; the app reuses the key from the record.
- When a resumed session has unconfirmed (pending) tool calls, the final "connection restored" message now names the interrupted tool and tells the LLM to say the outcome can't be confirmed. With the generic message, 0/20 first replies mentioned the booking.
- Storage `save`/`delete` errors are now raised by the backends, so a failed checkpoint is counted as an error in metrics instead of a success. `checkpoint` still logs it and does not end the call.
### Added
- **Per-tool pending timeouts** (`SessionContinuity(pending_timeouts={...})`, `set_pending_timeout(tool, seconds)`). On resume, a record pending longer than its timeout goes to the tool's status check if one is registered; otherwise, or if the check cannot answer, it becomes the new `UNRESOLVED` status. Its notice says the outcome is unknown and asks the caller whether to retry the same request, and one identical retry is allowed under the same idempotency key. Live (one model, one caller script, one scenario): loss 0/20 when the drop came before the write, but 10/10 duplicates when it came after the write and the backend ignored the key. Use it only where your backend honours idempotency keys, or add a status check. See the table in the README.
- `StorageUnavailableError`, exported from the package and `pipecat_session_continuity.storage`.
- A warning is logged the first time a tool with no status check and no pending timeout is guarded.
- Tests: `test_pending_never_completed.py`, `test_timeout_after_commit.py` (retry after a committed drop duplicates with a backend that ignores the key, and is harmless with one that honours it), `test_storage_fail_closed.py`, and a `run_llm` regression test in `test_resume_message.py`.
### Changed
- **A status check that cannot answer is no longer treated like having no status check.** When a hook returns `unknown`, an untrusted `not_found`, raises, or times out, the record stays pending but gets a stricter notice: don't say it succeeded or failed, say it can't be confirmed, and don't suggest repeating it with different details.
- When a status check gives a definite answer, the final resume message tells the LLM to state that outcome first.
- `benchmarks/measure_pending_notice_claims.py` classifies replies as `CLAIMS_CONFIRMED`, `CLAIMS_NOT_DONE`, `FLAGS_UNCERTAIN` or `SILENT`, and retries on Groq rate limits.
### Known limitations
- **Steering: a rebooking with different arguments slips past the guard.** Another slot or a differently spelled name gets a new idempotency key. Not fixed. The pending-timeout notice no longer invites it (0/20 steered live), but the default "pending" notice still says "do not call this tool again", and in the baseline that pushed the agent to offer another slot.
- The default (no status check, no timeout) still loses a booking dropped before the write, for backward compatibility.

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
