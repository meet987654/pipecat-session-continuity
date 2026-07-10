# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
