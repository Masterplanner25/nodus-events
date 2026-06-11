# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] — 2026-05-30

Initial release.

### Added

- **`EventBusConfig`** — connection and behaviour settings. Fields:
  `redis_url` (None → local-only), `channel`, `enabled`, `instance_id`,
  `reconnect_delay_secs`, `max_buffer_size`.

- **`Event`** — typed event record. Fields: `event_type`, `correlation_id`,
  `source_instance_id`, `payload` (dict | None), `timestamp` (UTC epoch float).

- **`AuditStore`** — protocol: `record(event)`, `list(event_type?, limit?)`.

- **`InMemoryAuditStore`** — thread-safe in-memory `AuditStore` implementation.

- **`EventBus`** — publishes events locally (audit store) and optionally via
  Redis pub/sub.
  `publish(event_type, correlation_id?, payload?)` → bool.
  `start_subscriber(callback)` — background Redis subscriber thread.
  `stop()` — stops subscriber.
  `drain_buffered_events()` — returns events buffered before subscriber start.
  `get_status()` — dict with connection state and counters.

- **`get_event_bus(config?, audit_store?)`** — process-level singleton.
  First call creates; subsequent calls return cached instance.

- **`reset_event_bus()`** — clears the singleton (for test isolation).

- **`publish_event(event_type, correlation_id?, payload?)`** — one-line
  convenience wrapper around `get_event_bus().publish()`.

- **Source-instance deduplication** — `source_instance_id` prevents an
  instance from re-processing its own events via Redis.

- **Pre-rehydration buffering** — events received over Redis before
  `start_subscriber()` is called are held in a bounded buffer and returned
  by `drain_buffered_events()`.

- **17 tests** in `tests/test_events.py`. Uses `fakeredis` to test Redis
  paths without a live server.

- **No required dependencies** — local mode is pure stdlib. Optional
  `[redis]` extra adds `redis>=4.0.0`.

[0.1.0]: https://github.com/Masterplanner25/nodus-events/releases/tag/v0.1.0
