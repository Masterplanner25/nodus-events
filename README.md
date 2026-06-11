# nodus-events

**Distributed event bus with Redis pub/sub, source-instance deduplication,
and pre-rehydration buffering.**

Local-first: events are recorded in the in-process `AuditStore` and optionally
broadcast over Redis pub/sub when `[redis]` is installed. No required
external dependencies — the bus works fully in local mode without Redis.

> **Status:** v0.1.0 — published on [PyPI](https://pypi.org/project/nodus-events/).

---

## Install

```bash
pip install nodus-events

# With Redis pub/sub support:
pip install "nodus-events[redis]"
```

---

## What it provides

| Component | Purpose |
|---|---|
| `EventBus` | Publish events locally and optionally via Redis pub/sub |
| `EventBusConfig` | Connection settings, channel, instance ID, buffer config |
| `Event` | Typed event record with type, payload, correlation ID, timestamp |
| `AuditStore` / `InMemoryAuditStore` | Persistent event log (protocol + in-memory impl) |
| `get_event_bus()` | Process-level singleton factory |
| `publish_event()` | Convenience wrapper for one-line event emission |

---

## Quick start

```python
from nodus_events import publish_event, get_event_bus

# One-liner emission (uses process singleton)
publish_event("user.created", payload={"user_id": "u123"}, correlation_id="req-abc")

# Direct bus access
bus = get_event_bus()
bus.publish("flow.completed", payload={"flow_id": "f1"})

# Read audit log
events = bus.audit_store.list()
```

---

## Redis pub/sub

```python
from nodus_events import EventBusConfig, get_event_bus

config = EventBusConfig(
    redis_url="redis://localhost:6379",
    channel="nodus:events",
    instance_id="worker-1",   # dedup: ignores events from own instance
)

bus = get_event_bus(config)
bus.start_subscriber(callback=lambda event: print(event.event_type))
```

Events published by this instance are **not** re-delivered to its own
subscriber — `source_instance_id` deduplication prevents echo loops.

Stop the subscriber and flush buffered events:

```python
bus.stop()
buffered = bus.drain_buffered_events()   # events received before start_subscriber()
```

---

## Pre-rehydration buffering

Events that arrive over Redis before `start_subscriber()` is called are
buffered in memory. `drain_buffered_events()` returns them in order after
the subscriber starts — no events are lost during startup.

---

## EventBusConfig

```python
from nodus_events import EventBusConfig

config = EventBusConfig(
    redis_url=None,           # None → local-only mode (no Redis)
    channel="nodus:events",   # Redis pub/sub channel name
    enabled=True,
    instance_id="worker-1",   # unique per process/pod
    reconnect_delay_secs=5,
    max_buffer_size=1000,
)
```

---

## Event record

```python
from nodus_events import Event

# Event fields:
event.event_type          # str
event.correlation_id      # str | None
event.source_instance_id  # str
event.payload             # dict | None
event.timestamp           # float (UTC epoch seconds)
```

---

## AuditStore protocol

```python
from nodus_events import AuditStore, InMemoryAuditStore

store = InMemoryAuditStore()
store.record(event)
events = store.list()                    # all events
events = store.list(event_type="user.created")
events = store.list(limit=50)
```

Implement `AuditStore` to back the log with a database.

---

## Singleton management

```python
from nodus_events import get_event_bus, reset_event_bus

bus = get_event_bus(config)   # creates singleton on first call
bus2 = get_event_bus()        # returns same instance (config ignored)

# In tests — reset between cases:
reset_event_bus()
```

---

## Design

- **No required dependencies.** Local mode works with zero extras.
  Redis is opt-in via `[redis]` extra.
- **Source-instance dedup.** Each bus has a unique `instance_id`;
  events it published are not re-delivered to itself.
- **Pre-rehydration buffer.** Events arriving before `start_subscriber()`
  are buffered so no events are dropped during startup.
- **Thread-safe.** Singleton creation and audit store use `threading.Lock`.

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q
```

---

## License

MIT — see [LICENSE](LICENSE).
