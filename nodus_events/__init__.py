"""nodus-events — distributed event emission with Redis pub/sub and local fallback.

Config:
    EventBusConfig  — redis_url, channel, enabled, instance_id, buffer/reconnect settings

Events:
    Event           — event_type, correlation_id, source_instance_id, payload, timestamp

Audit:
    AuditStore      — protocol for persistent event logs
    InMemoryAuditStore — thread-safe in-memory store

Bus:
    EventBus        — publish, start_subscriber, stop, drain_buffered_events, get_status
    get_event_bus() — module-level singleton with optional config override
    publish_event() — convenience wrapper around get_event_bus().publish()
"""
import threading
from typing import Callable, Optional

from .audit import AuditStore, Event, InMemoryAuditStore
from .bus import EventBus
from .config import EventBusConfig

_BUS: Optional[EventBus] = None
_BUS_LOCK = threading.Lock()


def get_event_bus(
    config: Optional[EventBusConfig] = None,
    *,
    audit_store: Optional[AuditStore] = None,
) -> EventBus:
    """Return the process-level EventBus singleton.

    The first call creates the bus with *config* and *audit_store*;
    subsequent calls return the cached instance (ignoring new arguments).
    """
    global _BUS
    if _BUS is None:
        with _BUS_LOCK:
            if _BUS is None:
                _BUS = EventBus(config, audit_store=audit_store)
    return _BUS


def reset_event_bus() -> None:
    """Reset the singleton (use in tests between test cases)."""
    global _BUS
    with _BUS_LOCK:
        _BUS = None


def publish_event(
    event_type: str,
    *,
    correlation_id: Optional[str] = None,
    payload: Optional[dict] = None,
) -> bool:
    """Emit *event_type* through the process-level event bus."""
    return get_event_bus().publish(
        event_type, correlation_id=correlation_id, payload=payload
    )


__all__ = [
    "EventBusConfig",
    "Event",
    "AuditStore",
    "InMemoryAuditStore",
    "EventBus",
    "get_event_bus",
    "reset_event_bus",
    "publish_event",
]
