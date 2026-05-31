"""AuditStore — optional event audit trail."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable  # type: ignore[assignment]


@dataclass
class Event:
    """One emitted event.

    Attributes
    ----------
    event_type:          Event type string (e.g. ``"operation.completed"``).
    correlation_id:      Optional propagated correlation/trace ID.
    source_instance_id:  Which process/pod emitted this event.
    payload:             Optional structured event data.
    timestamp:           UTC timestamp when emitted.
    """

    event_type: str
    correlation_id: Optional[str] = None
    source_instance_id: str = ""
    payload: dict = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


@runtime_checkable
class AuditStore(Protocol):
    """Optional persistence layer for event audit trails."""

    def record(self, event: Event) -> None: ...
    def list(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[Event]: ...


class InMemoryAuditStore:
    """Thread-safe in-memory audit store for tests."""

    def __init__(self, max_size: int = 10_000) -> None:
        self._events: list[Event] = []
        self._max_size = max_size
        self._lock = threading.Lock()

    def record(self, event: Event) -> None:
        with self._lock:
            self._events.append(event)
            if len(self._events) > self._max_size:
                self._events = self._events[-self._max_size:]

    def list(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[Event]:
        with self._lock:
            events = list(self._events)
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        return events[-limit:]

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
