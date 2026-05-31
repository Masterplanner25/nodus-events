"""EventBusConfig — configuration for the distributed event bus."""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _default_instance_id() -> str:
    return (
        os.environ.get("INSTANCE_ID")
        or os.environ.get("HOSTNAME")
        or socket.gethostname()
        or "unknown"
    )


@dataclass
class EventBusConfig:
    """Configuration for a distributed event bus.

    Attributes
    ----------
    redis_url:      Redis connection URL.  Standard ``REDIS_URL`` env var.
    channel:        Redis pub/sub channel name.
    enabled:        When False the bus operates in local-only mode (no Redis).
    instance_id:    Stable identifier for this OS process / pod.  Used for
                    source-instance deduplication.
    max_buffer_size: Maximum events to buffer before rehydration completes.
    reconnect_base_delay: Initial reconnect backoff in seconds.
    reconnect_max_delay:  Maximum reconnect backoff cap in seconds.
    """

    redis_url: str = field(
        default_factory=lambda: (
            os.getenv("REDIS_URL") or "redis://localhost:6379/0"
        )
    )
    channel: str = field(
        default_factory=lambda: os.getenv("NODUS_EVENT_BUS_CHANNEL", "nodus:events")
    )
    enabled: bool = field(
        default_factory=lambda: os.getenv(
            "NODUS_EVENT_BUS_ENABLED", "true"
        ).lower() not in {"0", "false", "no", "off"}
    )
    instance_id: str = field(default_factory=_default_instance_id)
    max_buffer_size: int = 1000
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 30.0
