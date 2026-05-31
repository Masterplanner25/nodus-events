"""EventBus — distributed event emission with Redis pub/sub and local fallback.

Key behaviours:
- Source-instance dedup: skip messages where source matches this instance's ID
- Pre-rehydration buffer: queue events received before mark_rehydrated() is called
- Exponential reconnect backoff: 1s → 30s cap on subscriber thread
- Graceful degradation: Redis unavailable → local-only mode (never raises to callers)
- Optional AuditStore: pass on_event callback to persist events to a DB
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Optional

from .audit import AuditStore, Event, InMemoryAuditStore
from .config import EventBusConfig

logger = logging.getLogger(__name__)

_SINGLETON: Optional["EventBus"] = None
_SINGLETON_LOCK = threading.Lock()

_NOTIFY_FN_TYPE = Callable[[str, Optional[str]], int]


class EventBus:
    """Thin, non-fatal wrapper around Redis pub/sub.

    Args:
        config:     ``EventBusConfig`` for Redis URL, channel, and instance ID.
        audit_store: Optional store for persisting all emitted events.
    """

    def __init__(
        self,
        config: Optional[EventBusConfig] = None,
        *,
        audit_store: Optional[AuditStore] = None,
    ) -> None:
        self._config = config or EventBusConfig()
        self._audit = audit_store
        self._enabled = self._config.enabled
        self._pub_lock = threading.Lock()
        self._pub_client = None
        self._subscriber_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._consecutive_failures = 0
        self._max_failures = 3
        self._pre_rehydration_buffer: list[tuple[str, Optional[str]]] = []
        self._buffer_lock = threading.Lock()
        self._rehydrated = False

    # ── Publisher ──────────────────────────────────────────────────────────────

    def _get_pub_client(self):
        if self._pub_client is None:
            import redis as _redis  # noqa: PLC0415
            self._pub_client = _redis.from_url(
                self._config.redis_url,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1,
            )
        return self._pub_client

    def publish(
        self,
        event_type: str,
        *,
        correlation_id: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> bool:
        """Publish *event_type* to all instances via Redis pub/sub.

        Never raises — returns False on any error.
        """
        event = Event(
            event_type=event_type,
            correlation_id=correlation_id,
            source_instance_id=self._config.instance_id,
            payload=dict(payload or {}),
        )
        if self._audit is not None:
            try:
                self._audit.record(event)
            except Exception:
                pass

        if not self._enabled:
            return False

        message = json.dumps({
            "event_type": event_type,
            "correlation_id": correlation_id,
            "source_instance_id": self._config.instance_id,
            "payload": payload or {},
        })

        with self._pub_lock:
            try:
                client = self._get_pub_client()
                client.publish(self._config.channel, message)
                self._consecutive_failures = 0
                return True
            except Exception as exc:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._max_failures:
                    self._enabled = False
                    logger.warning(
                        "[EventBus] disabled after %d consecutive failures: %s",
                        self._consecutive_failures, exc,
                    )
                else:
                    logger.warning("[EventBus] publish failed: %s", exc)
                self._pub_client = None
                return False

    # ── Subscriber ────────────────────────────────────────────────────────────

    def start_subscriber(self, notify_fn: _NOTIFY_FN_TYPE) -> None:
        """Start the background subscriber daemon thread.

        Args:
            notify_fn: Called with ``(event_type, correlation_id)`` for each
                       received message.  Should return the number of listeners
                       notified.  Called from the daemon thread.
        """
        if not self._enabled:
            return
        if self._subscriber_thread is not None and self._subscriber_thread.is_alive():
            return
        self._stop_event.clear()
        self._subscriber_thread = threading.Thread(
            target=self._subscriber_loop,
            args=(notify_fn,),
            name="nodus-event-bus-subscriber",
            daemon=True,
        )
        self._subscriber_thread.start()
        logger.info("[EventBus] subscriber started (channel=%s)", self._config.channel)

    def stop_subscriber(self) -> None:
        self._stop_event.set()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stop_event.set()
        t = self._subscriber_thread
        if t is not None:
            t.join(timeout=timeout)
        self._subscriber_thread = None
        self._pub_client = None

    def mark_rehydrated(self) -> None:
        """Signal that startup rehydration is complete.

        Call this after all waiting flows/runs have been re-registered.
        Any events buffered before this call will be drained on the next
        ``drain_buffered_events()`` call.
        """
        self._rehydrated = True

    def drain_buffered_events(self, notify_fn: _NOTIFY_FN_TYPE) -> int:
        """Dispatch all pre-rehydration buffered events.

        Returns:
            Number of events drained.
        """
        with self._buffer_lock:
            pending = list(self._pre_rehydration_buffer)
            self._pre_rehydration_buffer.clear()
        if not pending:
            return 0
        dispatched = 0
        for event_type, correlation_id in pending:
            try:
                notify_fn(event_type, correlation_id)
                dispatched += 1
            except Exception as exc:
                logger.warning("[EventBus] drain notify failed: %s", exc)
        return dispatched

    def get_status(self) -> dict:
        return {
            "enabled": self._enabled,
            "subscriber_running": bool(
                self._subscriber_thread and self._subscriber_thread.is_alive()
            ),
            "redis_connected": self._is_redis_connected(),
            "buffered_events": len(self._pre_rehydration_buffer),
        }

    # ── Private ────────────────────────────────────────────────────────────────

    def _is_redis_connected(self) -> bool:
        if not self._enabled:
            return False
        try:
            client = self._get_pub_client()
            client.ping()
            return True
        except Exception:
            return False

    def _subscriber_loop(self, notify_fn: _NOTIFY_FN_TYPE) -> None:
        import redis as _redis  # noqa: PLC0415
        delay = self._config.reconnect_base_delay

        while not self._stop_event.is_set():
            try:
                r = _redis.from_url(self._config.redis_url, decode_responses=True)
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self._config.channel)
                delay = self._config.reconnect_base_delay
                for message in pubsub.listen():
                    if self._stop_event.is_set():
                        break
                    if message is None or message.get("type") != "message":
                        continue
                    self._handle_message(message.get("data", ""), notify_fn)
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                logger.warning("[EventBus] subscriber lost connection (%s) — reconnect in %.1fs", exc, delay)
                time.sleep(delay)
                delay = min(delay * 2, self._config.reconnect_max_delay)

    def _handle_message(self, data: str, notify_fn: _NOTIFY_FN_TYPE) -> None:
        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return

        # Skip own-instance messages
        source = payload.get("source_instance_id")
        if source == self._config.instance_id:
            return

        event_type = payload.get("event_type")
        if not event_type or not isinstance(event_type, str):
            return

        correlation_id: Optional[str] = payload.get("correlation_id") or None

        if not self._rehydrated:
            with self._buffer_lock:
                if len(self._pre_rehydration_buffer) < self._config.max_buffer_size:
                    self._pre_rehydration_buffer.append((event_type, correlation_id))
            return

        try:
            notify_fn(event_type, correlation_id)
        except Exception as exc:
            logger.warning("[EventBus] notify_fn failed: %s", exc)
