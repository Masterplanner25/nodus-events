"""nodus-events tests — no real Redis required (uses fakeredis for pub/sub tests)."""
import time
import pytest

from nodus_events import (
    Event, EventBus, EventBusConfig,
    InMemoryAuditStore,
    get_event_bus, publish_event, reset_event_bus,
)

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis not installed")


@pytest.fixture(autouse=True)
def _reset():
    reset_event_bus()
    yield
    reset_event_bus()


# ── EventBusConfig ─────────────────────────────────────────────────────────────

def test_config_disabled():
    cfg = EventBusConfig(enabled=False)
    assert cfg.enabled is False


def test_config_default_channel():
    import os
    os.environ.pop("NODUS_EVENT_BUS_CHANNEL", None)
    cfg = EventBusConfig()
    assert cfg.channel == "nodus:events"


# ── Event ─────────────────────────────────────────────────────────────────────

def test_event_creation():
    e = Event(event_type="test.event", correlation_id="c1", source_instance_id="host-1")
    assert e.event_type == "test.event"
    assert e.correlation_id == "c1"
    assert e.timestamp is not None


# ── InMemoryAuditStore ────────────────────────────────────────────────────────

def test_audit_record_and_list():
    store = InMemoryAuditStore()
    e = Event("op.done", correlation_id="c1")
    store.record(e)
    results = store.list()
    assert len(results) == 1
    assert results[0].event_type == "op.done"


def test_audit_filter_by_type():
    store = InMemoryAuditStore()
    store.record(Event("a.done"))
    store.record(Event("b.done"))
    assert len(store.list(event_type="a.done")) == 1
    assert len(store.list(event_type="b.done")) == 1
    assert len(store.list()) == 2


def test_audit_limit():
    store = InMemoryAuditStore()
    for i in range(10):
        store.record(Event(f"event.{i}"))
    assert len(store.list(limit=3)) == 3


# ── EventBus disabled mode ────────────────────────────────────────────────────

def test_publish_disabled_returns_false():
    bus = EventBus(EventBusConfig(enabled=False))
    assert bus.publish("test.event") is False


def test_get_status_disabled():
    bus = EventBus(EventBusConfig(enabled=False))
    status = bus.get_status()
    assert status["enabled"] is False
    assert status["subscriber_running"] is False


# ── EventBus with audit store ─────────────────────────────────────────────────

def test_publish_records_to_audit_store_even_when_disabled():
    audit = InMemoryAuditStore()
    bus = EventBus(EventBusConfig(enabled=False), audit_store=audit)
    bus.publish("audit.test", correlation_id="c1", payload={"x": 1})
    events = audit.list()
    assert len(events) == 1
    assert events[0].event_type == "audit.test"
    assert events[0].correlation_id == "c1"


# ── Pre-rehydration buffer ────────────────────────────────────────────────────

def test_drain_empty_buffer():
    bus = EventBus(EventBusConfig(enabled=False))
    calls = []
    count = bus.drain_buffered_events(lambda t, c: calls.append(t) or 1)
    assert count == 0
    assert len(calls) == 0


# ── Singleton ─────────────────────────────────────────────────────────────────

def test_get_event_bus_singleton():
    b1 = get_event_bus(EventBusConfig(enabled=False))
    b2 = get_event_bus()
    assert b1 is b2


def test_reset_event_bus():
    b1 = get_event_bus(EventBusConfig(enabled=False))
    reset_event_bus()
    b2 = get_event_bus(EventBusConfig(enabled=False))
    assert b1 is not b2


def test_publish_event_convenience():
    get_event_bus(EventBusConfig(enabled=False))
    result = publish_event("convenience.test")
    assert result is False   # disabled → False


# ── Redis pub/sub with fakeredis ───────────────────────────────────────────────

def test_publish_with_fakeredis():
    """Publish succeeds when Redis is available via fakeredis."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    cfg = EventBusConfig(enabled=True, instance_id="test-instance")

    bus = EventBus(cfg)
    # Inject fakeredis client
    bus._pub_client = fake
    bus._enabled = True

    result = bus.publish("test.published", correlation_id="c1")
    assert result is True


def test_source_instance_dedup():
    """Messages from the same instance should be skipped."""
    cfg = EventBusConfig(enabled=False, instance_id="same-instance")
    bus = EventBus(cfg)
    bus.mark_rehydrated()

    calls = []
    import json as _json
    bus._handle_message(
        _json.dumps({
            "event_type": "test.event",
            "source_instance_id": "same-instance",
            "correlation_id": None,
            "payload": {},
        }),
        lambda t, c: calls.append(t) or 1,
    )
    assert len(calls) == 0   # own-instance message skipped


def test_cross_instance_message_delivered():
    """Messages from other instances should be delivered."""
    cfg = EventBusConfig(enabled=False, instance_id="instance-A")
    bus = EventBus(cfg)
    bus.mark_rehydrated()

    calls = []
    import json as _json
    bus._handle_message(
        _json.dumps({
            "event_type": "op.completed",
            "source_instance_id": "instance-B",
            "correlation_id": "c1",
            "payload": {},
        }),
        lambda t, c: calls.append((t, c)) or 1,
    )
    assert len(calls) == 1
    assert calls[0] == ("op.completed", "c1")


def test_pre_rehydration_buffer():
    """Events received before mark_rehydrated() are buffered."""
    cfg = EventBusConfig(enabled=False, instance_id="A")
    bus = EventBus(cfg)
    # NOT yet rehydrated

    import json as _json
    bus._handle_message(
        _json.dumps({
            "event_type": "early.event",
            "source_instance_id": "B",
            "correlation_id": "c1",
            "payload": {},
        }),
        lambda t, c: None,  # should not be called yet
    )
    assert len(bus._pre_rehydration_buffer) == 1

    # Now rehydrate and drain
    bus.mark_rehydrated()
    calls = []
    count = bus.drain_buffered_events(lambda t, c: calls.append(t) or 1)
    assert count == 1
    assert calls == ["early.event"]
    assert len(bus._pre_rehydration_buffer) == 0
