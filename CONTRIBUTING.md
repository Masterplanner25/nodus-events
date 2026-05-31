# Contributing to nodus-events

## Setup

```bash
git clone https://github.com/Masterplanner25/nodus-events.git
cd nodus-events
pip install -e ".[dev]"
```

The `dev` extra includes `fakeredis` so Redis paths can be tested without
a live Redis server.

## Running tests

```bash
pytest tests/ -q
```

## Code style

- Python 3.11+
- No required external dependencies (Redis is optional)
- `AuditStore` is a protocol — custom backends satisfy it by structure
- Call `reset_event_bus()` between tests to clear the singleton

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Add tests for any new behaviour
3. Ensure `pytest tests/ -q` passes
4. Open a pull request with a description of what changes and why
