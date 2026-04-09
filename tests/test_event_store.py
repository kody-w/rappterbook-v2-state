"""Tests for the event store — the core append-only event log."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest

from scripts.event_store import (
    VALID_EVENT_TYPES,
    append_event,
    append_events,
    count_events,
    frame_range,
    generate_event_id,
    list_frames,
    read_all_events,
    read_frame_events,
    validate_event,
)
from tests.conftest import make_event


class TestAppendSingleEvent:
    """Tests for appending individual events."""

    def test_append_single_event(self, tmp_state: Path) -> None:
        """Append one event and read it back."""
        evt = make_event(frame=1)
        result = append_event(tmp_state, evt)

        assert result["frame"] == 1
        assert result["type"] == "agent.registered"
        assert result["id"].startswith("evt-")

        events = read_frame_events(tmp_state, 1)
        assert len(events) == 1
        assert events[0]["type"] == "agent.registered"

    def test_append_fills_defaults(self, tmp_state: Path) -> None:
        """Missing id, timestamp, agent_id, v1_source get defaults."""
        evt = {"frame": 1, "type": "agent.heartbeat", "data": {}}
        result = append_event(tmp_state, evt)

        assert result["id"].startswith("evt-")
        assert result["timestamp"] is not None
        assert result["agent_id"] is None
        assert result["v1_source"] is None

    def test_append_preserves_explicit_fields(self, tmp_state: Path) -> None:
        """Explicit id, timestamp, etc. are not overwritten."""
        evt = {
            "id": "evt-custom123456",
            "frame": 5,
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "agent.registered",
            "agent_id": "custom-agent",
            "data": {"name": "Custom", "framework": "test", "bio": "hi"},
            "v1_source": "state/agents.json",
        }
        result = append_event(tmp_state, evt)

        assert result["id"] == "evt-custom123456"
        assert result["timestamp"] == "2026-01-01T00:00:00Z"
        assert result["agent_id"] == "custom-agent"
        assert result["v1_source"] == "state/agents.json"

    def test_append_multiple_to_same_frame(self, tmp_state: Path) -> None:
        """Multiple events can be appended to the same frame file."""
        for i in range(5):
            append_event(tmp_state, make_event(frame=1, agent_id=f"agent-{i}"))

        events = read_frame_events(tmp_state, 1)
        assert len(events) == 5

    def test_append_to_different_frames(self, tmp_state: Path) -> None:
        """Events land in the correct frame files."""
        append_event(tmp_state, make_event(frame=1))
        append_event(tmp_state, make_event(frame=2))
        append_event(tmp_state, make_event(frame=3))

        assert len(read_frame_events(tmp_state, 1)) == 1
        assert len(read_frame_events(tmp_state, 2)) == 1
        assert len(read_frame_events(tmp_state, 3)) == 1


class TestAppendMultipleEvents:
    """Tests for batch event appending."""

    def test_batch_append(self, tmp_state: Path) -> None:
        """Append a batch of events across frames."""
        events = [
            make_event(frame=1, agent_id="a1"),
            make_event(frame=1, agent_id="a2"),
            make_event(frame=2, agent_id="a3"),
        ]
        results = append_events(tmp_state, events)

        assert len(results) == 3
        assert len(read_frame_events(tmp_state, 1)) == 2
        assert len(read_frame_events(tmp_state, 2)) == 1

    def test_batch_validation_rejects_all_on_error(self, tmp_state: Path) -> None:
        """If any event in a batch is invalid, no events are written."""
        events = [
            make_event(frame=1),
            {"frame": 1, "type": "invalid.type", "data": {}},
        ]
        with pytest.raises(ValueError, match="Invalid event"):
            append_events(tmp_state, events)

        # No events should have been written
        assert count_events(tmp_state) == 0

    def test_large_batch_append(self, tmp_state: Path) -> None:
        """Append 1000 events in a single batch."""
        events = [
            make_event(frame=1, agent_id=f"agent-{i}", event_type="agent.heartbeat", data={})
            for i in range(1000)
        ]
        results = append_events(tmp_state, events)

        assert len(results) == 1000
        assert count_events(tmp_state) == 1000


class TestReadEvents:
    """Tests for reading events."""

    def test_read_empty_frame(self, tmp_state: Path) -> None:
        """Reading a nonexistent frame returns empty list."""
        events = read_frame_events(tmp_state, 999)
        assert events == []

    def test_read_all_events_empty(self, tmp_state: Path) -> None:
        """Reading all events from empty store returns empty list."""
        assert read_all_events(tmp_state) == []

    def test_read_all_events_sorted(self, tmp_state: Path) -> None:
        """All events are sorted by (frame, timestamp)."""
        append_event(tmp_state, make_event(frame=3, timestamp="2026-01-03T00:00:00Z"))
        append_event(tmp_state, make_event(frame=1, timestamp="2026-01-01T00:00:00Z"))
        append_event(tmp_state, make_event(frame=2, timestamp="2026-01-02T00:00:00Z"))

        events = read_all_events(tmp_state)
        frames = [e["frame"] for e in events]
        assert frames == [1, 2, 3]

    def test_events_sorted_by_timestamp_within_frame(self, tmp_state: Path) -> None:
        """Events within a frame are sorted by timestamp."""
        append_event(tmp_state, make_event(frame=1, timestamp="2026-01-01T12:00:00Z", agent_id="late"))
        append_event(tmp_state, make_event(frame=1, timestamp="2026-01-01T06:00:00Z", agent_id="early"))

        events = read_frame_events(tmp_state, 1)
        assert events[0]["agent_id"] == "early"
        assert events[1]["agent_id"] == "late"

    def test_event_data_preserved_exactly(self, tmp_state: Path) -> None:
        """Complex data payloads are preserved through write/read."""
        complex_data = {
            "name": "Test Agent",
            "framework": "custom",
            "bio": "A test bio with special chars: <>&\"'",
            "nested": {"key": [1, 2, 3]},
            "unicode": "Hello",
        }
        evt = make_event(frame=1, data=complex_data)
        append_event(tmp_state, evt)

        events = read_frame_events(tmp_state, 1)
        assert events[0]["data"] == complex_data


class TestValidation:
    """Tests for event validation."""

    def test_missing_required_field_frame(self, tmp_state: Path) -> None:
        """Event without frame is rejected."""
        with pytest.raises(ValueError, match="Missing required field: frame"):
            append_event(tmp_state, {"type": "agent.heartbeat", "data": {}})

    def test_missing_required_field_type(self, tmp_state: Path) -> None:
        """Event without type is rejected."""
        with pytest.raises(ValueError, match="Missing required field: type"):
            append_event(tmp_state, {"frame": 1, "data": {}})

    def test_missing_required_field_data(self, tmp_state: Path) -> None:
        """Event without data is rejected."""
        with pytest.raises(ValueError, match="Missing required field: data"):
            append_event(tmp_state, {"frame": 1, "type": "agent.heartbeat"})

    def test_invalid_event_type_rejected(self, tmp_state: Path) -> None:
        """Unknown event types are rejected."""
        with pytest.raises(ValueError, match="Invalid event type"):
            append_event(tmp_state, {"frame": 1, "type": "bogus.type", "data": {}})

    def test_negative_frame_rejected(self, tmp_state: Path) -> None:
        """Negative frame numbers are rejected."""
        with pytest.raises(ValueError, match="non-negative"):
            append_event(tmp_state, {"frame": -1, "type": "agent.heartbeat", "data": {}})

    def test_non_int_frame_rejected(self, tmp_state: Path) -> None:
        """Non-integer frame is rejected."""
        with pytest.raises(ValueError, match="integer"):
            append_event(tmp_state, {"frame": "one", "type": "agent.heartbeat", "data": {}})

    def test_non_dict_data_rejected(self, tmp_state: Path) -> None:
        """Non-dict data is rejected."""
        with pytest.raises(ValueError, match="dict"):
            append_event(tmp_state, {"frame": 1, "type": "agent.heartbeat", "data": "string"})

    def test_validate_returns_errors_list(self) -> None:
        """validate_event returns list of error strings."""
        errors = validate_event({"frame": -1, "type": "bad.type", "data": "nope"})
        assert len(errors) >= 2


class TestEventIdUniqueness:
    """Tests for event ID generation."""

    def test_generated_ids_are_unique(self) -> None:
        """generate_event_id produces unique IDs."""
        ids = {generate_event_id() for _ in range(10000)}
        assert len(ids) == 10000

    def test_id_format(self) -> None:
        """IDs follow the evt-{12hex} format."""
        eid = generate_event_id()
        assert eid.startswith("evt-")
        assert len(eid) == 16  # "evt-" + 12 hex chars


class TestFrameOperations:
    """Tests for frame-level operations."""

    def test_count_events_empty(self, tmp_state: Path) -> None:
        """Empty store has zero events."""
        assert count_events(tmp_state) == 0

    def test_count_events_after_appends(self, tmp_state: Path) -> None:
        """Count reflects actual number of events."""
        for i in range(7):
            append_event(tmp_state, make_event(frame=i % 3, agent_id=f"a-{i}"))
        assert count_events(tmp_state) == 7

    def test_frame_range_empty(self, tmp_state: Path) -> None:
        """Empty store has (None, None) range."""
        assert frame_range(tmp_state) == (None, None)

    def test_frame_range_with_events(self, tmp_state: Path) -> None:
        """Frame range reflects actual min/max frames."""
        append_event(tmp_state, make_event(frame=5))
        append_event(tmp_state, make_event(frame=10))
        append_event(tmp_state, make_event(frame=7))

        assert frame_range(tmp_state) == (5, 10)

    def test_list_frames(self, tmp_state: Path) -> None:
        """list_frames returns sorted unique frame numbers."""
        append_event(tmp_state, make_event(frame=3))
        append_event(tmp_state, make_event(frame=1))
        append_event(tmp_state, make_event(frame=5))
        append_event(tmp_state, make_event(frame=1))  # duplicate frame

        frames = list_frames(tmp_state)
        assert frames == [1, 3, 5]

    def test_frame_file_is_valid_json(self, tmp_state: Path) -> None:
        """Frame file is valid JSON after every append."""
        for i in range(10):
            append_event(tmp_state, make_event(frame=1, agent_id=f"a-{i}"))

        frame_path = tmp_state / "events" / "frame-1.json"
        with open(frame_path) as f:
            data = json.load(f)
        assert isinstance(data, list)
        assert len(data) == 10


class TestAtomicWrite:
    """Tests for write atomicity and safety."""

    def test_frame_file_valid_after_append(self, tmp_state: Path) -> None:
        """Frame file is always valid JSON, even after many appends."""
        for i in range(50):
            append_event(tmp_state, make_event(frame=1, agent_id=f"agent-{i}"))

        frame_path = tmp_state / "events" / "frame-1.json"
        with open(frame_path) as f:
            data = json.load(f)
        assert len(data) == 50

    def test_all_event_types_are_valid(self) -> None:
        """Every event type in VALID_EVENT_TYPES passes validation."""
        for etype in VALID_EVENT_TYPES:
            errors = validate_event({"frame": 1, "type": etype, "data": {}})
            assert not errors, f"Type {etype} failed validation: {errors}"
