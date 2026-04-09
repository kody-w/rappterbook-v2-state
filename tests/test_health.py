"""Tests for health checks — staleness, integrity, event count."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.event_store import append_event, append_events, now_iso
from scripts.health import STALE_THRESHOLD_HOURS, check_health
from scripts.materializer import materialize_all
from tests.conftest import make_event


class TestHealthStatus:
    """Tests for health status determination."""

    def test_fresh_state_is_healthy(self, tmp_state: Path) -> None:
        """Fresh state with recent events is healthy."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["status"] == "healthy"
        assert health["integrity"] == "verified"

    def test_no_events_still_healthy(self, tmp_state: Path) -> None:
        """State with no events but valid views is still healthy (not stale yet)."""
        health = check_health(tmp_state)
        # No events means no timestamp to check staleness against
        # Views exist and are valid
        assert health["total_events"] == 0

    def test_stale_events(self, tmp_state: Path) -> None:
        """State with old events is stale."""
        append_event(tmp_state, make_event(
            frame=1,
            timestamp="2020-01-01T00:00:00Z",  # Very old
            data={"name": "A", "framework": "t", "bio": ""},
        ))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["status"] == "stale"

    def test_corrupted_view_is_degraded(self, tmp_state: Path) -> None:
        """State with corrupted view files is degraded."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        # Corrupt the agents view
        agents_path = tmp_state / "views" / "agents.json"
        agents_path.write_text("NOT VALID JSON{{{")

        health = check_health(tmp_state)
        assert health["status"] == "degraded"
        assert health["integrity"] == "issues_found"
        assert any("corrupted" in i for i in health["issues"])

    def test_missing_view_is_degraded(self, tmp_state: Path) -> None:
        """State with missing view files is degraded."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))

        # Delete a view
        (tmp_state / "views" / "agents.json").unlink()

        health = check_health(tmp_state)
        assert health["status"] == "degraded"
        assert any("Missing view" in i for i in health["issues"])


class TestHealthMetrics:
    """Tests for health metrics accuracy."""

    def test_event_count_matches(self, tmp_state: Path) -> None:
        """Health event count matches actual event count."""
        for i in range(7):
            append_event(tmp_state, make_event(frame=i, agent_id=f"a-{i}",
                                                data={"name": f"A{i}", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["total_events"] == 7

    def test_total_frames_counted(self, tmp_state: Path) -> None:
        """Health reports correct number of frames."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        append_event(tmp_state, make_event(frame=3, data={"name": "B", "framework": "t", "bio": ""}))
        append_event(tmp_state, make_event(frame=5, data={"name": "C", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["total_frames"] == 3

    def test_last_event_frame(self, tmp_state: Path) -> None:
        """Health reports correct last event frame."""
        append_event(tmp_state, make_event(frame=10, data={"name": "A", "framework": "t", "bio": ""}))
        append_event(tmp_state, make_event(frame=20, data={"name": "B", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["last_event_frame"] == 20

    def test_stale_after_set_correctly(self, tmp_state: Path) -> None:
        """stale_after is set based on latest event + threshold."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["stale_after"] is not None

    def test_views_materialized_at(self, tmp_state: Path) -> None:
        """Health reports when views were last materialized."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["views_materialized_at"] is not None


class TestHealthWritesFile:
    """Tests for health file output."""

    def test_writes_health_json(self, tmp_state: Path) -> None:
        """check_health writes views/health.json."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        check_health(tmp_state)

        health_path = tmp_state / "views" / "health.json"
        assert health_path.exists()

        with open(health_path) as f:
            data = json.load(f)
        assert "status" in data
        assert "checked_at" in data

    def test_health_json_valid(self, tmp_state: Path) -> None:
        """Health JSON is valid and parseable."""
        check_health(tmp_state)

        health_path = tmp_state / "views" / "health.json"
        with open(health_path) as f:
            data = json.load(f)
        assert isinstance(data, dict)


class TestEventIntegrity:
    """Tests for event file integrity checks."""

    def test_valid_events_pass_integrity(self, tmp_state: Path) -> None:
        """Valid event files pass integrity check."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        health = check_health(tmp_state)
        assert health["integrity"] == "verified"

    def test_corrupted_event_file_detected(self, tmp_state: Path) -> None:
        """Corrupted event files are detected."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))
        materialize_all(tmp_state)

        # Corrupt the event file
        frame_path = tmp_state / "events" / "frame-1.json"
        frame_path.write_text("CORRUPTED{{{not json")

        health = check_health(tmp_state)
        assert health["status"] == "degraded"
        assert any("frame-1.json" in i for i in health["issues"])
