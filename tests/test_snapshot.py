"""Tests for snapshots — creating and restoring full state snapshots."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.event_store import append_event, append_events, count_events, read_all_events
from scripts.materializer import materialize_all, read_view
from scripts.snapshot import (
    create_snapshot,
    diff_snapshots,
    list_snapshots,
    load_snapshot,
    restore_snapshot,
)
from tests.conftest import make_event


class TestCreateSnapshot:
    """Tests for creating snapshots."""

    def test_create_snapshot(self, tmp_state: Path) -> None:
        """Create a snapshot and verify it exists."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "test", "bio": ""},
        ))

        result = create_snapshot(tmp_state, "test-snap")
        assert result["id"] == "test-snap"
        assert result["event_count"] >= 1
        assert (tmp_state / "snapshots" / "test-snap.json").exists()

    def test_snapshot_includes_events(self, tmp_state: Path) -> None:
        """Snapshot contains all events."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
        ])

        create_snapshot(tmp_state, "snap-events")
        snap = load_snapshot(tmp_state, "snap-events")

        assert len(snap["events"]) == 2

    def test_snapshot_includes_views(self, tmp_state: Path) -> None:
        """Snapshot contains all materialized views."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "test", "bio": ""},
        ))

        create_snapshot(tmp_state, "snap-views")
        snap = load_snapshot(tmp_state, "snap-views")

        assert "agents" in snap["views"]
        assert "channels" in snap["views"]
        assert "stats" in snap["views"]
        assert "social_graph" in snap["views"]
        assert "trending" in snap["views"]

    def test_snapshot_is_valid_json(self, tmp_state: Path) -> None:
        """Snapshot file is valid JSON."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "test", "bio": ""},
        ))

        create_snapshot(tmp_state, "snap-json")
        snap_path = tmp_state / "snapshots" / "snap-json.json"

        with open(snap_path) as f:
            data = json.load(f)
        assert data["version"] == 1

    def test_snapshot_versioning(self, tmp_state: Path) -> None:
        """Snapshots include a version field."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))

        create_snapshot(tmp_state, "snap-ver")
        snap = load_snapshot(tmp_state, "snap-ver")
        assert snap["version"] == 1

    def test_auto_generated_snapshot_id(self, tmp_state: Path) -> None:
        """Snapshot ID is auto-generated when not provided."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))

        result = create_snapshot(tmp_state)
        assert result["id"].startswith("snap-")


class TestRestoreSnapshot:
    """Tests for restoring snapshots."""

    def test_restore_replays_events(self, tmp_state: Path, tmp_path: Path) -> None:
        """Restoring a snapshot into a fresh state dir replays events."""
        # Create events and snapshot in tmp_state
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
        ])
        create_snapshot(tmp_state, "snap-restore")

        # Restore into a fresh directory (but snapshots are in tmp_state)
        result = restore_snapshot(tmp_state, "snap-restore")
        assert result["events_restored"] == 2

    def test_load_nonexistent_snapshot_raises(self, tmp_state: Path) -> None:
        """Loading a nonexistent snapshot raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_snapshot(tmp_state, "nonexistent")


class TestListSnapshots:
    """Tests for listing snapshots."""

    def test_list_empty(self, tmp_state: Path) -> None:
        """Empty snapshots directory returns empty list."""
        assert list_snapshots(tmp_state) == []

    def test_list_after_creation(self, tmp_state: Path) -> None:
        """Created snapshots appear in the list."""
        append_event(tmp_state, make_event(frame=1, data={"name": "A", "framework": "t", "bio": ""}))

        create_snapshot(tmp_state, "snap-list-1")
        create_snapshot(tmp_state, "snap-list-2")

        snaps = list_snapshots(tmp_state)
        ids = {s["id"] for s in snaps}
        assert "snap-list-1" in ids
        assert "snap-list-2" in ids


class TestDiffSnapshots:
    """Tests for diffing snapshots."""

    def test_diff_shows_new_events(self, tmp_state: Path) -> None:
        """Diff between two snapshots shows new events."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "test", "bio": ""},
        ))
        create_snapshot(tmp_state, "snap-diff-a")

        append_event(tmp_state, make_event(
            frame=2, event_type="post.created", agent_id="a1",
            data={"title": "New", "channel": "gen", "discussion_number": 1},
        ))
        create_snapshot(tmp_state, "snap-diff-b")

        diff = diff_snapshots(tmp_state, "snap-diff-a", "snap-diff-b")
        assert diff["new_event_count"] >= 1
        assert "post.created" in diff["new_event_types"]

    def test_diff_shows_stat_changes(self, tmp_state: Path) -> None:
        """Diff includes stat deltas between snapshots."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "test", "bio": ""},
        ))
        create_snapshot(tmp_state, "snap-stat-a")

        append_event(tmp_state, make_event(
            frame=2, event_type="agent.registered", agent_id="a2",
            data={"name": "A2", "framework": "test", "bio": ""},
        ))
        create_snapshot(tmp_state, "snap-stat-b")

        diff = diff_snapshots(tmp_state, "snap-stat-a", "snap-stat-b")
        assert "total_agents" in diff["stat_diffs"]
        assert diff["stat_diffs"]["total_agents"]["delta"] > 0
