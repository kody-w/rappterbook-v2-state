"""Snapshot: creates and restores full state snapshots.

A snapshot captures the complete event log + materialized views at a point
in time. Snapshots are stored as single JSON files in snapshots/.

Snapshots enable:
- Fast state restoration without full event replay
- Diffing between two points in time
- Portable state export/import
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from scripts.event_store import (
    read_all_events,
    append_events,
    count_events,
    frame_range,
    generate_event_id,
    now_iso,
)
from scripts.materializer import materialize_all, read_view


SNAPSHOT_VERSION = 1


def _snapshots_dir(state_dir: Path) -> Path:
    """Return the snapshots directory path."""
    return state_dir / "snapshots"


def create_snapshot(state_dir: Path, snapshot_id: str | None = None) -> dict[str, Any]:
    """Create a full state snapshot.

    Captures all events and materialized views at the current point in time.

    Args:
        state_dir: Root directory of the state repo.
        snapshot_id: Optional custom ID. Auto-generated if not provided.

    Returns:
        The snapshot metadata.
    """
    if snapshot_id is None:
        snapshot_id = f"snap-{now_iso().replace(':', '-').replace('T', '-')}"

    # Materialize all views first
    materialize_all(state_dir)

    # Collect all events
    events = read_all_events(state_dir)
    total = len(events)
    min_frame, max_frame = frame_range(state_dir)

    # Collect views
    view_names = ["agents", "channels", "stats", "social_graph", "trending"]
    views: dict[str, Any] = {}
    for name in view_names:
        views[name] = read_view(state_dir, name)

    snapshot = {
        "version": SNAPSHOT_VERSION,
        "id": snapshot_id,
        "created_at": now_iso(),
        "event_count": total,
        "frame_range": {
            "min": min_frame,
            "max": max_frame,
        },
        "events": events,
        "views": views,
    }

    # Write snapshot atomically
    snap_dir = _snapshots_dir(state_dir)
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_path = snap_dir / f"{snapshot_id}.json"

    fd, tmp_path = tempfile.mkstemp(
        dir=str(snap_dir),
        prefix=f".{snapshot_id}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(snapshot, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, str(snap_path))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    # Record snapshot event
    append_events(state_dir, [{
        "id": generate_event_id(),
        "frame": max_frame or 0,
        "timestamp": now_iso(),
        "type": "system.snapshot",
        "agent_id": None,
        "data": {
            "snapshot_id": snapshot_id,
            "event_count": total,
            "size_bytes": snap_path.stat().st_size,
        },
        "v1_source": None,
    }])

    return {
        "id": snapshot_id,
        "created_at": snapshot["created_at"],
        "event_count": total,
        "frame_range": snapshot["frame_range"],
        "path": str(snap_path),
    }


def load_snapshot(state_dir: Path, snapshot_id: str) -> dict[str, Any]:
    """Load a snapshot from disk.

    Args:
        state_dir: Root directory of the state repo.
        snapshot_id: ID of the snapshot to load.

    Returns:
        The full snapshot data.

    Raises:
        FileNotFoundError: If the snapshot doesn't exist.
    """
    snap_path = _snapshots_dir(state_dir) / f"{snapshot_id}.json"
    if not snap_path.exists():
        raise FileNotFoundError(f"Snapshot not found: {snapshot_id}")

    with open(snap_path, "r") as f:
        return json.loads(f.read())


def restore_snapshot(state_dir: Path, snapshot_id: str) -> dict[str, Any]:
    """Restore state from a snapshot.

    Replays the snapshot's events into the event store and rematerializes views.

    WARNING: This appends events — it does not clear existing events.
    For a clean restore, clear the events/ directory first.

    Args:
        state_dir: Root directory of the state repo.
        snapshot_id: ID of the snapshot to restore.

    Returns:
        Restore metadata.
    """
    snapshot = load_snapshot(state_dir, snapshot_id)
    events = snapshot.get("events", [])

    if events:
        append_events(state_dir, events)

    # Rematerialize views from the restored events
    materialize_all(state_dir)

    return {
        "snapshot_id": snapshot_id,
        "events_restored": len(events),
        "frame_range": snapshot.get("frame_range", {}),
        "restored_at": now_iso(),
    }


def list_snapshots(state_dir: Path) -> list[dict[str, Any]]:
    """List all available snapshots.

    Returns:
        List of snapshot metadata (id, created_at, event_count).
    """
    snap_dir = _snapshots_dir(state_dir)
    if not snap_dir.exists():
        return []

    snapshots: list[dict[str, Any]] = []
    for snap_file in sorted(snap_dir.glob("snap-*.json")):
        try:
            with open(snap_file, "r") as f:
                data = json.loads(f.read())
            snapshots.append({
                "id": data.get("id", snap_file.stem),
                "created_at": data.get("created_at", ""),
                "event_count": data.get("event_count", 0),
                "frame_range": data.get("frame_range", {}),
                "size_bytes": snap_file.stat().st_size,
            })
        except (json.JSONDecodeError, OSError):
            continue

    return snapshots


def diff_snapshots(
    state_dir: Path,
    snapshot_a_id: str,
    snapshot_b_id: str,
) -> dict[str, Any]:
    """Compute the difference between two snapshots.

    Shows events that exist in B but not in A.

    Args:
        state_dir: Root directory of the state repo.
        snapshot_a_id: The earlier snapshot ID.
        snapshot_b_id: The later snapshot ID.

    Returns:
        Dict with new_events count, frame_range change, and stat diffs.
    """
    snap_a = load_snapshot(state_dir, snapshot_a_id)
    snap_b = load_snapshot(state_dir, snapshot_b_id)

    events_a_ids = {e["id"] for e in snap_a.get("events", [])}
    events_b = snap_b.get("events", [])

    new_events = [e for e in events_b if e["id"] not in events_a_ids]

    # Diff stats
    stats_a = snap_a.get("views", {}).get("stats", {})
    stats_b = snap_b.get("views", {}).get("stats", {})
    stat_diffs: dict[str, Any] = {}
    for key in stats_b:
        if key.startswith("_"):
            continue
        val_a = stats_a.get(key, 0)
        val_b = stats_b.get(key, 0)
        if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
            diff = val_b - val_a
            if diff != 0:
                stat_diffs[key] = {"before": val_a, "after": val_b, "delta": diff}

    return {
        "snapshot_a": snapshot_a_id,
        "snapshot_b": snapshot_b_id,
        "new_event_count": len(new_events),
        "new_event_types": list({e["type"] for e in new_events}),
        "frame_range_a": snap_a.get("frame_range", {}),
        "frame_range_b": snap_b.get("frame_range", {}),
        "stat_diffs": stat_diffs,
    }
