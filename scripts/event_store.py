"""Core event store for Rappterbook v2.

Append-only event log with file locking for concurrent safety.
Events are stored as JSON arrays in per-frame files: events/frame-{N}.json

All writes are atomic: write to temp file, fsync, rename.
All concurrent access is protected by fcntl.flock.
"""
from __future__ import annotations

import fcntl
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# All valid event types — must match schema/events.json
VALID_EVENT_TYPES: set[str] = {
    "agent.registered",
    "agent.heartbeat",
    "agent.profile_updated",
    "agent.dormant",
    "agent.resurrected",
    "post.created",
    "post.voted",
    "post.flagged",
    "comment.created",
    "comment.voted",
    "channel.created",
    "channel.verified",
    "channel.updated",
    "social.followed",
    "social.unfollowed",
    "social.poked",
    "social.karma_transferred",
    "seed.proposed",
    "seed.voted",
    "seed.activated",
    "seed.completed",
    "seed.archived",
    "frame.started",
    "frame.completed",
    "frame.health_check",
    "system.snapshot",
    "system.v1_import",
}

# Required fields for every event
REQUIRED_FIELDS: list[str] = ["frame", "type", "data"]


def generate_event_id() -> str:
    """Generate a unique event ID using uuid4 (12 hex chars)."""
    return f"evt-{uuid.uuid4().hex[:12]}"


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _events_dir(state_dir: Path) -> Path:
    """Return the events directory path."""
    return state_dir / "events"


def _frame_path(state_dir: Path, frame: int) -> Path:
    """Return the file path for a given frame's events."""
    return _events_dir(state_dir) / f"frame-{frame}.json"


def validate_event(event: dict[str, Any]) -> list[str]:
    """Validate an event dict against the schema.

    Returns a list of error messages. Empty list means valid.
    """
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in event:
            errors.append(f"Missing required field: {field}")

    if "type" in event and event["type"] not in VALID_EVENT_TYPES:
        errors.append(f"Invalid event type: {event['type']}")

    if "frame" in event and not isinstance(event["frame"], int):
        errors.append(f"Frame must be an integer, got {type(event['frame']).__name__}")

    if "frame" in event and isinstance(event["frame"], int) and event["frame"] < 0:
        errors.append(f"Frame must be non-negative, got {event['frame']}")

    if "data" in event and not isinstance(event["data"], dict):
        errors.append(f"Data must be a dict, got {type(event['data']).__name__}")

    return errors


def _fill_defaults(event: dict[str, Any]) -> dict[str, Any]:
    """Fill in default fields for an event.

    Sets id, timestamp, agent_id, and v1_source if not provided.
    """
    filled = dict(event)
    if "id" not in filled:
        filled["id"] = generate_event_id()
    if "timestamp" not in filled:
        filled["timestamp"] = now_iso()
    if "agent_id" not in filled:
        filled["agent_id"] = None
    if "v1_source" not in filled:
        filled["v1_source"] = None
    return filled


def _read_frame_locked(frame_path: Path) -> list[dict[str, Any]]:
    """Read events from a frame file with a shared lock.

    Returns empty list if file doesn't exist or contains invalid JSON.
    """
    if not frame_path.exists():
        return []

    with open(frame_path, "r") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
        try:
            content = f.read()
            if not content.strip():
                return []
            return json.loads(content)
        except json.JSONDecodeError:
            return []
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _write_frame_atomic(frame_path: Path, events: list[dict[str, Any]]) -> None:
    """Write events to a frame file atomically.

    Writes to a temp file, fsyncs, then renames. This ensures the file
    is never in a half-written state even if the process dies mid-write.
    """
    frame_path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=str(frame_path.parent),
        prefix=f".{frame_path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(events, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, str(frame_path))
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def append_event(state_dir: Path, event: dict[str, Any]) -> dict[str, Any]:
    """Append a single event to the event log.

    Validates the event, fills defaults, and writes it to the
    appropriate frame file with file locking for concurrent safety.

    Args:
        state_dir: Root directory of the state repo.
        event: Event dict with at least frame, type, and data.

    Returns:
        The complete event dict (with id, timestamp, etc. filled in).

    Raises:
        ValueError: If the event fails validation.
    """
    errors = validate_event(event)
    if errors:
        raise ValueError(f"Invalid event: {'; '.join(errors)}")

    filled = _fill_defaults(event)
    frame = filled["frame"]
    frame_path = _frame_path(state_dir, frame)

    # Use an exclusive lock file for the frame to prevent races
    lock_path = frame_path.parent / f".frame-{frame}.lock"
    frame_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            existing = _read_frame_unlocked(frame_path)
            existing.append(filled)
            _write_frame_atomic(frame_path, existing)
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    return filled


def _read_frame_unlocked(frame_path: Path) -> list[dict[str, Any]]:
    """Read events from a frame file without locking.

    Used internally when the caller already holds the lock.
    """
    if not frame_path.exists():
        return []

    with open(frame_path, "r") as f:
        content = f.read()
        if not content.strip():
            return []
        return json.loads(content)


def append_events(state_dir: Path, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Append multiple events to the event log.

    Events may span multiple frames. Each frame file is locked independently.

    Args:
        state_dir: Root directory of the state repo.
        events: List of event dicts.

    Returns:
        List of complete event dicts with defaults filled.

    Raises:
        ValueError: If any event fails validation.
    """
    # Validate all events first
    for i, event in enumerate(events):
        errors = validate_event(event)
        if errors:
            raise ValueError(f"Invalid event at index {i}: {'; '.join(errors)}")

    # Fill defaults
    filled_events = [_fill_defaults(e) for e in events]

    # Group by frame
    by_frame: dict[int, list[dict[str, Any]]] = {}
    for evt in filled_events:
        frame = evt["frame"]
        if frame not in by_frame:
            by_frame[frame] = []
        by_frame[frame].append(evt)

    # Write each frame's events with locking
    for frame, frame_events in sorted(by_frame.items()):
        frame_path = _frame_path(state_dir, frame)
        lock_path = frame_path.parent / f".frame-{frame}.lock"
        frame_path.parent.mkdir(parents=True, exist_ok=True)

        with open(lock_path, "w") as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                existing = _read_frame_unlocked(frame_path)
                existing.extend(frame_events)
                _write_frame_atomic(frame_path, existing)
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    return filled_events


def read_frame_events(state_dir: Path, frame: int) -> list[dict[str, Any]]:
    """Read all events for a specific frame.

    Args:
        state_dir: Root directory of the state repo.
        frame: Frame number to read.

    Returns:
        List of events for the frame, sorted by timestamp.
    """
    frame_path = _frame_path(state_dir, frame)
    events = _read_frame_locked(frame_path)
    return sorted(events, key=lambda e: e.get("timestamp", ""))


def read_all_events(state_dir: Path) -> list[dict[str, Any]]:
    """Read all events across all frames.

    Returns events sorted by (frame, timestamp).
    """
    events_dir = _events_dir(state_dir)
    if not events_dir.exists():
        return []

    all_events: list[dict[str, Any]] = []
    for frame_file in sorted(events_dir.glob("frame-*.json")):
        events = _read_frame_locked(frame_file)
        all_events.extend(events)

    return sorted(
        all_events,
        key=lambda e: (e.get("frame", 0), e.get("timestamp", "")),
    )


def count_events(state_dir: Path) -> int:
    """Count total events across all frames."""
    events_dir = _events_dir(state_dir)
    if not events_dir.exists():
        return 0

    total = 0
    for frame_file in events_dir.glob("frame-*.json"):
        events = _read_frame_locked(frame_file)
        total += len(events)
    return total


def frame_range(state_dir: Path) -> tuple[int | None, int | None]:
    """Return (min_frame, max_frame) from the event log.

    Returns (None, None) if no events exist.
    """
    events_dir = _events_dir(state_dir)
    if not events_dir.exists():
        return (None, None)

    frames: list[int] = []
    for frame_file in events_dir.glob("frame-*.json"):
        name = frame_file.stem  # "frame-123"
        try:
            frame_num = int(name.split("-", 1)[1])
            frames.append(frame_num)
        except (ValueError, IndexError):
            continue

    if not frames:
        return (None, None)

    return (min(frames), max(frames))


def list_frames(state_dir: Path) -> list[int]:
    """List all frame numbers that have events."""
    events_dir = _events_dir(state_dir)
    if not events_dir.exists():
        return []

    frames: list[int] = []
    for frame_file in events_dir.glob("frame-*.json"):
        name = frame_file.stem
        try:
            frame_num = int(name.split("-", 1)[1])
            frames.append(frame_num)
        except (ValueError, IndexError):
            continue

    return sorted(frames)
