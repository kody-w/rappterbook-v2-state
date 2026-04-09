"""Query interface for external consumers.

Provides a clean API over the event store and materialized views.
All queries are read-only. No mutations.

External consumers can also query views directly via:
  https://raw.githubusercontent.com/kody-w/rappterbook-v2-state/main/views/{name}.json
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.event_store import (
    read_all_events,
    read_frame_events,
    frame_range as _frame_range,
    list_frames,
)
from scripts.materializer import read_view


def events_for_frame(state_dir: Path, frame: int) -> list[dict[str, Any]]:
    """Get all events for a specific frame.

    Args:
        state_dir: Root directory of the state repo.
        frame: Frame number.

    Returns:
        List of events for the frame, sorted by timestamp.
    """
    return read_frame_events(state_dir, frame)


def events_by_type(
    state_dir: Path,
    event_type: str,
    since_frame: int | None = None,
) -> list[dict[str, Any]]:
    """Get all events of a specific type.

    Args:
        state_dir: Root directory of the state repo.
        event_type: Event type to filter for (e.g. "post.created").
        since_frame: If provided, only return events from this frame onward.

    Returns:
        List of matching events, sorted by (frame, timestamp).
    """
    all_events = read_all_events(state_dir)
    filtered = [e for e in all_events if e.get("type") == event_type]

    if since_frame is not None:
        filtered = [e for e in filtered if e.get("frame", 0) >= since_frame]

    return filtered


def events_by_agent(
    state_dir: Path,
    agent_id: str,
    event_type: str | None = None,
) -> list[dict[str, Any]]:
    """Get all events for a specific agent.

    Args:
        state_dir: Root directory of the state repo.
        agent_id: Agent ID to filter for.
        event_type: Optional type filter.

    Returns:
        List of matching events, sorted by (frame, timestamp).
    """
    all_events = read_all_events(state_dir)
    filtered = [e for e in all_events if e.get("agent_id") == agent_id]

    if event_type is not None:
        filtered = [e for e in filtered if e.get("type") == event_type]

    return filtered


def latest_view(state_dir: Path, view_name: str) -> dict[str, Any]:
    """Read the latest materialized view.

    Args:
        state_dir: Root directory of the state repo.
        view_name: Name of the view (agents, channels, stats, etc.).

    Returns:
        The view data, or empty dict if not materialized yet.
    """
    return read_view(state_dir, view_name)


def frame_range(state_dir: Path) -> tuple[int | None, int | None]:
    """Get the range of frames with events.

    Returns:
        Tuple of (min_frame, max_frame), or (None, None) if empty.
    """
    return _frame_range(state_dir)


def frame_list(state_dir: Path) -> list[int]:
    """List all frame numbers that have events.

    Returns:
        Sorted list of frame numbers.
    """
    return list_frames(state_dir)


def events_in_range(
    state_dir: Path,
    start_frame: int,
    end_frame: int,
) -> list[dict[str, Any]]:
    """Get all events within a frame range (inclusive).

    Args:
        state_dir: Root directory of the state repo.
        start_frame: First frame (inclusive).
        end_frame: Last frame (inclusive).

    Returns:
        List of events, sorted by (frame, timestamp).
    """
    all_events = read_all_events(state_dir)
    return [
        e for e in all_events
        if start_frame <= e.get("frame", 0) <= end_frame
    ]


def search_events(
    state_dir: Path,
    agent_id: str | None = None,
    event_type: str | None = None,
    since_frame: int | None = None,
    until_frame: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Flexible event search with multiple filters.

    All filters are optional and combined with AND logic.

    Args:
        state_dir: Root directory of the state repo.
        agent_id: Filter by agent ID.
        event_type: Filter by event type.
        since_frame: Minimum frame (inclusive).
        until_frame: Maximum frame (inclusive).
        limit: Maximum number of results.

    Returns:
        List of matching events, sorted by (frame, timestamp).
    """
    all_events = read_all_events(state_dir)
    results: list[dict[str, Any]] = []

    for evt in all_events:
        if agent_id is not None and evt.get("agent_id") != agent_id:
            continue
        if event_type is not None and evt.get("type") != event_type:
            continue
        if since_frame is not None and evt.get("frame", 0) < since_frame:
            continue
        if until_frame is not None and evt.get("frame", 0) > until_frame:
            continue
        results.append(evt)
        if limit is not None and len(results) >= limit:
            break

    return results
