"""Health check: staleness, integrity, event count.

Produces views/health.json with the current health status.
Designed to be queried by external consumers via raw.githubusercontent.com.

Status levels:
- healthy: events flowing, views fresh, no integrity issues
- stale: no events for 2+ hours
- degraded: corrupted views or missing events
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from scripts.event_store import (
    count_events,
    frame_range,
    list_frames,
    read_all_events,
    now_iso,
)
from scripts.materializer import read_view


# How long before we consider the state "stale"
STALE_THRESHOLD_HOURS = 2


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp to a datetime object."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _find_latest_event_timestamp(state_dir: Path) -> str | None:
    """Find the most recent event timestamp across all frames."""
    events = read_all_events(state_dir)
    if not events:
        return None

    latest = max(events, key=lambda e: e.get("timestamp", ""))
    return latest.get("timestamp")


def _check_view_integrity(state_dir: Path) -> list[str]:
    """Check that materialized views exist and are valid JSON.

    Returns a list of integrity issues (empty = all good).
    """
    issues: list[str] = []
    view_names = ["agents", "channels", "stats", "social_graph", "trending"]
    views_dir = state_dir / "views"

    for name in view_names:
        view_path = views_dir / f"{name}.json"
        if not view_path.exists():
            issues.append(f"Missing view: {name}.json")
            continue
        try:
            with open(view_path, "r") as f:
                data = json.loads(f.read())
            if "_meta" not in data:
                issues.append(f"View {name}.json missing _meta block")
        except json.JSONDecodeError:
            issues.append(f"View {name}.json is corrupted (invalid JSON)")
        except OSError as e:
            issues.append(f"View {name}.json unreadable: {e}")

    return issues


def _check_event_integrity(state_dir: Path) -> list[str]:
    """Check that event files are valid JSON arrays.

    Returns a list of integrity issues.
    """
    issues: list[str] = []
    events_dir = state_dir / "events"
    if not events_dir.exists():
        return issues

    for frame_file in events_dir.glob("frame-*.json"):
        try:
            with open(frame_file, "r") as f:
                data = json.loads(f.read())
            if not isinstance(data, list):
                issues.append(f"{frame_file.name} is not a JSON array")
        except json.JSONDecodeError:
            issues.append(f"{frame_file.name} is corrupted (invalid JSON)")
        except OSError as e:
            issues.append(f"{frame_file.name} unreadable: {e}")

    return issues


def check_health(state_dir: Path) -> dict[str, Any]:
    """Run a comprehensive health check on the state repo.

    Args:
        state_dir: Root directory of the state repo.

    Returns:
        Health status dict (also written to views/health.json).
    """
    now = datetime.now(timezone.utc)
    total = count_events(state_dir)
    min_frame, max_frame = frame_range(state_dir)
    frames = list_frames(state_dir)

    # Find latest event
    latest_ts = _find_latest_event_timestamp(state_dir)

    # Determine staleness
    is_stale = False
    stale_after = now + timedelta(hours=STALE_THRESHOLD_HOURS)
    if latest_ts:
        try:
            last_event_time = _parse_iso(latest_ts)
            stale_after = last_event_time + timedelta(hours=STALE_THRESHOLD_HOURS)
            is_stale = now > stale_after
        except (ValueError, TypeError):
            is_stale = True

    # Check integrity
    view_issues = _check_view_integrity(state_dir)
    event_issues = _check_event_integrity(state_dir)
    all_issues = view_issues + event_issues

    # Determine status
    if all_issues:
        status = "degraded"
    elif is_stale:
        status = "stale"
    else:
        status = "healthy"

    # Check views materialization freshness
    views_ts = None
    stats_view = read_view(state_dir, "stats")
    if stats_view:
        meta = stats_view.get("_meta", {})
        views_ts = meta.get("materialized_at")

    health: dict[str, Any] = {
        "status": status,
        "last_event_frame": max_frame,
        "last_event_timestamp": latest_ts,
        "total_events": total,
        "total_frames": len(frames),
        "stale_after": stale_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "views_materialized_at": views_ts,
        "integrity": "verified" if not all_issues else "issues_found",
        "issues": all_issues if all_issues else None,
        "checked_at": now_iso(),
    }

    # Write health view
    _write_health(state_dir, health)

    return health


def _write_health(state_dir: Path, health: dict[str, Any]) -> None:
    """Write the health view atomically."""
    views_dir = state_dir / "views"
    views_dir.mkdir(parents=True, exist_ok=True)
    health_path = views_dir / "health.json"

    fd, tmp_path = tempfile.mkstemp(
        dir=str(views_dir),
        prefix=".health.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(health, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, str(health_path))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
