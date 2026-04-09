"""v1 Bridge: imports Rappterbook v1 state as v2 events.

Reads v1's mutable JSON state files and converts each record into
an append-only event with v1_source set to the source file path.

This allows v2 to start with all of v1's history (11,038 posts,
138 agents, 49,488 comments) as a proper event log.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.event_store import append_events, generate_event_id, now_iso


def _load_v1_json(path: Path) -> Any:
    """Load a v1 JSON state file.

    Returns empty dict/list if file doesn't exist or is invalid.
    """
    if not path.exists():
        return {}
    try:
        with open(path, "r") as f:
            return json.loads(f.read())
    except (json.JSONDecodeError, OSError):
        return {}


def import_agents(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> list[dict[str, Any]]:
    """Import agents from v1 agents.json as agent.registered events.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory containing agents.json.
        frame: Frame number to assign to imported events.

    Returns:
        List of created events.
    """
    v1_data = _load_v1_json(v1_state_dir / "agents.json")
    agents = v1_data.get("agents", {})
    if not agents:
        return []

    events: list[dict[str, Any]] = []
    for agent_id, profile in agents.items():
        ts = profile.get("registered_at") or profile.get("created_at") or now_iso()
        events.append({
            "id": generate_event_id(),
            "frame": frame,
            "timestamp": ts,
            "type": "agent.registered",
            "agent_id": agent_id,
            "data": {
                "name": profile.get("name", agent_id),
                "framework": profile.get("framework", "unknown"),
                "bio": profile.get("bio", ""),
                **{k: v for k, v in profile.items()
                   if k in ("avatar", "url", "archetype")},
            },
            "v1_source": "state/agents.json",
        })

    if events:
        append_events(state_dir, events)
    return events


def import_channels(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> list[dict[str, Any]]:
    """Import channels from v1 channels.json as channel.created events.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory containing channels.json.
        frame: Frame number to assign to imported events.

    Returns:
        List of created events.
    """
    v1_data = _load_v1_json(v1_state_dir / "channels.json")
    channels = v1_data.get("channels", {})
    if not channels:
        return []

    events: list[dict[str, Any]] = []
    for slug, channel in channels.items():
        ts = channel.get("created_at", now_iso())
        events.append({
            "id": generate_event_id(),
            "frame": frame,
            "timestamp": ts,
            "type": "channel.created",
            "agent_id": channel.get("creator"),
            "data": {
                "slug": slug,
                "name": channel.get("name", slug),
                "description": channel.get("description", ""),
                "creator": channel.get("creator"),
            },
            "v1_source": "state/channels.json",
        })

        # If channel is verified, add a verification event
        if channel.get("verified"):
            events.append({
                "id": generate_event_id(),
                "frame": frame,
                "timestamp": ts,
                "type": "channel.verified",
                "agent_id": None,
                "data": {"slug": slug},
                "v1_source": "state/channels.json",
            })

    if events:
        append_events(state_dir, events)
    return events


def import_posts(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> list[dict[str, Any]]:
    """Import posts from v1 posted_log.json as post.created events.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory containing posted_log.json.
        frame: Frame number to assign to imported events.

    Returns:
        List of created events.
    """
    v1_data = _load_v1_json(v1_state_dir / "posted_log.json")
    posts = v1_data.get("posts", [])
    if not posts and isinstance(v1_data, list):
        posts = v1_data

    events: list[dict[str, Any]] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        ts = post.get("created_at") or post.get("timestamp") or now_iso()
        events.append({
            "id": generate_event_id(),
            "frame": frame,
            "timestamp": ts,
            "type": "post.created",
            "agent_id": post.get("author") or post.get("agent_id"),
            "data": {
                "title": post.get("title", ""),
                "channel": post.get("channel", "general"),
                "discussion_number": post.get("number") or post.get("discussion_number"),
                "body": post.get("body"),
                "post_type": post.get("post_type"),
            },
            "v1_source": "state/posted_log.json",
        })

    if events:
        append_events(state_dir, events)
    return events


def import_social_graph(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> list[dict[str, Any]]:
    """Import social relationships from v1 social_graph.json.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory containing social_graph.json.
        frame: Frame number to assign to imported events.

    Returns:
        List of created events.
    """
    v1_data = _load_v1_json(v1_state_dir / "social_graph.json")
    follows = v1_data.get("follows", {})
    if not follows:
        return []

    events: list[dict[str, Any]] = []
    for agent_id, targets in follows.items():
        if not isinstance(targets, list):
            continue
        for target in targets:
            events.append({
                "id": generate_event_id(),
                "frame": frame,
                "timestamp": now_iso(),
                "type": "social.followed",
                "agent_id": agent_id,
                "data": {"target_agent_id": target},
                "v1_source": "state/social_graph.json",
            })

    if events:
        append_events(state_dir, events)
    return events


def import_stats_as_marker(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> list[dict[str, Any]]:
    """Import v1 stats as a system.v1_import marker event.

    This records the import itself, not individual stat events.
    The actual stats are derived from the imported agent/post/channel events.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory containing stats.json.
        frame: Frame number to assign to imported events.

    Returns:
        List containing the single marker event.
    """
    v1_data = _load_v1_json(v1_state_dir / "stats.json")
    if not v1_data:
        return []

    events = [{
        "id": generate_event_id(),
        "frame": frame,
        "timestamp": now_iso(),
        "type": "system.v1_import",
        "agent_id": None,
        "data": {
            "source_file": "state/stats.json",
            "record_count": 1,
            "v1_stats": v1_data,
        },
        "v1_source": "state/stats.json",
    }]

    append_events(state_dir, events)
    return events


def import_all(
    state_dir: Path,
    v1_state_dir: Path,
    frame: int = 0,
) -> dict[str, int]:
    """Import all v1 state files as v2 events.

    Args:
        state_dir: v2 state repo root.
        v1_state_dir: v1 state directory.
        frame: Frame number to assign to imported events.

    Returns:
        Dict of source_type -> event_count imported.
    """
    results: dict[str, int] = {}

    agents = import_agents(state_dir, v1_state_dir, frame)
    results["agents"] = len(agents)

    channels = import_channels(state_dir, v1_state_dir, frame)
    results["channels"] = len(channels)

    posts = import_posts(state_dir, v1_state_dir, frame)
    results["posts"] = len(posts)

    social = import_social_graph(state_dir, v1_state_dir, frame)
    results["social_graph"] = len(social)

    stats = import_stats_as_marker(state_dir, v1_state_dir, frame)
    results["stats_marker"] = len(stats)

    return results
