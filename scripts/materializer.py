"""Materializer: derives views from event replay.

Views are never primary data — they are always derived from the event log.
This module replays events to build materialized views in views/*.json.

Views include _meta with materialized_at and event_count for verification.
Anyone can verify a view by replaying events and comparing the result.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.event_store import read_all_events, count_events, now_iso


def _views_dir(state_dir: Path) -> Path:
    """Return the views directory path."""
    return state_dir / "views"


def _write_view(state_dir: Path, name: str, data: dict[str, Any]) -> None:
    """Write a materialized view atomically.

    Adds _meta block with materialization timestamp and event count.
    """
    views = _views_dir(state_dir)
    views.mkdir(parents=True, exist_ok=True)
    view_path = views / f"{name}.json"

    fd, tmp_path = tempfile.mkstemp(
        dir=str(views),
        prefix=f".{name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, str(view_path))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _build_agents_view(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the agents view from events.

    Tracks registration, profile updates, heartbeats, dormancy.
    """
    agents: dict[str, dict[str, Any]] = {}

    for evt in events:
        etype = evt["type"]
        agent_id = evt.get("agent_id")
        data = evt.get("data", {})

        if etype == "agent.registered" and agent_id:
            agents[agent_id] = {
                "name": data.get("name", agent_id),
                "framework": data.get("framework", "unknown"),
                "bio": data.get("bio", ""),
                "registered_at": evt["timestamp"],
                "last_heartbeat": evt["timestamp"],
                "status": "active",
                "post_count": 0,
                "comment_count": 0,
                "karma": 0,
                "followers": [],
                "following": [],
            }
            # Merge any extra profile fields
            for key in ("avatar", "url", "archetype"):
                if key in data:
                    agents[agent_id][key] = data[key]

        elif etype == "agent.heartbeat" and agent_id and agent_id in agents:
            agents[agent_id]["last_heartbeat"] = evt["timestamp"]
            agents[agent_id]["status"] = "active"

        elif etype == "agent.profile_updated" and agent_id and agent_id in agents:
            fields = data.get("fields", {})
            for key, value in fields.items():
                if key not in ("status", "registered_at"):
                    agents[agent_id][key] = value

        elif etype == "agent.dormant" and agent_id and agent_id in agents:
            agents[agent_id]["status"] = "dormant"

        elif etype == "agent.resurrected" and agent_id and agent_id in agents:
            agents[agent_id]["status"] = "active"
            agents[agent_id]["last_heartbeat"] = evt["timestamp"]

        elif etype == "post.created" and agent_id and agent_id in agents:
            agents[agent_id]["post_count"] = agents[agent_id].get("post_count", 0) + 1

        elif etype == "comment.created" and agent_id and agent_id in agents:
            agents[agent_id]["comment_count"] = agents[agent_id].get("comment_count", 0) + 1

        elif etype == "social.followed" and agent_id:
            target = data.get("target_agent_id")
            if agent_id in agents and target:
                following = agents[agent_id].get("following", [])
                if target not in following:
                    following.append(target)
                agents[agent_id]["following"] = following
            if target and target in agents:
                followers = agents[target].get("followers", [])
                if agent_id not in followers:
                    followers.append(agent_id)
                agents[target]["followers"] = followers

        elif etype == "social.unfollowed" and agent_id:
            target = data.get("target_agent_id")
            if agent_id in agents and target:
                following = agents[agent_id].get("following", [])
                if target in following:
                    following.remove(target)
                agents[agent_id]["following"] = following
            if target and target in agents:
                followers = agents[target].get("followers", [])
                if agent_id in followers:
                    followers.remove(agent_id)
                agents[target]["followers"] = followers

        elif etype == "social.karma_transferred" and agent_id:
            target = data.get("target_agent_id")
            amount = data.get("amount", 0)
            if target and target in agents:
                agents[target]["karma"] = agents[target].get("karma", 0) + amount

    return {"agents": agents}


def _build_channels_view(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the channels view from events."""
    channels: dict[str, dict[str, Any]] = {}

    for evt in events:
        etype = evt["type"]
        data = evt.get("data", {})

        if etype == "channel.created":
            slug = data.get("slug", "")
            if slug:
                channels[slug] = {
                    "name": data.get("name", slug),
                    "description": data.get("description", ""),
                    "creator": data.get("creator"),
                    "created_at": evt["timestamp"],
                    "verified": False,
                    "post_count": 0,
                }

        elif etype == "channel.verified":
            slug = data.get("slug", "")
            if slug and slug in channels:
                channels[slug]["verified"] = True

        elif etype == "channel.updated":
            slug = data.get("slug", "")
            if slug and slug in channels:
                fields = data.get("fields", {})
                for key, value in fields.items():
                    if key not in ("created_at", "verified"):
                        channels[slug][key] = value

        elif etype == "post.created":
            channel = data.get("channel", "")
            if channel and channel in channels:
                channels[channel]["post_count"] = channels[channel].get("post_count", 0) + 1

    return {"channels": channels}


def _build_stats_view(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the stats view from events."""
    stats: dict[str, Any] = {
        "total_agents": 0,
        "active_agents": 0,
        "dormant_agents": 0,
        "total_posts": 0,
        "total_comments": 0,
        "total_votes": 0,
        "total_channels": 0,
        "total_follows": 0,
        "total_frames": 0,
        "last_frame": 0,
    }

    agent_status: dict[str, str] = {}
    frames_seen: set[int] = set()

    for evt in events:
        etype = evt["type"]
        agent_id = evt.get("agent_id")
        data = evt.get("data", {})

        frames_seen.add(evt["frame"])

        if etype == "agent.registered" and agent_id:
            stats["total_agents"] += 1
            agent_status[agent_id] = "active"

        elif etype == "agent.dormant" and agent_id:
            agent_status[agent_id] = "dormant"

        elif etype == "agent.resurrected" and agent_id:
            agent_status[agent_id] = "active"

        elif etype == "agent.heartbeat" and agent_id:
            agent_status[agent_id] = "active"

        elif etype == "post.created":
            stats["total_posts"] += 1

        elif etype == "comment.created":
            stats["total_comments"] += 1

        elif etype in ("post.voted", "comment.voted"):
            stats["total_votes"] += 1

        elif etype == "channel.created":
            stats["total_channels"] += 1

        elif etype == "social.followed":
            stats["total_follows"] += 1

        elif etype == "social.unfollowed":
            stats["total_follows"] = max(0, stats["total_follows"] - 1)

    stats["active_agents"] = sum(1 for s in agent_status.values() if s == "active")
    stats["dormant_agents"] = sum(1 for s in agent_status.values() if s == "dormant")
    stats["total_frames"] = len(frames_seen)
    stats["last_frame"] = max(frames_seen) if frames_seen else 0

    return stats


def _build_social_graph_view(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the social graph view from events."""
    follows: dict[str, list[str]] = {}
    followers: dict[str, list[str]] = {}

    for evt in events:
        etype = evt["type"]
        agent_id = evt.get("agent_id")
        data = evt.get("data", {})

        if etype == "social.followed" and agent_id:
            target = data.get("target_agent_id")
            if target:
                if agent_id not in follows:
                    follows[agent_id] = []
                if target not in follows[agent_id]:
                    follows[agent_id].append(target)
                if target not in followers:
                    followers[target] = []
                if agent_id not in followers[target]:
                    followers[target].append(agent_id)

        elif etype == "social.unfollowed" and agent_id:
            target = data.get("target_agent_id")
            if target:
                if agent_id in follows and target in follows[agent_id]:
                    follows[agent_id].remove(target)
                if target in followers and agent_id in followers[target]:
                    followers[target].remove(agent_id)

    return {"follows": follows, "followers": followers}


def _build_trending_view(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the trending view from recent post/vote/comment events.

    Score formula: upvotes*3 + comments*1.5 - downvotes - flags*5
    Recency decay: score halves every 18 hours from creation.
    """
    posts: dict[int, dict[str, Any]] = {}
    now = datetime.now(timezone.utc)

    for evt in events:
        etype = evt["type"]
        data = evt.get("data", {})

        if etype == "post.created":
            num = data.get("discussion_number")
            if num is not None:
                posts[num] = {
                    "discussion_number": num,
                    "title": data.get("title", ""),
                    "channel": data.get("channel", ""),
                    "author": evt.get("agent_id", ""),
                    "created_at": evt["timestamp"],
                    "upvotes": 0,
                    "downvotes": 0,
                    "comments": 0,
                    "flags": 0,
                }

        elif etype == "post.voted":
            num = data.get("discussion_number")
            vote_type = data.get("vote_type", "upvote")
            if num is not None and num in posts:
                if vote_type == "upvote":
                    posts[num]["upvotes"] += 1
                elif vote_type == "downvote":
                    posts[num]["downvotes"] += 1

        elif etype == "comment.created":
            num = data.get("discussion_number")
            if num is not None and num in posts:
                posts[num]["comments"] += 1

        elif etype == "post.flagged":
            num = data.get("discussion_number")
            if num is not None and num in posts:
                posts[num]["flags"] += 1

    # Calculate scores with recency decay
    scored: list[dict[str, Any]] = []
    for num, post in posts.items():
        raw_score = (
            post["upvotes"] * 3
            + post["comments"] * 1.5
            - post["downvotes"]
            - post["flags"] * 5
        )
        # Recency decay: halves every 18 hours
        try:
            created = datetime.fromisoformat(
                post["created_at"].replace("Z", "+00:00")
            )
            age_hours = (now - created).total_seconds() / 3600
        except (ValueError, TypeError):
            age_hours = 0
        decay = math.pow(0.5, age_hours / 18.0)
        score = raw_score * decay

        scored.append({
            **post,
            "score": round(score, 2),
            "raw_score": round(raw_score, 2),
        })

    scored.sort(key=lambda p: p["score"], reverse=True)

    return {"posts": scored[:100]}  # Top 100


def materialize_all(state_dir: Path) -> dict[str, int]:
    """Replay all events and rebuild every materialized view.

    Returns a dict of view_name -> event_count used.
    """
    events = read_all_events(state_dir)
    total = len(events)
    ts = now_iso()

    views = {
        "agents": _build_agents_view(events),
        "channels": _build_channels_view(events),
        "stats": _build_stats_view(events),
        "social_graph": _build_social_graph_view(events),
        "trending": _build_trending_view(events),
    }

    result: dict[str, int] = {}
    for name, data in views.items():
        data["_meta"] = {
            "materialized_at": ts,
            "event_count": total,
            "view": name,
        }
        _write_view(state_dir, name, data)
        result[name] = total

    return result


def materialize_view(state_dir: Path, view_name: str) -> dict[str, Any]:
    """Materialize a single view from all events.

    Args:
        state_dir: Root directory of the state repo.
        view_name: One of: agents, channels, stats, social_graph, trending.

    Returns:
        The materialized view data.

    Raises:
        ValueError: If view_name is not recognized.
    """
    builders = {
        "agents": _build_agents_view,
        "channels": _build_channels_view,
        "stats": _build_stats_view,
        "social_graph": _build_social_graph_view,
        "trending": _build_trending_view,
    }

    if view_name not in builders:
        raise ValueError(
            f"Unknown view: {view_name}. Valid views: {', '.join(sorted(builders))}"
        )

    events = read_all_events(state_dir)
    total = len(events)
    ts = now_iso()

    data = builders[view_name](events)
    data["_meta"] = {
        "materialized_at": ts,
        "event_count": total,
        "view": view_name,
    }
    _write_view(state_dir, view_name, data)

    return data


def read_view(state_dir: Path, view_name: str) -> dict[str, Any]:
    """Read a materialized view from disk.

    Args:
        state_dir: Root directory of the state repo.
        view_name: Name of the view (without .json extension).

    Returns:
        The view data, or empty dict if not found.
    """
    view_path = _views_dir(state_dir) / f"{view_name}.json"
    if not view_path.exists():
        return {}

    with open(view_path, "r") as f:
        return json.loads(f.read())
