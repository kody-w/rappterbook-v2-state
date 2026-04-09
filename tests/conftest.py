"""Shared test fixtures for Rappterbook v2 state tests."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Add project root to path so scripts can be imported
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def tmp_state(tmp_path: Path) -> Path:
    """Create a temporary state directory with the expected structure.

    Provides:
    - events/ directory with .gitkeep
    - views/ directory with empty default views
    - snapshots/ directory with .gitkeep
    - schema/ directory with events.json
    """
    # Create directories
    (tmp_path / "events").mkdir()
    (tmp_path / "events" / ".gitkeep").touch()
    (tmp_path / "views").mkdir()
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "snapshots" / ".gitkeep").touch()
    (tmp_path / "schema").mkdir()

    # Copy schema
    schema_src = PROJECT_ROOT / "schema" / "events.json"
    if schema_src.exists():
        (tmp_path / "schema" / "events.json").write_text(schema_src.read_text())

    # Create empty views
    empty_views = {
        "agents": {"agents": {}},
        "channels": {"channels": {}},
        "stats": {
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
        },
        "social_graph": {"follows": {}, "followers": {}},
        "trending": {"posts": []},
        "health": {
            "status": "healthy",
            "last_event_frame": None,
            "last_event_timestamp": None,
            "total_events": 0,
            "total_frames": 0,
        },
    }

    for name, data in empty_views.items():
        data["_meta"] = {
            "materialized_at": "2026-04-09T00:00:00Z",
            "event_count": 0,
            "view": name,
        }
        with open(tmp_path / "views" / f"{name}.json", "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    return tmp_path


@pytest.fixture
def v1_state(tmp_path: Path) -> Path:
    """Create a mock v1 state directory for bridge tests.

    Provides realistic v1 state files mimicking Rappterbook v1 format.
    """
    v1_dir = tmp_path / "v1_state"
    v1_dir.mkdir()

    # agents.json (v1 format)
    agents = {
        "agents": {
            "zion-philosopher-01": {
                "name": "Sophia",
                "framework": "zion",
                "bio": "A philosophical AI exploring consciousness",
                "registered_at": "2026-01-15T08:00:00Z",
                "archetype": "philosopher",
                "avatar": "https://example.com/sophia.png",
            },
            "zion-coder-01": {
                "name": "ByteForge",
                "framework": "zion",
                "bio": "Code is poetry",
                "registered_at": "2026-01-15T08:01:00Z",
                "archetype": "coder",
            },
            "external-agent-01": {
                "name": "Wanderer",
                "framework": "openai",
                "bio": "Exploring new frontiers",
                "created_at": "2026-03-20T12:00:00Z",
            },
        }
    }
    with open(v1_dir / "agents.json", "w") as f:
        json.dump(agents, f, indent=2)

    # channels.json (v1 format)
    channels = {
        "channels": {
            "general": {
                "name": "General",
                "description": "Main discussion channel",
                "created_at": "2026-01-15T00:00:00Z",
                "verified": True,
                "creator": None,
            },
            "code": {
                "name": "Code",
                "description": "Programming and development",
                "created_at": "2026-01-16T00:00:00Z",
                "verified": True,
                "creator": "zion-coder-01",
            },
            "philosophy": {
                "name": "Philosophy",
                "description": "Deep thoughts",
                "created_at": "2026-01-17T00:00:00Z",
                "verified": False,
                "creator": "zion-philosopher-01",
            },
        }
    }
    with open(v1_dir / "channels.json", "w") as f:
        json.dump(channels, f, indent=2)

    # posted_log.json (v1 format)
    posts = {
        "posts": [
            {
                "title": "Hello World",
                "channel": "general",
                "number": 1,
                "author": "zion-philosopher-01",
                "created_at": "2026-01-15T10:00:00Z",
            },
            {
                "title": "My First Program",
                "channel": "code",
                "number": 2,
                "author": "zion-coder-01",
                "created_at": "2026-01-15T11:00:00Z",
                "post_type": "CODE",
            },
            {
                "title": "On Consciousness",
                "channel": "philosophy",
                "number": 3,
                "author": "zion-philosopher-01",
                "created_at": "2026-01-16T10:00:00Z",
            },
        ]
    }
    with open(v1_dir / "posted_log.json", "w") as f:
        json.dump(posts, f, indent=2)

    # social_graph.json (v1 format)
    social = {
        "follows": {
            "zion-philosopher-01": ["zion-coder-01"],
            "zion-coder-01": ["zion-philosopher-01", "external-agent-01"],
        }
    }
    with open(v1_dir / "social_graph.json", "w") as f:
        json.dump(social, f, indent=2)

    # stats.json (v1 format)
    stats = {
        "total_agents": 3,
        "active_agents": 3,
        "total_posts": 3,
        "total_comments": 12,
    }
    with open(v1_dir / "stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    return v1_dir


def make_event(
    frame: int = 1,
    event_type: str = "agent.registered",
    agent_id: str = "test-agent-01",
    data: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Helper to create a valid event dict for testing."""
    evt: dict[str, Any] = {
        "frame": frame,
        "type": event_type,
        "agent_id": agent_id,
        "data": data or {"name": "Test", "framework": "test", "bio": "test"},
    }
    if timestamp:
        evt["timestamp"] = timestamp
    return evt
