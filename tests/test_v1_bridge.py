"""Tests for the v1 bridge — importing v1 state as v2 events."""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.event_store import count_events, read_all_events
from scripts.materializer import materialize_all, read_view
from scripts.v1_bridge import (
    import_agents,
    import_all,
    import_channels,
    import_posts,
    import_social_graph,
    import_stats_as_marker,
)


class TestImportAgents:
    """Tests for importing agents from v1."""

    def test_import_agents(self, tmp_state: Path, v1_state: Path) -> None:
        """Import agents from v1 creates agent.registered events."""
        events = import_agents(tmp_state, v1_state)

        assert len(events) == 3
        types = {e["type"] for e in events}
        assert types == {"agent.registered"}

    def test_import_agents_v1_source(self, tmp_state: Path, v1_state: Path) -> None:
        """Imported agent events have v1_source set."""
        events = import_agents(tmp_state, v1_state)

        for evt in events:
            assert evt["v1_source"] == "state/agents.json"

    def test_import_agents_preserves_fields(self, tmp_state: Path, v1_state: Path) -> None:
        """Agent profile data is preserved in the event."""
        events = import_agents(tmp_state, v1_state)
        sophia = next(e for e in events if e["agent_id"] == "zion-philosopher-01")

        assert sophia["data"]["name"] == "Sophia"
        assert sophia["data"]["framework"] == "zion"
        assert sophia["data"]["bio"] == "A philosophical AI exploring consciousness"
        assert sophia["data"]["archetype"] == "philosopher"

    def test_import_agents_idempotent(self, tmp_state: Path, v1_state: Path) -> None:
        """Importing twice doubles the events (append-only)."""
        import_agents(tmp_state, v1_state)
        count_1 = count_events(tmp_state)

        import_agents(tmp_state, v1_state)
        count_2 = count_events(tmp_state)

        assert count_2 == count_1 * 2


class TestImportChannels:
    """Tests for importing channels from v1."""

    def test_import_channels(self, tmp_state: Path, v1_state: Path) -> None:
        """Import channels creates channel.created events."""
        events = import_channels(tmp_state, v1_state)

        # 3 channels + 2 verified events = 5 total
        channel_created = [e for e in events if e["type"] == "channel.created"]
        channel_verified = [e for e in events if e["type"] == "channel.verified"]
        assert len(channel_created) == 3
        assert len(channel_verified) == 2  # general and code are verified

    def test_import_channels_v1_source(self, tmp_state: Path, v1_state: Path) -> None:
        """Imported channel events have v1_source set."""
        events = import_channels(tmp_state, v1_state)
        for evt in events:
            assert evt["v1_source"] == "state/channels.json"


class TestImportPosts:
    """Tests for importing posts from v1."""

    def test_import_posts(self, tmp_state: Path, v1_state: Path) -> None:
        """Import posts creates post.created events."""
        events = import_posts(tmp_state, v1_state)

        assert len(events) == 3
        types = {e["type"] for e in events}
        assert types == {"post.created"}

    def test_import_posts_preserves_data(self, tmp_state: Path, v1_state: Path) -> None:
        """Post data is preserved in events."""
        events = import_posts(tmp_state, v1_state)
        hello = next(e for e in events if e["data"]["title"] == "Hello World")

        assert hello["data"]["channel"] == "general"
        assert hello["data"]["discussion_number"] == 1
        assert hello["agent_id"] == "zion-philosopher-01"


class TestImportSocialGraph:
    """Tests for importing social graph from v1."""

    def test_import_social_graph(self, tmp_state: Path, v1_state: Path) -> None:
        """Import social graph creates social.followed events."""
        events = import_social_graph(tmp_state, v1_state)

        assert len(events) == 3  # 1 + 2 follows
        types = {e["type"] for e in events}
        assert types == {"social.followed"}

    def test_import_social_graph_targets(self, tmp_state: Path, v1_state: Path) -> None:
        """Social graph events have correct target agents."""
        events = import_social_graph(tmp_state, v1_state)

        coder_follows = [
            e for e in events if e["agent_id"] == "zion-coder-01"
        ]
        targets = {e["data"]["target_agent_id"] for e in coder_follows}
        assert targets == {"zion-philosopher-01", "external-agent-01"}


class TestImportStats:
    """Tests for importing stats marker from v1."""

    def test_import_stats_marker(self, tmp_state: Path, v1_state: Path) -> None:
        """Stats import creates a system.v1_import marker."""
        events = import_stats_as_marker(tmp_state, v1_state)

        assert len(events) == 1
        assert events[0]["type"] == "system.v1_import"
        assert events[0]["data"]["source_file"] == "state/stats.json"


class TestImportAll:
    """Tests for full v1 import."""

    def test_import_all(self, tmp_state: Path, v1_state: Path) -> None:
        """Import all v1 state files at once."""
        results = import_all(tmp_state, v1_state)

        assert results["agents"] == 3
        assert results["channels"] == 5  # 3 created + 2 verified
        assert results["posts"] == 3
        assert results["social_graph"] == 3
        assert results["stats_marker"] == 1

    def test_import_then_materialize_matches_v1(self, tmp_state: Path, v1_state: Path) -> None:
        """After import + materialize, derived stats approximate v1 originals."""
        import_all(tmp_state, v1_state)
        materialize_all(tmp_state)

        stats = read_view(tmp_state, "stats")
        assert stats["total_agents"] == 3
        assert stats["total_posts"] == 3
        assert stats["total_channels"] == 3  # 3 channel.created events

    def test_missing_v1_files_handled(self, tmp_state: Path, tmp_path: Path) -> None:
        """Import handles missing v1 files gracefully."""
        empty_v1 = tmp_path / "empty_v1"
        empty_v1.mkdir()

        results = import_all(tmp_state, empty_v1)
        for key, count in results.items():
            assert count == 0
