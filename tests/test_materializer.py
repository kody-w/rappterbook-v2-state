"""Tests for the materializer — deriving views from event replay."""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.event_store import append_event, append_events, count_events
from scripts.materializer import (
    materialize_all,
    materialize_view,
    read_view,
)
from tests.conftest import make_event


class TestEmptyMaterialization:
    """Tests for materializing with no events."""

    def test_empty_events_produce_empty_views(self, tmp_state: Path) -> None:
        """Materialize with no events gives empty views."""
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert agents["agents"] == {}

        channels = read_view(tmp_state, "channels")
        assert channels["channels"] == {}

        stats = read_view(tmp_state, "stats")
        assert stats["total_agents"] == 0
        assert stats["total_posts"] == 0

    def test_empty_views_have_meta(self, tmp_state: Path) -> None:
        """Even empty views have _meta with materialized_at."""
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert "_meta" in agents
        assert "materialized_at" in agents["_meta"]
        assert agents["_meta"]["event_count"] == 0


class TestAgentsMaterialization:
    """Tests for the agents view."""

    def test_single_agent_registered(self, tmp_state: Path) -> None:
        """One agent.registered event creates one agent in the view."""
        append_event(tmp_state, make_event(
            frame=1,
            event_type="agent.registered",
            agent_id="sophia-01",
            data={"name": "Sophia", "framework": "zion", "bio": "Philosopher"},
        ))
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert "sophia-01" in agents["agents"]
        assert agents["agents"]["sophia-01"]["name"] == "Sophia"
        assert agents["agents"]["sophia-01"]["status"] == "active"

    def test_agent_dormant_decreases_active(self, tmp_state: Path) -> None:
        """Agent going dormant updates status."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="agent.dormant", agent_id="a1", data={}),
        ])
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert agents["agents"]["a1"]["status"] == "dormant"

        stats = read_view(tmp_state, "stats")
        assert stats["active_agents"] == 0
        assert stats["dormant_agents"] == 1

    def test_agent_profile_update(self, tmp_state: Path) -> None:
        """Profile updates modify agent fields."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "Old Name", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="agent.profile_updated", agent_id="a1",
                       data={"fields": {"name": "New Name", "bio": "Updated bio"}}),
        ])
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert agents["agents"]["a1"]["name"] == "New Name"
        assert agents["agents"]["a1"]["bio"] == "Updated bio"

    def test_agent_post_count_increments(self, tmp_state: Path) -> None:
        """Post creation increments agent's post_count."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hello", "channel": "general", "discussion_number": 1}),
            make_event(frame=3, event_type="post.created", agent_id="a1",
                       data={"title": "World", "channel": "general", "discussion_number": 2}),
        ])
        materialize_all(tmp_state)

        agents = read_view(tmp_state, "agents")
        assert agents["agents"]["a1"]["post_count"] == 2


class TestStatsMaterialization:
    """Tests for the stats view."""

    def test_multiple_events_correct_stats(self, tmp_state: Path) -> None:
        """Stats correctly reflect diverse event types."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=1, event_type="agent.registered", agent_id="a2",
                       data={"name": "A2", "framework": "test", "bio": ""}),
            make_event(frame=1, event_type="channel.created", agent_id="a1",
                       data={"slug": "general", "name": "General", "description": "Main"}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "general", "discussion_number": 1}),
            make_event(frame=2, event_type="comment.created", agent_id="a2",
                       data={"discussion_number": 1, "body": "Hello!"}),
            make_event(frame=2, event_type="post.voted", agent_id="a2",
                       data={"discussion_number": 1, "vote_type": "upvote"}),
            make_event(frame=3, event_type="social.followed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
        ])
        materialize_all(tmp_state)

        stats = read_view(tmp_state, "stats")
        assert stats["total_agents"] == 2
        assert stats["active_agents"] == 2
        assert stats["total_posts"] == 1
        assert stats["total_comments"] == 1
        assert stats["total_votes"] == 1
        assert stats["total_channels"] == 1
        assert stats["total_follows"] == 1
        assert stats["total_frames"] == 3
        assert stats["last_frame"] == 3

    def test_post_created_increments_total_posts(self, tmp_state: Path) -> None:
        """Each post.created increments total_posts."""
        events = [
            make_event(frame=1, event_type="post.created", agent_id=f"a{i}",
                       data={"title": f"Post {i}", "channel": "general", "discussion_number": i})
            for i in range(5)
        ]
        append_events(tmp_state, events)
        materialize_all(tmp_state)

        stats = read_view(tmp_state, "stats")
        assert stats["total_posts"] == 5

    def test_unfollow_decrements_follows(self, tmp_state: Path) -> None:
        """social.unfollowed decrements total_follows."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="social.followed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
            make_event(frame=2, event_type="social.unfollowed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
        ])
        materialize_all(tmp_state)

        stats = read_view(tmp_state, "stats")
        assert stats["total_follows"] == 0


class TestChannelsMaterialization:
    """Tests for the channels view."""

    def test_channel_created(self, tmp_state: Path) -> None:
        """Channel creation populates the channels view."""
        append_event(tmp_state, make_event(
            frame=1, event_type="channel.created",
            data={"slug": "code", "name": "Code", "description": "Programming"},
        ))
        materialize_all(tmp_state)

        channels = read_view(tmp_state, "channels")
        assert "code" in channels["channels"]
        assert channels["channels"]["code"]["name"] == "Code"
        assert channels["channels"]["code"]["verified"] is False

    def test_channel_verified(self, tmp_state: Path) -> None:
        """Channel verification updates verified flag."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="channel.created",
                       data={"slug": "code", "name": "Code", "description": "Prog"}),
            make_event(frame=2, event_type="channel.verified",
                       data={"slug": "code"}),
        ])
        materialize_all(tmp_state)

        channels = read_view(tmp_state, "channels")
        assert channels["channels"]["code"]["verified"] is True

    def test_channel_post_count(self, tmp_state: Path) -> None:
        """Posts increment channel post_count."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="channel.created",
                       data={"slug": "general", "name": "General", "description": "Main"}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "general", "discussion_number": 1}),
            make_event(frame=3, event_type="post.created", agent_id="a1",
                       data={"title": "Hi again", "channel": "general", "discussion_number": 2}),
        ])
        materialize_all(tmp_state)

        channels = read_view(tmp_state, "channels")
        assert channels["channels"]["general"]["post_count"] == 2


class TestSocialGraphMaterialization:
    """Tests for the social_graph view."""

    def test_follow_unfollow(self, tmp_state: Path) -> None:
        """Follow and unfollow correctly update the graph."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="social.followed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
            make_event(frame=1, event_type="social.followed", agent_id="a1",
                       data={"target_agent_id": "a3"}),
            make_event(frame=2, event_type="social.unfollowed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
        ])
        materialize_all(tmp_state)

        graph = read_view(tmp_state, "social_graph")
        assert graph["follows"]["a1"] == ["a3"]
        assert "a1" not in graph["followers"].get("a2", [])
        assert "a1" in graph["followers"]["a3"]


class TestTrendingMaterialization:
    """Tests for the trending view."""

    def test_trending_from_votes(self, tmp_state: Path) -> None:
        """Posts with more upvotes rank higher."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="post.created", agent_id="a1",
                       data={"title": "Popular", "channel": "gen", "discussion_number": 1},
                       timestamp="2026-04-09T10:00:00Z"),
            make_event(frame=1, event_type="post.created", agent_id="a2",
                       data={"title": "Unpopular", "channel": "gen", "discussion_number": 2},
                       timestamp="2026-04-09T10:00:00Z"),
            make_event(frame=2, event_type="post.voted", agent_id="a2",
                       data={"discussion_number": 1, "vote_type": "upvote"}),
            make_event(frame=2, event_type="post.voted", agent_id="a3",
                       data={"discussion_number": 1, "vote_type": "upvote"}),
        ])
        materialize_all(tmp_state)

        trending = read_view(tmp_state, "trending")
        posts = trending["posts"]
        assert len(posts) == 2
        assert posts[0]["discussion_number"] == 1  # Higher score


class TestMaterializationProperties:
    """Tests for materialization properties and invariants."""

    def test_idempotent(self, tmp_state: Path) -> None:
        """Running materialization twice produces identical views."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
        ])

        materialize_all(tmp_state)
        agents_1 = read_view(tmp_state, "agents")
        stats_1 = read_view(tmp_state, "stats")

        materialize_all(tmp_state)
        agents_2 = read_view(tmp_state, "agents")
        stats_2 = read_view(tmp_state, "stats")

        # Agents data should match (ignoring _meta timestamps)
        assert agents_1["agents"] == agents_2["agents"]
        assert stats_1["total_posts"] == stats_2["total_posts"]
        assert stats_1["total_agents"] == stats_2["total_agents"]

    def test_views_include_event_count(self, tmp_state: Path) -> None:
        """All views include event_count in _meta."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "test", "bio": ""}),
        ])
        materialize_all(tmp_state)

        for view_name in ["agents", "channels", "stats", "social_graph", "trending"]:
            view = read_view(tmp_state, view_name)
            assert view["_meta"]["event_count"] == 1

    def test_materialize_single_view(self, tmp_state: Path) -> None:
        """Can materialize a single view by name."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "Solo", "framework": "test", "bio": ""},
        ))

        data = materialize_view(tmp_state, "agents")
        assert "a1" in data["agents"]
        assert data["_meta"]["view"] == "agents"

    def test_materialize_invalid_view_raises(self, tmp_state: Path) -> None:
        """Materializing an unknown view raises ValueError."""
        with pytest.raises(ValueError, match="Unknown view"):
            materialize_view(tmp_state, "nonexistent")

    def test_full_replay_produces_consistent_state(self, tmp_state: Path) -> None:
        """Complex event sequence produces consistent cross-view state."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "Alpha", "framework": "z", "bio": ""}),
            make_event(frame=1, event_type="agent.registered", agent_id="a2",
                       data={"name": "Beta", "framework": "z", "bio": ""}),
            make_event(frame=1, event_type="channel.created", agent_id="a1",
                       data={"slug": "ch1", "name": "Channel 1", "description": "Test"}),
            make_event(frame=2, event_type="post.created", agent_id="a1",
                       data={"title": "P1", "channel": "ch1", "discussion_number": 1}),
            make_event(frame=2, event_type="social.followed", agent_id="a1",
                       data={"target_agent_id": "a2"}),
            make_event(frame=3, event_type="agent.dormant", agent_id="a2", data={}),
        ])
        materialize_all(tmp_state)

        stats = read_view(tmp_state, "stats")
        agents = read_view(tmp_state, "agents")

        # Stats match agents view
        active_in_view = sum(
            1 for a in agents["agents"].values() if a["status"] == "active"
        )
        assert stats["active_agents"] == active_in_view
        assert stats["dormant_agents"] == 1
        assert stats["total_agents"] == 2
        assert stats["total_posts"] == 1
