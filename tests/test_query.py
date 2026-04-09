"""Tests for the query interface — the read API for consumers."""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.event_store import append_event, append_events
from scripts.materializer import materialize_all
from scripts.query import (
    events_by_agent,
    events_by_type,
    events_for_frame,
    events_in_range,
    frame_list,
    frame_range,
    latest_view,
    search_events,
)
from tests.conftest import make_event


class TestEventsForFrame:
    """Tests for querying events by frame."""

    def test_events_for_existing_frame(self, tmp_state: Path) -> None:
        """Query returns events for a specific frame."""
        append_events(tmp_state, [
            make_event(frame=5, agent_id="a1", event_type="agent.heartbeat", data={}),
            make_event(frame=5, agent_id="a2", event_type="agent.heartbeat", data={}),
            make_event(frame=6, agent_id="a3", event_type="agent.heartbeat", data={}),
        ])

        events = events_for_frame(tmp_state, 5)
        assert len(events) == 2

    def test_events_for_empty_frame(self, tmp_state: Path) -> None:
        """Query for nonexistent frame returns empty list."""
        events = events_for_frame(tmp_state, 999)
        assert events == []

    def test_events_for_frame_sorted(self, tmp_state: Path) -> None:
        """Events within a frame are sorted by timestamp."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id="late", timestamp="2026-04-09T12:00:00Z",
                       event_type="agent.heartbeat", data={}),
            make_event(frame=1, agent_id="early", timestamp="2026-04-09T06:00:00Z",
                       event_type="agent.heartbeat", data={}),
        ])

        events = events_for_frame(tmp_state, 1)
        assert events[0]["agent_id"] == "early"
        assert events[1]["agent_id"] == "late"


class TestEventsByType:
    """Tests for querying events by type."""

    def test_filter_by_type(self, tmp_state: Path) -> None:
        """Query returns only events matching the type."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id="a1",
                       data={"name": "A1", "framework": "t", "bio": ""}),
            make_event(frame=1, event_type="post.created", agent_id="a1",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
            make_event(frame=2, event_type="agent.registered", agent_id="a2",
                       data={"name": "A2", "framework": "t", "bio": ""}),
        ])

        agents = events_by_type(tmp_state, "agent.registered")
        assert len(agents) == 2

        posts = events_by_type(tmp_state, "post.created")
        assert len(posts) == 1

    def test_filter_by_type_with_since_frame(self, tmp_state: Path) -> None:
        """since_frame filter excludes earlier frames."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.heartbeat", data={}),
            make_event(frame=5, event_type="agent.heartbeat", data={}),
            make_event(frame=10, event_type="agent.heartbeat", data={}),
        ])

        events = events_by_type(tmp_state, "agent.heartbeat", since_frame=5)
        assert len(events) == 2
        assert all(e["frame"] >= 5 for e in events)

    def test_no_matching_type(self, tmp_state: Path) -> None:
        """Query for a type with no events returns empty list."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.heartbeat", data={},
        ))

        events = events_by_type(tmp_state, "post.created")
        assert events == []


class TestEventsByAgent:
    """Tests for querying events by agent."""

    def test_filter_by_agent(self, tmp_state: Path) -> None:
        """Query returns only events for the specified agent."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id="sophia", event_type="agent.heartbeat", data={}),
            make_event(frame=1, agent_id="byteforge", event_type="agent.heartbeat", data={}),
            make_event(frame=2, agent_id="sophia", event_type="post.created",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
        ])

        sophia_events = events_by_agent(tmp_state, "sophia")
        assert len(sophia_events) == 2

        byte_events = events_by_agent(tmp_state, "byteforge")
        assert len(byte_events) == 1

    def test_filter_by_agent_and_type(self, tmp_state: Path) -> None:
        """Query by agent + type returns intersection."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id="a1", event_type="agent.heartbeat", data={}),
            make_event(frame=2, agent_id="a1", event_type="post.created",
                       data={"title": "Hi", "channel": "gen", "discussion_number": 1}),
            make_event(frame=3, agent_id="a1", event_type="agent.heartbeat", data={}),
        ])

        heartbeats = events_by_agent(tmp_state, "a1", event_type="agent.heartbeat")
        assert len(heartbeats) == 2

    def test_no_events_for_agent(self, tmp_state: Path) -> None:
        """Query for nonexistent agent returns empty list."""
        events = events_by_agent(tmp_state, "nonexistent")
        assert events == []


class TestLatestView:
    """Tests for reading materialized views."""

    def test_read_existing_view(self, tmp_state: Path) -> None:
        """Read a materialized view."""
        append_event(tmp_state, make_event(
            frame=1, event_type="agent.registered", agent_id="a1",
            data={"name": "A1", "framework": "t", "bio": ""},
        ))
        materialize_all(tmp_state)

        view = latest_view(tmp_state, "agents")
        assert "a1" in view["agents"]

    def test_read_nonexistent_view(self, tmp_state: Path) -> None:
        """Reading a nonexistent view returns empty dict."""
        view = latest_view(tmp_state, "nonexistent")
        assert view == {} or "_meta" in view


class TestFrameRange:
    """Tests for frame range queries."""

    def test_frame_range_with_events(self, tmp_state: Path) -> None:
        """Frame range reflects min/max frames."""
        append_events(tmp_state, [
            make_event(frame=3, event_type="agent.heartbeat", data={}),
            make_event(frame=7, event_type="agent.heartbeat", data={}),
            make_event(frame=15, event_type="agent.heartbeat", data={}),
        ])

        min_f, max_f = frame_range(tmp_state)
        assert min_f == 3
        assert max_f == 15

    def test_frame_range_empty(self, tmp_state: Path) -> None:
        """Empty store has (None, None) range."""
        assert frame_range(tmp_state) == (None, None)

    def test_frame_range_with_gaps(self, tmp_state: Path) -> None:
        """Gaps in frame numbers are reflected in range."""
        append_event(tmp_state, make_event(frame=1, event_type="agent.heartbeat", data={}))
        append_event(tmp_state, make_event(frame=100, event_type="agent.heartbeat", data={}))

        min_f, max_f = frame_range(tmp_state)
        assert min_f == 1
        assert max_f == 100


class TestFrameList:
    """Tests for listing frames."""

    def test_frame_list(self, tmp_state: Path) -> None:
        """Frame list returns sorted unique frame numbers."""
        append_events(tmp_state, [
            make_event(frame=5, event_type="agent.heartbeat", data={}),
            make_event(frame=1, event_type="agent.heartbeat", data={}),
            make_event(frame=10, event_type="agent.heartbeat", data={}),
        ])

        frames = frame_list(tmp_state)
        assert frames == [1, 5, 10]

    def test_frame_list_empty(self, tmp_state: Path) -> None:
        """Empty store returns empty frame list."""
        assert frame_list(tmp_state) == []


class TestEventsInRange:
    """Tests for range-based event queries."""

    def test_events_in_range(self, tmp_state: Path) -> None:
        """Query returns events within the frame range."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id="a1", event_type="agent.heartbeat", data={}),
            make_event(frame=5, agent_id="a2", event_type="agent.heartbeat", data={}),
            make_event(frame=10, agent_id="a3", event_type="agent.heartbeat", data={}),
            make_event(frame=15, agent_id="a4", event_type="agent.heartbeat", data={}),
        ])

        events = events_in_range(tmp_state, 5, 10)
        assert len(events) == 2
        assert all(5 <= e["frame"] <= 10 for e in events)

    def test_empty_range(self, tmp_state: Path) -> None:
        """Query for range with no events returns empty list."""
        append_event(tmp_state, make_event(frame=1, event_type="agent.heartbeat", data={}))

        events = events_in_range(tmp_state, 50, 100)
        assert events == []


class TestSearchEvents:
    """Tests for the flexible search interface."""

    def test_search_by_agent_and_type(self, tmp_state: Path) -> None:
        """Search with agent_id + type filters."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id="a1", event_type="agent.heartbeat", data={}),
            make_event(frame=1, agent_id="a1", event_type="post.created",
                       data={"title": "X", "channel": "g", "discussion_number": 1}),
            make_event(frame=1, agent_id="a2", event_type="agent.heartbeat", data={}),
        ])

        results = search_events(tmp_state, agent_id="a1", event_type="agent.heartbeat")
        assert len(results) == 1

    def test_search_with_frame_range(self, tmp_state: Path) -> None:
        """Search with since_frame and until_frame."""
        append_events(tmp_state, [
            make_event(frame=i, event_type="agent.heartbeat", data={})
            for i in range(1, 11)
        ])

        results = search_events(tmp_state, since_frame=3, until_frame=7)
        assert len(results) == 5
        assert all(3 <= e["frame"] <= 7 for e in results)

    def test_search_with_limit(self, tmp_state: Path) -> None:
        """Search with limit returns at most N results."""
        append_events(tmp_state, [
            make_event(frame=1, agent_id=f"a-{i}", event_type="agent.heartbeat", data={})
            for i in range(20)
        ])

        results = search_events(tmp_state, limit=5)
        assert len(results) == 5

    def test_search_no_filters(self, tmp_state: Path) -> None:
        """Search with no filters returns all events."""
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.heartbeat", data={}),
            make_event(frame=2, event_type="post.created",
                       data={"title": "X", "channel": "g", "discussion_number": 1}),
        ])

        results = search_events(tmp_state)
        assert len(results) == 2
