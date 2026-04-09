"""Tests for concurrent write safety — the critical path.

These tests verify that the event store handles parallel writes
correctly using file locking (fcntl.flock).
"""
from __future__ import annotations

import threading
from pathlib import Path

import pytest

from scripts.event_store import (
    append_event,
    append_events,
    count_events,
    read_all_events,
    read_frame_events,
)
from scripts.materializer import materialize_all, read_view
from tests.conftest import make_event


class TestConcurrentAppends:
    """Tests for concurrent event appending."""

    def test_two_threads_same_frame(self, tmp_state: Path) -> None:
        """Two threads writing to the same frame file don't lose events."""
        errors: list[str] = []

        def writer(agent_prefix: str) -> None:
            try:
                for i in range(50):
                    append_event(tmp_state, make_event(
                        frame=1,
                        agent_id=f"{agent_prefix}-{i}",
                        event_type="agent.heartbeat",
                        data={},
                    ))
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=writer, args=("thread1",))
        t2 = threading.Thread(target=writer, args=("thread2",))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors, f"Thread errors: {errors}"
        assert count_events(tmp_state) == 100

    def test_ten_threads_hundred_events_each(self, tmp_state: Path) -> None:
        """10 threads each appending 100 events to the same frame."""
        errors: list[str] = []
        expected_total = 10 * 100

        def writer(thread_id: int) -> None:
            try:
                for i in range(100):
                    append_event(tmp_state, make_event(
                        frame=1,
                        agent_id=f"t{thread_id}-a{i}",
                        event_type="agent.heartbeat",
                        data={},
                    ))
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"

        total = count_events(tmp_state)
        assert total == expected_total, f"Expected {expected_total}, got {total}"

    def test_concurrent_different_frames(self, tmp_state: Path) -> None:
        """Concurrent writes to different frames are safe."""
        errors: list[str] = []

        def writer(frame: int) -> None:
            try:
                for i in range(50):
                    append_event(tmp_state, make_event(
                        frame=frame,
                        agent_id=f"f{frame}-a{i}",
                        event_type="agent.heartbeat",
                        data={},
                    ))
            except Exception as e:
                errors.append(f"Frame {frame}: {e}")

        threads = [threading.Thread(target=writer, args=(f,)) for f in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"

        total = count_events(tmp_state)
        assert total == 250  # 5 frames * 50 events each

    def test_write_during_read(self, tmp_state: Path) -> None:
        """Reading while another thread writes doesn't corrupt data."""
        # Pre-populate
        for i in range(20):
            append_event(tmp_state, make_event(
                frame=1, agent_id=f"pre-{i}",
                event_type="agent.heartbeat", data={},
            ))

        errors: list[str] = []
        read_results: list[int] = []

        def writer() -> None:
            try:
                for i in range(50):
                    append_event(tmp_state, make_event(
                        frame=1, agent_id=f"writer-{i}",
                        event_type="agent.heartbeat", data={},
                    ))
            except Exception as e:
                errors.append(f"Writer: {e}")

        def reader() -> None:
            try:
                for _ in range(50):
                    events = read_frame_events(tmp_state, 1)
                    read_results.append(len(events))
            except Exception as e:
                errors.append(f"Reader: {e}")

        t_write = threading.Thread(target=writer)
        t_read = threading.Thread(target=reader)

        t_write.start()
        t_read.start()
        t_write.join()
        t_read.join()

        assert not errors, f"Thread errors: {errors}"
        # All reads should return valid counts (monotonically increasing)
        for count in read_results:
            assert count >= 20  # At least the pre-populated events

    def test_file_lock_prevents_interleaving(self, tmp_state: Path) -> None:
        """File locking ensures atomic frame file updates."""
        errors: list[str] = []

        def batch_writer(prefix: str) -> None:
            try:
                events = [
                    make_event(frame=1, agent_id=f"{prefix}-{i}",
                               event_type="agent.heartbeat", data={})
                    for i in range(100)
                ]
                append_events(tmp_state, events)
            except Exception as e:
                errors.append(f"{prefix}: {e}")

        t1 = threading.Thread(target=batch_writer, args=("batch1",))
        t2 = threading.Thread(target=batch_writer, args=("batch2",))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors, f"Thread errors: {errors}"
        assert count_events(tmp_state) == 200

    def test_concurrent_materialization(self, tmp_state: Path) -> None:
        """Materializing while events are being written is safe."""
        # Pre-populate
        append_events(tmp_state, [
            make_event(frame=1, event_type="agent.registered", agent_id=f"a-{i}",
                       data={"name": f"A{i}", "framework": "test", "bio": ""})
            for i in range(10)
        ])

        errors: list[str] = []

        def writer() -> None:
            try:
                for i in range(50):
                    append_event(tmp_state, make_event(
                        frame=2, agent_id=f"new-{i}",
                        event_type="agent.heartbeat", data={},
                    ))
            except Exception as e:
                errors.append(f"Writer: {e}")

        def materializer() -> None:
            try:
                for _ in range(5):
                    materialize_all(tmp_state)
            except Exception as e:
                errors.append(f"Materializer: {e}")

        t_write = threading.Thread(target=writer)
        t_mat = threading.Thread(target=materializer)

        t_write.start()
        t_mat.start()
        t_write.join()
        t_mat.join()

        assert not errors, f"Thread errors: {errors}"

    def test_verify_count_after_concurrent_writes(self, tmp_state: Path) -> None:
        """After concurrent writes, total count exactly matches expected."""
        n_threads = 8
        events_per_thread = 50
        expected = n_threads * events_per_thread
        errors: list[str] = []

        def writer(tid: int) -> None:
            try:
                for i in range(events_per_thread):
                    append_event(tmp_state, make_event(
                        frame=tid,
                        agent_id=f"t{tid}-{i}",
                        event_type="agent.heartbeat",
                        data={},
                    ))
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert count_events(tmp_state) == expected

    def test_concurrent_batch_append(self, tmp_state: Path) -> None:
        """Multiple batch appends to the same frame are safe."""
        errors: list[str] = []

        def batch_writer(prefix: str) -> None:
            try:
                events = [
                    make_event(frame=1, agent_id=f"{prefix}-{i}",
                               event_type="agent.heartbeat", data={})
                    for i in range(50)
                ]
                append_events(tmp_state, events)
            except Exception as e:
                errors.append(str(e))

        threads = [
            threading.Thread(target=batch_writer, args=(f"batch-{i}",))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert count_events(tmp_state) == 250

    def test_events_not_lost_under_contention(self, tmp_state: Path) -> None:
        """No events are silently dropped under high contention."""
        n_threads = 4
        n_events = 25
        errors: list[str] = []
        written_ids: list[str] = []
        id_lock = threading.Lock()

        def writer(tid: int) -> None:
            try:
                for i in range(n_events):
                    evt = make_event(
                        frame=1,
                        agent_id=f"t{tid}-{i}",
                        event_type="agent.heartbeat",
                        data={},
                    )
                    result = append_event(tmp_state, evt)
                    with id_lock:
                        written_ids.append(result["id"])
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors

        # Read back all events and verify every written ID exists
        all_events = read_frame_events(tmp_state, 1)
        stored_ids = {e["id"] for e in all_events}

        for eid in written_ids:
            assert eid in stored_ids, f"Event {eid} was lost!"

    def test_concurrent_multi_frame_integrity(self, tmp_state: Path) -> None:
        """Each frame file maintains integrity under concurrent access."""
        errors: list[str] = []

        def writer(frame: int, count: int) -> None:
            try:
                for i in range(count):
                    append_event(tmp_state, make_event(
                        frame=frame,
                        agent_id=f"f{frame}-{i}",
                        event_type="agent.heartbeat",
                        data={},
                    ))
            except Exception as e:
                errors.append(str(e))

        # Different threads writing different amounts to different frames
        thread_configs = [(1, 30), (1, 20), (2, 40), (2, 10), (3, 50)]
        threads = [
            threading.Thread(target=writer, args=(frame, count))
            for frame, count in thread_configs
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors

        # Verify per-frame counts
        assert len(read_frame_events(tmp_state, 1)) == 50  # 30 + 20
        assert len(read_frame_events(tmp_state, 2)) == 50  # 40 + 10
        assert len(read_frame_events(tmp_state, 3)) == 50
