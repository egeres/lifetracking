from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval


@given(st.integers(min_value=2, max_value=100_000))
@settings(deadline=None)
def test_node_segments_run(n: int):
    a = Node_segments_generate(
        Segments(
            [
                Time_interval.today().to_seg(),
                Time_interval.last_week().to_seg(),
                Time_interval.last_n_days(n).to_seg(),
            ]
        )
    )
    b = a.run()
    assert b is not None
    assert len(b) == 3
    assert len(b[Time_interval.today()]) == 1


def test_node_segments_run_prefect():
    a = Node_segments_generate(
        Segments(
            [
                Time_interval.today().to_seg(),
                Time_interval.last_week().to_seg(),
            ]
        )
    )
    b = a.run(prefect=True)
    assert b is not None
    assert len(b) == 2
    assert len(b[Time_interval.today()]) == 1


def test_node_segments_available():
    a = Node_segments_generate(
        Segments(
            [
                Time_interval.today().to_seg(),
                Time_interval.last_week().to_seg(),
            ]
        )
    )
    assert a.available
