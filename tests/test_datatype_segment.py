from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval


@given(st.integers(min_value=2, max_value=100_000))
def test_segments_getter(n: int):
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
    assert len(b[Time_interval.today()]) == 1
