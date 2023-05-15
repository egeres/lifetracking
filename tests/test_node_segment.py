from __future__ import annotations

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node import Node, run_multiple, run_multiple_parallel
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def test_0():
    a = Node_segments_generate(
        Segments(
            [
                Time_interval.today().to_seg(),
                Time_interval.last_n_days(2).to_seg(),
                Time_interval.last_week().to_seg(),
            ],
        )
    )
    b = a.run()
    assert b is not None
    assert len(b[Time_interval.today()]) == 1
