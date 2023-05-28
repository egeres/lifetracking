from __future__ import annotations

import datetime

import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_pandas import Node_pandas_generate
from lifetracking.graph.Node_segments import (
    Node_segmentize_pandas,
    Node_segments_generate,
)
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


def test_node_segments_segmentize():
    # Data setup
    d = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    df = pd.DataFrame(
        [
            # A
            {"time": d + datetime.timedelta(minutes=1), "label": "A"},
            {"time": d + datetime.timedelta(minutes=2), "label": "A"},
            {"time": d + datetime.timedelta(minutes=3), "label": "A"},
            # B
            {"time": d + datetime.timedelta(minutes=4), "label": "."},
            {"time": d + datetime.timedelta(minutes=5), "label": "."},
            {"time": d + datetime.timedelta(minutes=6), "label": "."},
            {"time": d + datetime.timedelta(minutes=7), "label": "."},
            {"time": d + datetime.timedelta(minutes=8), "label": "."},
            {"time": d + datetime.timedelta(minutes=9), "label": "."},
            # A
            {"time": d + datetime.timedelta(minutes=10), "label": "A"},
            {"time": d + datetime.timedelta(minutes=11), "label": "."},
            {"time": d + datetime.timedelta(minutes=12), "label": "A"},
        ]
    )

    # Graph & run
    a = Node_pandas_generate(df)
    b = Node_segmentize_pandas(
        a,
        ("label", ["A"]),
        "time",
    )
    o = b.run()

    # Assertions
    assert o is not None
    assert len(o) == 2
    assert o[0].start == d + datetime.timedelta(minutes=1)
    assert o[0].end == d + datetime.timedelta(minutes=3)
    assert o[1].start == d + datetime.timedelta(minutes=10)
    assert o[1].end == d + datetime.timedelta(minutes=12)

    # Prefect
    o = b.run()
    o_prefect = b.run(prefect=True)
    assert o is not None
    assert o_prefect is not None
    assert len(o) == len(o_prefect)
    assert o[0].start == o_prefect[0].start
    assert o[0].end == o_prefect[0].end
    assert o[1].start == o_prefect[1].start
    assert o[1].end == o_prefect[1].end

    # New graph
    b = Node_segmentize_pandas(a, ("label", ["A"]), "time", 99999)
    o = b.run()
    assert o is not None
    assert len(o) == 1
    assert o[0].start == d + datetime.timedelta(minutes=1)
    assert o[0].end == d + datetime.timedelta(minutes=12)


def test_node_segments_segmentize_timetosplitinmins():
    # Data setup
    d = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    df = pd.DataFrame(
        [
            # A
            {"time": d + datetime.timedelta(minutes=0), "label": "A"},
            {"time": d + datetime.timedelta(minutes=100), "label": "A"},
            {"time": d + datetime.timedelta(minutes=200), "label": "A"},
            {"time": d + datetime.timedelta(minutes=300), "label": "A"},
        ]
    )

    # Graph & run, time_to_split_in_mins=1
    a = Node_pandas_generate(df)
    b = Node_segmentize_pandas(a, ("label", ["A"]), "time", time_to_split_in_mins=1)
    o = b.run()
    assert o is not None
    assert len(o) == 0

    # Graph & run, time_to_split_in_mins=99999
    a = Node_pandas_generate(df)
    b = Node_segmentize_pandas(a, ("label", ["A"]), "time", time_to_split_in_mins=99999)
    o = b.run()
    assert o is not None
    assert len(o) == 1


def test_node_segments_segmentize_mincount():
    # Data setup
    d = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    df = pd.DataFrame(
        [
            # A
            {"time": d + datetime.timedelta(minutes=0), "label": "A"},
            {"time": d + datetime.timedelta(minutes=1), "label": "A"},
            {"time": d + datetime.timedelta(minutes=2), "label": "A"},
            {"time": d + datetime.timedelta(minutes=3), "label": "A"},
            # B
            {"time": d + datetime.timedelta(minutes=50), "label": "B"},
            {"time": d + datetime.timedelta(minutes=51), "label": "B"},
            {"time": d + datetime.timedelta(minutes=52), "label": "B"},
            {"time": d + datetime.timedelta(minutes=53), "label": "B"},
        ]
    )

    # Graph & run with min_count=1
    a = Node_pandas_generate(df)
    b = Node_segmentize_pandas(a, ("label", ["A", "B"]), "time", min_count=1)
    o = b.run()
    assert o is not None
    assert len(o) == 2

    # Graph & run with min_count=9999
    a = Node_pandas_generate(df)
    b = Node_segmentize_pandas(a, ("label", ["A", "B"]), "time", min_count=9999)
    o = b.run()
    assert o is not None
    assert len(o) == 0
