from datetime import datetime, timedelta

import pandas as pd

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node_pandas import Node_pandas_generate
from lifetracking.graph.Time_interval import Time_interval


def test_plot_pd_countbyday_0():
    # Pre
    today = datetime.now()
    df = pd.DataFrame(
        [
            {"datetime": today + timedelta(days=+2)},
            {"datetime": today + timedelta(days=+1)},
            {"datetime": today + timedelta(days=-0)},
            {"datetime": today + timedelta(days=-1)},
            {"datetime": today + timedelta(days=-2)},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    a.name = "🤔"

    # Plot 0
    t = Time_interval.last_n_days(2)

    o = a.run(t)
    assert o is not None
    fig = a.plot_countbyday(t)
    assert fig is not None

    # Plot 1
    t = Time_interval.last_n_days(2)
    t.start = t.start + timedelta(days=40)
    t.end = t.end + timedelta(days=40)
    fig = a.plot_countbyday(t)
    assert fig is None


def test_plot_pd_countbyday_1():
    df_a = pd.DataFrame(
        [
            {"datetime": datetime(year=2023, month=2, day=1, hour=2), "cat": "b"},
            {"datetime": datetime(year=2023, month=2, day=1, hour=4), "cat": "b"},
            {"datetime": datetime(year=2023, month=2, day=1, hour=5), "cat": "b"},
        ]
    )
    a = Node_pandas_generate(df_a, datetime_column="datetime")
    df_b = pd.DataFrame(
        [
            {"datetime": datetime(year=2023, month=2, day=1), "cat": "a", "other": 0},
            {"datetime": datetime(year=2023, month=2, day=2), "cat": "a", "other": 1},
            {"datetime": datetime(year=2023, month=2, day=2), "cat": "a"},
            {"datetime": datetime(year=2023, month=2, day=2), "cat": "a"},
            {"datetime": datetime(year=2023, month=2, day=6), "cat": "a", "other": 4},
        ]
    )
    b = Node_pandas_generate(df_b, datetime_column="datetime")

    # Plot 0
    fig = (a + b).plot_countbyday()
    assert fig is not None

    # Plot 1
    fig = (a + b).plot_countbyday(stackgroup="cat")
    assert fig is not None
    assert len(fig.data) > 1  # type: ignore


def test_plot_pd_columns_0():
    # Pre
    today = datetime.now()
    df = pd.DataFrame(
        [
            {"datetime": today + timedelta(days=+2), "thing": 0},
            {"datetime": today + timedelta(days=+1), "thing": 0},
            {"datetime": today + timedelta(days=-0), "thing": 0},
            {"datetime": today + timedelta(days=-1), "thing": 0},
            {"datetime": today + timedelta(days=-2), "thing": 0},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    a.name = "🤔"
    annotations = [
        {"date": "2001-05-10", "title": "A"},
        {"date": "2001-05-15", "title": "B"},
        {"date": "2001-05-20", "title": "C"},
    ]

    # Plot 0
    t = Time_interval.last_n_days(2)
    fig = a.plot_columns(t, "thing")
    assert fig is not None

    # Plot 1
    t = Time_interval.last_n_days(2)
    t.start = t.start + timedelta(days=40)
    t.end = t.end + timedelta(days=40)
    fig = a.plot_columns(t, "thing")
    assert fig is not None

    # Plot 2
    t = Time_interval.last_n_days(2)
    fig = a.plot_columns(t, "thing", annotations=annotations)


def test_plot_seg_0():
    a = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + timedelta(minutes=0), a + timedelta(minutes=8)),
            Seg(a + timedelta(minutes=1), a + timedelta(minutes=9)),
            Seg(a + timedelta(minutes=4), a + timedelta(minutes=5)),
        ]
    )

    # Plot 0
    t = Time_interval.last_n_days(2)
    fig = b.plot_hours(t)
    assert fig is not None

    # Plot 1
    t = Time_interval.last_n_days(2)
    t.start = t.start + timedelta(days=40)
    t.end = t.end + timedelta(days=40)
    fig = b.plot_hours(t)
    assert fig is not None


def test_plot_seg_1():
    a = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + timedelta(minutes=0), a + timedelta(minutes=8), {"type": "a"}),
            Seg(a + timedelta(minutes=1), a + timedelta(minutes=9), {"type": "b"}),
        ]
    )
    t = Time_interval.last_n_days(2)
    fig = b.plot_hours(t, stackgroup="type")
    assert fig is not None
