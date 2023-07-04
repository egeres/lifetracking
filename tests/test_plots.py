import datetime
from datetime import timedelta

import pandas as pd

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_pandas import Node_pandas_generate
from lifetracking.graph.Time_interval import Time_interval


def test_plot_pd_countbyday_0():
    # Pre
    today = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"datetime": today + datetime.timedelta(days=+2)},
            {"datetime": today + datetime.timedelta(days=+1)},
            {"datetime": today + datetime.timedelta(days=-0)},
            {"datetime": today + datetime.timedelta(days=-1)},
            {"datetime": today + datetime.timedelta(days=-2)},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    a.name = "🤔"

    # Plot 0
    t = Time_interval.last_n_days(2)
    fig = a.plot_countbyday(t)
    assert fig is not None

    # Plot 1
    t = Time_interval.last_n_days(2)
    t.start = t.start + datetime.timedelta(days=40)
    t.end = t.end + datetime.timedelta(days=40)
    fig = a.plot_countbyday(t)
    assert fig is not None


def test_plot_pd_columns_0():
    # Pre
    today = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"datetime": today + datetime.timedelta(days=+2), "thing": 0},
            {"datetime": today + datetime.timedelta(days=+1), "thing": 0},
            {"datetime": today + datetime.timedelta(days=-0), "thing": 0},
            {"datetime": today + datetime.timedelta(days=-1), "thing": 0},
            {"datetime": today + datetime.timedelta(days=-2), "thing": 0},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    a.name = "🤔"

    # Plot 0
    t = Time_interval.last_n_days(2)
    fig = a.plot_columns(t, "thing")
    assert fig is not None

    # Plot 1
    t = Time_interval.last_n_days(2)
    t.start = t.start + datetime.timedelta(days=40)
    t.end = t.end + datetime.timedelta(days=40)
    fig = a.plot_columns(t, "thing")
    assert fig is not None


def test_plot_seg():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
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
    t.start = t.start + datetime.timedelta(days=40)
    t.end = t.end + datetime.timedelta(days=40)
    fig = b.plot_hours(t)
    assert fig is not None
