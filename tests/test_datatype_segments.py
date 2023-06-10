from __future__ import annotations

import copy
import datetime
import json
import os
import tempfile

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Time_interval import Time_interval


def test_segments_getitem_int():
    a = Time_interval.today().to_seg()
    b = Segments(
        [
            a,
            Time_interval.last_week().to_seg(),
            # Time_interval.last_n_days(n).to_seg(),
        ]
    )
    assert b[-1] == a

    with pytest.raises(TypeError):
        b["asdadad"]  # type: ignore


@given(st.integers(min_value=2, max_value=100_000))
def test_segments_getitem_timeslice(n: int):
    a = Segments(
        [
            Time_interval.today().to_seg(),
            Time_interval.last_week().to_seg(),
            Time_interval.last_n_days(n).to_seg(),
        ]
    )
    assert len(a[Time_interval.today()]) == 1


@given(st.integers(min_value=2, max_value=100_000))
def test_segments_hash(n: int):
    a = Segments(
        [
            Time_interval.today().to_seg(),
            Time_interval.last_week().to_seg(),
            Time_interval.last_n_days(n).to_seg(),
        ]
    )
    b = Segments(
        [
            Time_interval.today().to_seg(),
            Time_interval.last_week().to_seg(),
            Time_interval.last_n_days(n).to_seg(),
        ]
    )
    assert a._hashstr() == b._hashstr()
    assert a[Time_interval.today()]._hashstr() == b[Time_interval.today()]._hashstr()


def test_segments_minmax_add():
    a = Segments(
        [
            Time_interval.today().to_seg(),
            Time_interval.last_week().to_seg(),
            Time_interval.last_trimester().to_seg(),
        ]
    )
    assert a.min() == Time_interval.last_trimester().start
    assert a.max() == Time_interval.today().end

    b = Segments(
        [
            Time_interval.tomorrow().to_seg(),
            Time_interval.last_decade().to_seg(),
        ]
    )
    c = a + b
    assert c.min() == Time_interval.last_decade().start
    assert c.max() == Time_interval.tomorrow().end


@given(st.floats(min_value=0.0, max_value=1.0))
@settings(deadline=None)  # To avoid hypothesis.errors.Flaky
def test_export_to_longcalendar(opacity: float):
    a = Segments(
        [
            Time_interval.today().to_seg(),
            Time_interval.last_week().to_seg(),
            Time_interval.last_trimester().to_seg(),
        ]
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Ok
        filename = os.path.join(tmpdirname, "a", "b", "c", "test.json")
        a.export_to_longcalendar(filename)
        a.export_to_longcalendar(filename, opacity=opacity)
        a.export_to_longcalendar(filename, tooltip=lambda x: "test")

        # Failure
        with pytest.raises(ValueError):
            filename = os.path.join(
                tmpdirname, "a_nice_subfolder", "another_sub_folder", "test.csv"
            )
            a.export_to_longcalendar(filename, opacity=opacity)


def test_export_to_longcalendar_multidays():
    a = Segments(
        [
            Time_interval.last_n_days(1).to_seg(),
        ]
    )
    with tempfile.TemporaryDirectory() as tmpdirname:
        filename = os.path.join(tmpdirname, "a", "b", "c", "test.json")
        a.export_to_longcalendar(filename)

        with open(filename) as f:
            data = json.load(f)

            # This is the important part of this test, the thing is, exporting
            # data to long calendar should split segments into other
            # sub-segments
            assert len(data) == 2


def test_segments_add():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    s0 = Segments(
        [Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1))]
    )
    s1 = Segments(
        [Seg(a + datetime.timedelta(minutes=3), a + datetime.timedelta(minutes=5))]
    )
    s2 = s0 + s1
    assert len(s2) == 2


def test_segments_merge_0():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1)),
            Seg(a + datetime.timedelta(minutes=3), a + datetime.timedelta(minutes=5)),
            Seg(
                a + datetime.timedelta(minutes=100), a + datetime.timedelta(minutes=105)
            ),
        ]
    )
    c = Segments.merge(b, 5 * 60)
    assert len(c) == 2
    assert c[0].start == b[0].start
    assert c[0].end == b[1].end

    b = Segments(
        [
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1)),
            Seg(a + datetime.timedelta(minutes=3), a + datetime.timedelta(minutes=5)),
            Seg(
                a + datetime.timedelta(minutes=90), a + datetime.timedelta(minutes=105)
            ),
        ]
    )
    c = Segments.merge(b, 0)
    assert len(c) == 3
    b["my_key"] = 0
    for i in b:
        assert i["my_key"] == 0


def test_segments_merge_1():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1)),
            Seg(a + datetime.timedelta(minutes=4), a + datetime.timedelta(minutes=5)),
            Seg(a + datetime.timedelta(minutes=8), a + datetime.timedelta(minutes=9)),
        ]
    )
    c = Segments.merge(b, 0.01)
    assert len(c) == 3

    c = Segments.merge(b, 9999)
    assert len(c) == 1


def test_segments_merge_2():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=8)),
            Seg(a + datetime.timedelta(minutes=1), a + datetime.timedelta(minutes=9)),
            Seg(a + datetime.timedelta(minutes=4), a + datetime.timedelta(minutes=5)),
        ]
    )
    # Segments look like this
    # |-------|
    #  |-------|
    #     ||
    c = Segments.merge(b, 2)
    assert len(c) == 1


def test_segments_merge_empty():
    b = Segments([])
    c = Segments.merge(b, 5 * 60)
    assert len(c) == 0

    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1))]
    )
    c = Segments.merge(b, 5 * 60)
    assert len(c) == 1


def test_segments_merge_with_customrule():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(
                a + datetime.timedelta(minutes=0),
                a + datetime.timedelta(minutes=1),
                {"my_key": 0},
            ),
            Seg(
                a + datetime.timedelta(minutes=3),
                a + datetime.timedelta(minutes=4),
                {"my_key": 1},
            ),
            Seg(
                a + datetime.timedelta(minutes=6),
                a + datetime.timedelta(minutes=7),
                {"my_key": 0},
            ),
            Seg(
                a + datetime.timedelta(minutes=9),
                a + datetime.timedelta(minutes=10),
                {"my_key": 0},
            ),
        ]
    )
    c = Segments.merge(b, 5 * 60, lambda x, y: x["my_key"] == y["my_key"])

    assert len(c) == 3
    assert c[0]["my_key"] == 0
    assert c[1]["my_key"] == 1
    assert c[2]["my_key"] == 0


def test_segments_remove_if_short():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=1)),
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=5)),
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=15)),
            Seg(a + datetime.timedelta(minutes=0), a + datetime.timedelta(minutes=40)),
        ]
    )
    c = b.remove_if_shorter_than(10 * 60)

    assert len(c) == 2
