from __future__ import annotations

import os
import tempfile

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval


@given(st.integers(min_value=2, max_value=100_000))
def test_segments_getitem_len(n: int):
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
def test_export_data_to_lc(opacity: float):
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
        a.export_to_longcalendar(filename, opacity=opacity)

        # Failure
        with pytest.raises(ValueError):
            filename = os.path.join(
                tmpdirname, "a_nice_subfolder", "another_sub_folder", "test.csv"
            )
            a.export_to_longcalendar(filename, opacity=opacity)
