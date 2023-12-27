import copy
import datetime
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis.strategies import (
    booleans,
    complex_numbers,
    dictionaries,
    floats,
    frozensets,
    integers,
    lists,
    one_of,
    sets,
    text,
)

from lifetracking.datatypes.Seg import Seg
from lifetracking.graph.Time_interval import Time_interval


def test_basic_creation():
    a = Seg(
        datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
        datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
    )
    a["value"] = "Oh, I can save stuff here?"
    _ = a["value"]

    with pytest.raises(AssertionError):
        a = Seg(
            datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
            datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
        )
        _ = a["value"]


def test_seg_repr():
    a = Seg(
        datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
        datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
    )
    b = Seg(
        datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
        datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
        {"A": "Huh, I could write text here?? 🤡"},
    )
    assert len(str(a)) < len(str(b))


def test_seg_lt():
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    b = Seg(
        datetime.datetime(2022, 1, 1),
        datetime.datetime(2022, 1, 2),
    )
    assert a < b


def test_seg_add():
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )

    # Add 1 day
    b = a + datetime.timedelta(days=1)
    assert b.start == datetime.datetime(2021, 1, 2)
    assert b.end == datetime.datetime(2021, 1, 3)

    # Add an int and capture the TypeError
    with pytest.raises(TypeError):
        b = a + 1  # type: ignore


def test_seg_sub():
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )

    # Duh 🙄
    assert a == a
    assert a == copy.copy(a)
    assert a != 1
    assert a != [1, 2, 3]
    assert a != "🤡"
    assert a != {"A": "🐷"}

    # Remove 1 day
    b = a - datetime.timedelta(days=1)
    assert b.start == datetime.datetime(2020, 12, 31)
    assert b.end == datetime.datetime(2021, 1, 1)

    # Add an int and capture the TypeError
    with pytest.raises(TypeError):
        b = a - 1  # type: ignore


def test_seg_eq():
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    b = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    assert a == b

    c = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
        {"A": "🤗 I break this __eq__, wii"},
    )
    assert a != c


@given(
    one_of(
        integers(),
        floats(allow_nan=True, allow_infinity=True),
        text(),
        lists(integers()),
        sets(integers()),
        frozensets(integers()),
        dictionaries(text(), integers()),
        booleans(),
        complex_numbers(),
    )
)
@settings(deadline=None)  # To avoid hypothesis.errors.Flaky
def test_seg_hashstr(object_of_datatype: Any):
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    b = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    assert a._hashstr() == b._hashstr()

    a.value = object_of_datatype
    b.value = object_of_datatype
    assert a._hashstr() == b._hashstr()


def test_seg_getvalue():
    a = Seg(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    a["a"] = 1
    assert a["a"] == 1

    with pytest.raises(KeyError):
        a["b"]


def test_seg_split():
    a = Seg(
        datetime.datetime(2021, 1, 1, 12),
        datetime.datetime(2021, 1, 3, 12),
        {"1+1": "2"},
    )
    b = a.split_into_segments_per_day()

    assert len(b) == 3
    assert b[0].start == datetime.datetime(2021, 1, 1, 12)
    assert b[0].end == datetime.datetime(2021, 1, 1, 23, 59, 59)
    assert b[0].value == {"1+1": "2"}
    assert b[1].start == datetime.datetime(2021, 1, 2, 0)
    assert b[1].end == datetime.datetime(2021, 1, 2, 23, 59, 59)
    assert b[1].value == {"1+1": "2"}
    assert b[2].start == datetime.datetime(2021, 1, 3, 0)
    assert b[2].end == datetime.datetime(2021, 1, 3, 12)
    assert b[2].value == {"1+1": "2"}


def test_seg_length_h():
    a = Seg(
        datetime.datetime(2021, 1, 1, 12),
        datetime.datetime(2021, 1, 3, 12),
        {"1+1": "2"},
    )
    assert a.length_h() == 48
    assert a.length_s() > a.length_m() > a.length_h() > a.length_days()


def test_seg_overlap():
    #           |---a---|
    # |---b---|
    a = Seg(datetime.datetime(2010, 1, 1), datetime.datetime(2010, 6, 1))
    b = Seg(datetime.datetime(2000, 1, 1), datetime.datetime(2000, 6, 1))
    assert not a.overlaps(b)
    assert not b.overlaps(a)

    #           |---a---|
    #             |-c-|
    c = Seg(datetime.datetime(2010, 3, 1), datetime.datetime(2010, 4, 1))
    assert c.overlaps(a)
    assert a.overlaps(c)

    #           |---a---|
    #      |---d---|
    d = Seg(datetime.datetime(2009, 8, 1), datetime.datetime(2010, 3, 1))
    assert d.overlaps(a)
    assert a.overlaps(d)

    #           |---a---|
    #               |---e---|
    e = Seg(datetime.datetime(2010, 3, 1), datetime.datetime(2010, 9, 1))
    assert e.overlaps(a)
    assert a.overlaps(e)


def test_seg_intosegmentsperday():
    a = Time_interval.today().to_seg()
    b = copy.copy(a)
    o = a.split_into_segments_per_day()

    assert a == b
    assert len(o) == 1
    assert o[0].start == b.start
    assert o[0].end == b.end
    assert o[0].value == b.value


def test_seg_get():
    a = Time_interval.today().to_seg()
    assert a.get("ok") is None
    a["ok"] = "boomer"
    assert a.get("ok") == "boomer"
