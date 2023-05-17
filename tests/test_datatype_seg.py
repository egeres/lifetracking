import datetime
from typing import Any

import pytest
from hypothesis import given
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


def test_seg_repr():
    a = Seg(
        datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
        datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
    )
    b = Seg(
        datetime.datetime(2021, 1, 1, 0, 0, 0, 0),
        datetime.datetime(2021, 1, 1, 23, 59, 59, 999999),
        "Huh, I could write text here?? 🤡",
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
        "🤗 I break this __eq__, wii",
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
