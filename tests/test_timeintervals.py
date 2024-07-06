import copy
import datetime
from datetime import timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def s(a: int, b: int) -> Time_interval:
    assert a > 0
    assert a < b
    """To make syntax smaller"""
    return Time_interval(
        datetime.datetime(2000, 1, a),
        datetime.datetime(2000, 1, b),
    )


def test_overlaps():
    a = s(5, 8)
    assert not a.overlaps(s(1, 3))
    assert not a.overlaps(s(1, 5))
    assert a.overlaps(s(1, 6))
    assert a.overlaps(s(1, 9))
    assert a.overlaps(s(5, 8))
    assert a.overlaps(s(6, 7))
    assert not a.overlaps(s(8, 9))
    assert not a.overlaps(s(9, 10))


def test_contains():
    # Is IIIIN
    a = datetime.datetime.now() - timedelta(days=1)
    assert a in Time_interval.last_week()

    # Is NOT in... 🥺
    assert a not in (Time_interval.last_week() + timedelta(days=1000))
    assert a not in (Time_interval.last_week() - timedelta(days=1000))


def test_timeinterval_truncate():
    a = Time_interval(
        datetime.datetime(2021, 1, 1, 12, 5, 23),
        datetime.datetime(2021, 1, 1, 13, 4, 3),
    )

    b = a.truncate(Time_resolution.HOUR)
    assert b.start == datetime.datetime(2021, 1, 1, 12, 0, 0)
    assert b.end == datetime.datetime(2021, 1, 1, 13, 59, 59, 999999)

    b = a.truncate(Time_resolution.DAY)
    assert b.start == datetime.datetime(2021, 1, 1, 0, 0, 0)
    assert b.end == datetime.datetime(2021, 1, 1, 23, 59, 59, 999999)

    with pytest.raises(AssertionError):
        b = a.truncate(999)  # type: ignore


def test_timeinterval():
    a = Time_interval.last_week()
    assert a.start < a.end
    assert a.duration_days <= 7 + 1


def test_get_overlap_innerouter():
    a = Time_interval.today()
    b = Time_interval(a.start + timedelta(hours=4), a.end - timedelta(hours=4))
    c = Time_interval(a.start - timedelta(hours=8), a.end)
    d = Time_interval(a.start, a.end + timedelta(hours=8))
    e = Time_interval(a.start - timedelta(hours=8), a.end + timedelta(hours=8))
    f = Time_interval(a.start - timedelta(hours=999), a.end - timedelta(hours=999))
    g = Time_interval(a.start + timedelta(hours=999), a.end + timedelta(hours=999))

    assert a.get_overlap_innerouter(a) == ([a], [])
    assert a.get_overlap_innerouter(b) == ([b], [])
    assert a.get_overlap_innerouter(c) == ([a], [Time_interval(c.start, a.start)])
    assert a.get_overlap_innerouter(d) == ([a], [Time_interval(a.end, d.end)])
    assert a.get_overlap_innerouter(e) == (
        [a],
        [Time_interval(e.start, a.start), Time_interval(a.end, e.end)],
    )
    assert a.get_overlap_innerouter(f) == ([], [f])
    assert a.get_overlap_innerouter(g) == ([], [g])


def test_get_overlap_innerouter_list_0():
    intervals = [s(1, 3), s(6, 8), s(10, 14)]
    overlap, non_overlap = s(5, 12).get_overlap_innerouter_list(intervals)
    assert overlap == [s(6, 8), s(10, 12)]
    assert non_overlap == [s(5, 6), s(8, 10)]


def test_get_overlap_innerouter_list_1():
    overlap, non_overlap = s(1, 5).get_overlap_innerouter_list([s(1, 5)])
    assert overlap == [s(1, 5)]
    assert non_overlap == []


def test_merge():
    assert Time_interval.merge([]) == []
    assert Time_interval.merge([s(1, 5), s(4, 8), s(10, 14)]) == [s(1, 8), s(10, 14)]
    assert Time_interval.merge([s(1, 2), s(4, 8)]) == [s(1, 2), s(4, 8)]
    assert Time_interval.merge([s(1, 9), s(3, 5)]) == [s(1, 9)]
    assert Time_interval.merge([s(1, 2), s(2, 3)]) == [s(1, 3)]


def test_normalize_ends():
    now = datetime.datetime(2013, 1, 1, 12, 4, 23, 378654)
    a = Time_interval(
        now,
        now + timedelta(hours=1, minutes=1, seconds=1, microseconds=1, milliseconds=1),
    )
    a_normalized = copy.copy(a).normalize_ends()
    assert a_normalized.start != a.start
    assert a_normalized.end != a.end

    a_normalized = a.normalize_ends()
    assert a_normalized.start == a.start
    assert a_normalized.end == a.end


def test_lastnext_n_something():
    a = Time_interval.today()
    b = Time_interval.last_n_days(0)
    c = Time_interval.next_n_days(0)
    assert a == b == c

    a = Time_interval.today()
    assert a.start.microsecond == 0
    assert a.end.microsecond == 999_999

    a = Time_interval.last_decade()
    assert a.start.microsecond == 0
    assert a.end.microsecond == 999_999


def test_time_iterator_days():
    # Uses Time_resolution.DAY by default
    assert len(list(Time_interval.today().iterate_over_interval())) == 1
    assert len(list(Time_interval.last_n_days(1).iterate_over_interval())) == 2
    assert len(list(Time_interval.last_week().iterate_over_interval())) == 8


# TODO add st.choice for st.sampled_from(Time_resolution)
@given(st.integers(min_value=0, max_value=10_000))
@settings(deadline=None)  # To avoid hypothesis.errors.Flaky
def test_time_iterator_days_procedural(n):
    now = datetime.datetime(2023, 5, 17)
    a = list(
        Time_interval.last_n_days(n, now).iterate_over_interval(Time_resolution.DAY)
    )
    assert len(a) == n + 1  # including today


def test_time_iterator_hour():
    # Hour resolution
    n = 0
    a = list(
        Time_interval.last_n_days(1 * n).iterate_over_interval(Time_resolution.HOUR)
    )
    assert len(a) == 24 + 24 * n  # (24h in a day!)
    n = 1
    a = list(
        Time_interval.last_n_days(1 * n).iterate_over_interval(Time_resolution.HOUR)
    )
    assert len(a) == 24 + 24 * n  # (48h in 2 days!)


def test_all_last_something():
    """Just to check it doesnt crash"""

    assert (
        Time_interval.last_day().duration
        < Time_interval.last_week().duration
        < Time_interval.last_month().duration
        < Time_interval.last_semester().duration
        < Time_interval.last_year().duration
        < Time_interval.last_decade().duration
    )
