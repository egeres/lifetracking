import datetime

from hypothesis import given, reproduce_failure
from hypothesis import strategies as st

from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def test_lastnext_n_something():
    a = Time_interval.today()
    b = Time_interval.last_n_days(0)
    c = Time_interval.next_n_days(0)
    assert a == b == c


def test_time_iterator_days():
    # Day resolution
    a = list(Time_interval.today().iterate_over_interval())
    assert len(a) == 1
    a = list(list(Time_interval.last_n_days(1).iterate_over_interval()))
    assert len(a) == 2
    a = list(Time_interval.last_week().iterate_over_interval())
    assert len(a) == 8


# TODO add st.choice for st.sampled_from(Time_resolution)
@given(st.integers(min_value=0, max_value=10_000))
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
