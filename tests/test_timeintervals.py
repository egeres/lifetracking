from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def test_lastnext_n_something():
    a = Time_interval.today()
    b = Time_interval.last_n_days(0)
    c = Time_interval.next_n_days(0)
    assert a == b == c


def test_time_iterator():
    # Day resolution
    a = list(Time_interval.today().iterate_over_interval())
    assert len(a) == 1
    a = list(list(Time_interval.last_n_days(1).iterate_over_interval()))
    assert len(a) == 2
    a = list(Time_interval.last_week().iterate_over_interval())
    assert len(a) == 8

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
