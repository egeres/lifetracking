from lifetracking.graph.Time_interval import Time_interval


def test_lastnext_n_something():
    a = Time_interval.today()
    b = Time_interval.last_n_days(0)
    c = Time_interval.next_n_days(0)
    assert a == b == c


def test_day_iterator():
    a = list(Time_interval.today().iterate_over_days())
    assert len(a) == 1
    a = list(list(Time_interval.last_n_days(1).iterate_over_days()))
    assert len(a) == 2
    a = list(Time_interval.last_week().iterate_over_days())
    assert len(a) == 8
