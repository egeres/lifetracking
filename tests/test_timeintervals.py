from lifetracking.graph.Time_interval import Time_interval


def test_lastnext_n_something():
    a = Time_interval.today()
    b = Time_interval.last_n_days(0)
    c = Time_interval.next_n_days(0)

    assert a == b == c
