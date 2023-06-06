import datetime

import pandas as pd

from lifetracking.graph.Node_pandas import (
    Node_pandas_filter,
    Node_pandas_generate,
    Node_pandas_remove_close,
)


def test_node_pddataframe_0():
    df = pd.DataFrame([{"a": 0}, {"a": 1}, {"a": 2}])
    a = Node_pandas_generate(df)

    assert a.available
    assert len(a.children) == 0
    o = a.run()
    assert o is not None
    assert o.equals(df)


def test_node_pddataframe_filter():
    df = pd.DataFrame([{"a": 0, "b": 0}, {"a": 1, "b": 1}, {"a": 2, "b": 2}])
    a = Node_pandas_generate(df)
    b = Node_pandas_filter(a, lambda x: x["a"] % 2 == 0)

    assert b.available
    assert len(b.children) == 1
    o = b.run()
    assert o is not None
    assert o.reset_index(drop=True).equals(
        pd.DataFrame([{"a": 0, "b": 0}, {"a": 2, "b": 2}])
    )


def test_node_pddataframe_removeifclose():
    t = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"datetime": t + datetime.timedelta(minutes=0)},
            {"datetime": t + datetime.timedelta(minutes=1)},
            {"datetime": t + datetime.timedelta(minutes=2)},
            {"datetime": t + datetime.timedelta(minutes=3)},
            {"datetime": t + datetime.timedelta(minutes=4)},
            {"datetime": t + datetime.timedelta(minutes=999)},
        ]
    )
    a = Node_pandas_generate(df)
    b = Node_pandas_remove_close(a, "datetime", datetime.timedelta(minutes=2))

    o = b.run()
    assert o is not None
    assert len(o) == 2
