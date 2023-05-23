import pandas as pd

from lifetracking.graph.Node_pandas import Node_pandas_filter, Node_pandas_generate


def test_node_pddataframe_0():
    df = pd.DataFrame([{"a": 0}, {"a": 1}, {"a": 2}])
    a = Node_pandas_generate(df)
    o = a.run()
    assert o is not None
    assert o.equals(df)


def test_node_pddataframe_filter():
    df = pd.DataFrame([{"a": 0, "b": 0}, {"a": 1, "b": 1}, {"a": 2, "b": 2}])
    a = Node_pandas_generate(df)
    b = Node_pandas_filter(a, lambda x: x["a"] % 2 == 0)
    o = b.run()
    assert o is not None
    assert o.reset_index(drop=True).equals(
        pd.DataFrame([{"a": 0, "b": 0}, {"a": 2, "b": 2}])
    )
