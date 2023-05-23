import pandas as pd

from lifetracking.graph.Node_pandas import Node_pandas_generate


def test_node_pddataframe_0():
    df = pd.DataFrame(
        [
            {"a": 0},
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 4},
            {"a": 5},
            {"a": 6},
            {"a": 7},
            {"a": 8},
            {"a": 9},
        ]
    )
    a = Node_pandas_generate(df)
    o = a.run()
    assert o is not None
    assert o.equals(df)
