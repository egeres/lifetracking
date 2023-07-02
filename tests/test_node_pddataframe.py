import datetime
import os
import tempfile

import pandas as pd
from hypothesis import given
from hypothesis import strategies as st

from lifetracking.graph.Node_pandas import (
    Node_pandas_filter,
    Node_pandas_generate,
    Node_pandas_remove_close,
    Reader_csvs,
    Reader_jsons,
)
from lifetracking.graph.Time_interval import Time_interval


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
    a = Node_pandas_generate(df, datetime_column="datetime")
    b = Node_pandas_remove_close(a, datetime.timedelta(minutes=2))

    o = b.run()
    assert o is not None
    assert len(o) == 2


file_extensions_to_test = ["csv", "json"]


@given(file_format=st.sampled_from(file_extensions_to_test))
def test_node_pddataframe_readdata_0(file_format: str):
    """Reads data from a file"""

    # Data preparation
    df = pd.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}])
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = f"{tmpdirname}/test_1000_01_01.{file_format}"

        # File creation n stuff
        if file_format == "csv":
            df.to_csv(file_path, index=False)
            reader = Reader_csvs(
                file_path,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        elif file_format == "json":
            df.to_json(file_path)
            reader = Reader_jsons(
                file_path,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        else:
            raise NotImplementedError()

        # Case 0: Simple run
        assert reader.available
        o = reader.run()
        assert o is not None
        assert o.equals(df)

        # Case 0: Run with t=today
        o = reader.run(Time_interval.today())
        assert o is not None
        assert len(o) == 0


@given(file_format=st.sampled_from(file_extensions_to_test))
def test_node_pddataframe_readdata_1(file_format: str):
    """Like before, but reads a directory instead of a file"""

    # Data preparation
    df_a = pd.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}])
    df_b = pd.DataFrame([{"a": 5}, {"a": 6}, {"a": 7}, {"a": 8}])
    with tempfile.TemporaryDirectory() as tmpdirname:
        # File creation n stuff
        if file_format == "csv":
            df_a.to_csv(
                os.path.join(tmpdirname, f"test_1000_01_01.{file_format}"), index=False
            )
            df_b.to_csv(
                os.path.join(tmpdirname, f"test_2023_01_01.{file_format}"), index=False
            )
            reader = Reader_csvs(
                tmpdirname,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        elif file_format == "json":
            df_a.to_json(os.path.join(tmpdirname, f"test_1000_01_01.{file_format}"))
            df_b.to_json(os.path.join(tmpdirname, f"test_2023_01_01.{file_format}"))
            reader = Reader_jsons(
                tmpdirname,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        else:
            raise NotImplementedError()

        # Case 0: Simple run
        assert reader.available
        o = reader.run()
        assert o is not None
        assert o.equals(pd.concat((df_a, df_b), axis=0))

        # Case 0: Run with t=last_decade
        o = reader.run(Time_interval.last_decade())  # Yeah, it will crash in 2033 😣
        assert o is not None
        assert len(o) == 4


# TODO: Test the above with `column_date_index`
