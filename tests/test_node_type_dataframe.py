import datetime
import json
import tempfile
from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pytest
from hypothesis import given
from hypothesis import strategies as st

from lifetracking.graph.Node_pandas import (
    Node_pandas_filter,
    Node_pandas_generate,
    Node_pandas_remove_close,
    Reader_csvs,
    Reader_filecreation,
    Reader_jsons,
)
from lifetracking.graph.Time_interval import Time_interval


def test_node_pddataframe_creation():
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


def test_node_pddataframe_removeifclose_0():
    t = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"a": "a", "t": t + timedelta(minutes=0)},
            {"a": "a", "t": t + timedelta(minutes=0)},  # Oh, it's dupe!
            {"a": "a", "t": t + timedelta(minutes=999999)},
        ]
    )
    df.set_index("t", inplace=True)
    assert isinstance(df.index, pd.DatetimeIndex)

    a = Node_pandas_generate(df)
    b = Node_pandas_remove_close(a, timedelta(minutes=2), keep="first")
    o = b.run()
    assert o is not None
    assert len(o) == 2


def test_node_pddataframe_removeifclose_1():
    """Like befote, but the index is not specified beforehand"""

    t = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"a": "a", "t": t + timedelta(minutes=0)},
            {"a": "a", "t": t + timedelta(minutes=0)},  # Oh, it's dupe!
            {"a": "a", "t": t + timedelta(minutes=999999)},
        ]
    )

    a = Node_pandas_generate(df)
    b = Node_pandas_remove_close(a, timedelta(minutes=2), column_name="t", keep="first")
    o = b.run()
    assert o is not None
    assert len(o) == 2


def test_node_pddataframe_removeifclose_first():
    t = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"a": "a", "datetime": t + timedelta(minutes=0)},
            {"a": "a", "datetime": t + timedelta(minutes=0)},  # Oh, it's dupe!
            {"a": "b", "datetime": t + timedelta(minutes=1)},
            {"a": "c", "datetime": t + timedelta(minutes=2)},
            {"a": "d", "datetime": t + timedelta(minutes=3)},
            {"a": "e", "datetime": t + timedelta(minutes=4)},
            {"a": "f", "datetime": t + timedelta(minutes=50)},
            {"a": "g", "datetime": t + timedelta(minutes=51)},
            {"a": "h", "datetime": t + timedelta(minutes=999)},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    b = Node_pandas_remove_close(a, timedelta(minutes=2), keep="first")
    o = b.run()
    assert o is not None
    assert len(o) == 3
    assert list(o["a"]) == ["a", "f", "h"]


def test_node_pddataframe_removeifclose_last():
    t = datetime.datetime.now()
    df = pd.DataFrame(
        [
            {"a": "a", "datetime": t + timedelta(minutes=0)},
            {"a": "b", "datetime": t + timedelta(minutes=1)},
            {"a": "c", "datetime": t + timedelta(minutes=2)},
            {"a": "d", "datetime": t + timedelta(minutes=3)},
            {"a": "e", "datetime": t + timedelta(minutes=4)},
            {"a": "e", "datetime": t + timedelta(minutes=4)},  # Dupe line :)
            {"a": "f", "datetime": t + timedelta(minutes=50)},
            {"a": "g", "datetime": t + timedelta(minutes=51)},
            {"a": "h", "datetime": t + timedelta(minutes=999)},
        ]
    )
    a = Node_pandas_generate(df, datetime_column="datetime")
    b = Node_pandas_remove_close(a, timedelta(minutes=2), keep="last")

    o = b.run()
    assert o is not None
    assert len(o) == 3
    assert list(o["a"]) == ["e", "g", "h"]


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
            raise NotImplementedError

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
        tmpdir = Path(tmpdirname)
        # File creation n stuff
        if file_format == "csv":
            df_a.to_csv(tmpdir / f"test_1000_01_01.{file_format}", index=False)
            df_b.to_csv(tmpdir / f"test_2023_01_01.{file_format}", index=False)
            reader = Reader_csvs(
                tmpdir,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        elif file_format == "json":
            df_a.to_json(tmpdir / f"test_1000_01_01.{file_format}")
            df_b.to_json(tmpdir / f"test_2023_01_01.{file_format}")
            reader = Reader_jsons(
                tmpdir,
                lambda x: datetime.datetime.strptime(x, f"test_%Y_%m_%d.{file_format}"),
            )
        else:
            raise NotImplementedError

        # Case 0: Simple run
        assert reader.available
        o = reader.run()
        assert o is not None
        assert o.equals(pd.concat((df_a, df_b), axis=0))

        # Case 0: Run with t=last_decade
        o = reader.run(Time_interval.last_decade())  # Yeah, it will crash in 2033 😣
        assert o is not None
        assert len(o) == 4


# TEST: Test the above with `column_date_index`


def test_node_pddataframe_add_0():
    df_a = pd.DataFrame([{"a": 0}, {"a": 1}, {"a": 2}])
    a = Node_pandas_generate(df_a)
    df_b = pd.DataFrame([{"a": 3}, {"a": 4}, {"a": 5}])
    b = Node_pandas_generate(df_b)
    c = a + b

    o = c.run()
    assert o is not None
    assert o.shape == (6, 1)


def test_node_pddataframe_readjson_0():
    with tempfile.TemporaryDirectory() as tmpdirname:
        t = datetime.datetime.now()
        b = [
            {"datetime": t + timedelta(minutes=0)},
            {"datetime": t + timedelta(minutes=1)},
            {"datetime": t + timedelta(minutes=2)},
            {"datetime": t + timedelta(minutes=3)},
            {"datetime": t + timedelta(minutes=4)},
            {"datetime": t + timedelta(minutes=999)},
        ]
        tmpdir = Path(tmpdirname)
        filename = tmpdir / "test.json"
        with filename.open("w") as f:
            json.dump(b, f, indent=4, default=str)
        a = Reader_jsons(str(filename), column_date_index="datetime")
        o = a.run()

        assert isinstance(o, pd.DataFrame)
        assert o.shape == (6, 0)

        o = a.plot_countbyday()
        assert isinstance(o, go.Figure)


def test_node_pddataframe_readjson_1():

    with pytest.raises(FileNotFoundError):
        Reader_jsons("/This_file_does_not_exist.json", column_date_index="datetime")

    # a = Reader_jsons("/This_file_does_not_exist.json", column_date_index="datetime")
    # o = a.run()
    # assert o is None
    # o = a.plot_countbyday()
    # assert o is None


def test_node_pddataframe_filecreation_0():
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        (tmpdir / "2020-05-30.txt").touch()
        (tmpdir / "2019-05-30.txt").touch()

        a = Reader_filecreation(
            str(tmpdir),
            lambda x: pd.to_datetime(x.name.split(".")[0], format="%Y-%m-%d"),
        )
        o = a.run()

        assert isinstance(o, pd.DataFrame)
        assert o.shape == (2, 1)


def test_node_pddataframe_filecreation_1():
    a = Reader_filecreation(
        "/this_dir_does_not_exist",
        lambda x: pd.to_datetime(x.name.split(".")[0], format="%Y-%m-%d"),
    )
    o = a.run()

    assert o is None
