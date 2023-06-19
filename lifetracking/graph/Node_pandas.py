from __future__ import annotations

import datetime
import dis
import hashlib
import os
import warnings
from abc import abstractmethod
from typing import Callable

import pandas as pd
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Seg import Seg
from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import export_pddataframe_to_lc_single, hash_method


# Actually, the value of fn should be:
# Callable[[pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]], pd.DataFrame]
# But PrefectFuture[pd.DataFrame, Sync]] doesn't inherit properties from
# pd.DataFrame, so it gives type errors when called by the user
class Node_pandas(Node[pd.DataFrame]):
    def __init__(self) -> None:
        super().__init__()

    def operation(
        self,
        fn: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> Node_pandas:
        return Node_pandas_operation(self, fn)  # type: ignore

    def filter(self, fn: Callable[[pd.Series], bool]) -> Node_pandas:
        return Node_pandas_filter(self, fn)

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        fn: Callable[[pd.Series], str],  # Specifies a way to get the "start"
        path_filename: str,
        color: str | Callable[[pd.Series], str] | None = None,
        opacity: float | Callable[[pd.Series], float] = 1.0,
    ):
        o = self.run(t)
        assert o is not None
        export_pddataframe_to_lc_single(
            o,
            fn,
            path_filename=path_filename,
            color=color,
            opacity=opacity,
        )

    def remove_nans(self) -> Node_pandas:
        return self.operation(lambda df: df.dropna())


class Node_pandas_generate(Node_0child, Node_pandas):
    """Created for debugging purposes and such"""

    def __init__(self, df: pd.DataFrame) -> None:
        assert isinstance(df, pd.DataFrame)
        super().__init__()
        self.df = df

    def _hashstr(self) -> str:
        # TODO: Decide if all static generators will be done in this way...
        return super()._hashstr()

    def _available(self) -> bool:
        return self.df is not None

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        df = self.df.copy()
        if t is not None:
            return df[t.start : t.end]
        return df


class Node_pandas_operation(Node_1child, Node_pandas):
    """Node to apply an operation to a pandas dataframe"""

    def __init__(
        self,
        n0: Node_pandas,
        fn_operation: Callable[
            [pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]], pd.DataFrame
        ],
    ) -> None:
        assert isinstance(n0, Node_pandas)
        assert callable(fn_operation), "operation_main must be callable"
        super().__init__()
        self.n0 = n0
        self.fn_operation = fn_operation

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + hash_method(self.fn_operation)).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        return self.fn_operation(n0)


class Node_pandas_filter(Node_pandas_operation):
    """Filter rows of a dataframe based on `fn_filter`"""

    def __init__(self, n0: Node_pandas, fn_filter: Callable[[pd.Series], bool]) -> None:
        assert callable(fn_filter), "operation_main must be callable"
        assert isinstance(n0, Node_pandas)
        super().__init__(n0, lambda df: df[df.apply(fn_filter, axis=1)])  # type: ignore


class Node_pandas_remove_close(Node_pandas_operation):
    """Remove rows that are too close together in time"""

    @staticmethod
    def _remove_dupe_rows_df(
        df: pd.DataFrame,
        column_name: str,
        max_time: int | float | datetime.timedelta,
    ):
        if isinstance(max_time, datetime.timedelta):
            max_time = max_time.total_seconds() / 60.0
        if df[column_name].dtype != "datetime64[ns]":
            df[column_name] = pd.to_datetime(df[column_name])
        df = df.sort_values(by=[column_name])
        df["time_diff"] = df[column_name].diff()
        mask = df["time_diff"].isnull() | (
            df["time_diff"].dt.total_seconds() / 60.0 >= max_time
        )
        return df[mask].drop(columns=["time_diff"])

    def __init__(
        self,
        n0: Node_pandas,
        column_name: str,
        max_time: int | float | datetime.timedelta,
    ):
        assert isinstance(n0, Node_pandas)
        assert isinstance(column_name, str)
        assert isinstance(max_time, (int, float, datetime.timedelta))
        super().__init__(
            n0,
            lambda df: self._remove_dupe_rows_df(
                df,  # type: ignore
                column_name,
                max_time,
            ),
        )


class Reader_pandas(Node_0child, Node_pandas):
    """Base class for reading pandas dataframes from files of varied formats"""

    @abstractmethod
    def _gen_file_extension(self) -> str:
        """Something like .csv etc"""
        ...  # pragma: no cover

    @abstractmethod
    def _gen_reading_method(self) -> Callable:
        """Stuff like pd.read_csv etc"""
        ...  # pragma: no cover

    def __init__(
        self,
        path_dir: str,
        dated_name: Callable[[str], datetime.datetime] | None = None,
        column_date_index: str | None = None,
    ) -> None:
        # We parse info that should be specified by the subclasses
        self.file_extension = self._gen_file_extension()
        assert self.file_extension.startswith(".")
        assert len(self.file_extension) > 1
        self.reading_method = self._gen_reading_method()
        assert callable(self.reading_method)

        # The rest of the init!
        assert isinstance(path_dir, str)
        assert isinstance(column_date_index, (str, type(None)))
        if not path_dir.endswith(self.file_extension):
            if not os.path.exists(path_dir):
                raise ValueError(f"{path_dir} does not exist")
        else:
            if not os.path.isfile(path_dir):
                raise ValueError(f"{path_dir} is not a file")
        super().__init__()
        self.path_dir = path_dir
        self.dated_name = dated_name
        self.column_date_index = column_date_index

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr() + str(self.path_dir) + str(self.column_date_index)
            ).encode()
        ).hexdigest()

    def _available(self) -> bool:
        if self.path_dir.endswith(self.file_extension):
            return os.path.exists(self.path_dir)
        else:
            return (
                os.path.isdir(self.path_dir)
                and len(
                    [
                        i
                        for i in os.listdir(self.path_dir)
                        if i.endswith(self.file_extension)
                    ]
                )
                > 0
            )

    def _operation_filter_by_date(
        self,
        t: Time_interval | None,
        filename: str,
        dated_name: Callable[[str], datetime.datetime] | None = None,
    ) -> bool:
        if t is not None:
            if dated_name is None:
                warnings.warn(
                    "No dated_name function provided,"
                    " so the files will not be filtered by date",
                    stacklevel=2,
                )
            else:
                try:
                    filename_date = dated_name(os.path.split(filename)[1])
                    if t is not None and not (t.start <= filename_date <= t.end):
                        return True
                except ValueError:
                    return True

        return False

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        # Get files
        files_to_read = (
            [self.path_dir]
            if self.path_dir.endswith(self.file_extension)
            else (
                x for x in os.listdir(self.path_dir) if x.endswith(self.file_extension)
            )
        )

        # Sort the files by the name if self.dated_name is not None
        if self.dated_name is not None:
            files_to_read = sorted(
                files_to_read,
                key=lambda x: self.dated_name(os.path.split(x)[1]),
            )

        # Load them
        to_return: list = []
        for filename in files_to_read:
            # Filter by date
            if self._operation_filter_by_date(t, filename, self.dated_name):
                continue

            # Read
            try:
                to_return.append(
                    self.reading_method(os.path.join(self.path_dir, filename))
                )
            except pd.errors.ParserError:
                print(f"Error reading {filename}")
            except ValueError:
                print(f"Error reading {filename} (value)")

        # Hehe, will I concat?? ╰(*°▽°*)╯
        if len(to_return) == 0:
            return pd.DataFrame()
        df = pd.concat(to_return, axis=0)

        # If the user specifies a date column, use it to filter the t
        if self.column_date_index is not None:
            df[self.column_date_index] = pd.to_datetime(df[self.column_date_index])
            # df = df.set_index(self.column_date_index) # Hums.....
            if t is not None:
                df = df[
                    (df[self.column_date_index] >= t.start)
                    & (df[self.column_date_index] <= t.end)
                ]
            df = df.sort_values(by=[self.column_date_index])

        return df


class Reader_csvs(Reader_pandas):
    """Can be used to read a .csv or a directory of .csv files"""

    def _gen_file_extension(self) -> str:
        return ".csv"

    def _gen_reading_method(self) -> Callable:
        return pd.read_csv


class Reader_jsons(Reader_pandas):
    """Can be used to read a .json or a directory of .json files"""

    def _gen_file_extension(self) -> str:
        return ".json"

    def _gen_reading_method(self) -> Callable:
        return pd.read_json


class Reader_csvs_datedsubfolders(Reader_csvs):
    # DOCS Add docstring

    def __init__(
        self,
        path_dir: str,
        criteria_to_select_file: Callable[[str], bool],
    ) -> None:
        super().__init__(path_dir)
        self.criteria_to_select_file = criteria_to_select_file

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.path_dir)
                + hash_method(self.criteria_to_select_file)
            ).encode()
        ).hexdigest()

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        to_return: list = []
        for dirname in os.listdir(self.path_dir):
            # Filter the folders we are interested in
            path_dir = os.path.join(self.path_dir, dirname)
            if not os.path.isdir(path_dir):
                continue
            a = datetime.datetime.strptime(dirname, "%Y-%m-%d")
            if t is not None and a not in t:
                continue

            # Get through the files we care about
            for sub_file in os.listdir(path_dir):
                if not os.path.isfile(os.path.join(path_dir, sub_file)):
                    continue
                if self.criteria_to_select_file(sub_file):
                    try:
                        to_return.append(
                            pd.read_csv(os.path.join(self.path_dir, dirname, sub_file))
                        )
                    except pd.errors.ParserError:
                        print(f"Error reading {sub_file}")
                    break

        return pd.concat(to_return, axis=0)
