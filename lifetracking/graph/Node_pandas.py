from __future__ import annotations

import datetime
import hashlib
import json
import os
import tempfile
import warnings
import zipfile
from abc import abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync
from rich import print

from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.plots.graphs import (
    graph_annotate_annotations,
    graph_annotate_title,
    graph_annotate_today,
    graph_udate_layout,
)
from lifetracking.utils import (
    export_pddataframe_to_lc_single,
    hash_method,
    operator_resample_stringified,
    plot_empty,
)


# Actually, the value of fn should be:
# Callable[[pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]], pd.DataFrame]
# But PrefectFuture[pd.DataFrame, Sync]] doesn't inherit properties from
# pd.DataFrame, so it gives type errors when called by the user
class Node_pandas(Node[pd.DataFrame]):
    def __init__(self) -> None:
        super().__init__()

    def apply(
        self,
        fn: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> Node_pandas:
        return Node_pandas_operation(self, fn)  # type: ignore

    def filter(self, fn: Callable[[pd.Series], bool]) -> Node_pandas:
        return Node_pandas_filter(self, fn)

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        color: str | Callable[[pd.Series], str] | None = None,
        opacity: float | Callable[[pd.Series], float] = 1.0,
        hour_offset: float = 0,
    ):
        o = self.run(t)
        assert o is not None
        export_pddataframe_to_lc_single(
            o,
            path_filename=path_filename,
            time_offset=timedelta(hours=hour_offset),
            color=color,
            opacity=opacity,
        )

    def remove_nans(self) -> Node_pandas:
        return self.apply(lambda df: df.dropna())

    def _plot_countbyday_generatedata(
        self,
        t: Time_interval | None,
        time_col: pd.DatetimeIndex,
        smooth: int,
    ) -> list[int]:
        # We calculate a, b and cut time_col
        if t is None:
            a, b = time_col.min(), time_col.max()
        else:
            a, b = t.start, t.end
            if time_col.shape[0] > 0 and time_col[0].tzinfo != a.tzinfo:
                # a.tzinfo = time_col.iloc[0].tzinfo
                # b.tzinfo = time_col.iloc[0].tzinfo
                a = a.replace(tzinfo=time_col[0].tzinfo)
                b = b.replace(tzinfo=time_col[0].tzinfo)
            time_col = time_col[(time_col >= a) & (time_col <= b)]

        # Actual list generation
        c: list[int] = [0] * ((b - a).days + 1)
        for i in time_col:
            index = (i - a).days
            if index < 0 or index >= len(c):
                continue
            c[index] += 1
        if smooth > 0:
            c = np.convolve(c, np.ones(smooth) / smooth, mode="same").tolist()

        return c

    def plot_countbyday(
        self,
        t: Time_interval | None = None,
        smooth: int = 1,
        annotations: list | None = None,
        title: str | None = None,
        stackgroup: str | None = None,
    ) -> go.Figure | None:
        assert t is None or isinstance(t, Time_interval)
        assert isinstance(smooth, int)
        assert smooth >= 0
        assert isinstance(annotations, list) or annotations is None

        df = self.run(t)
        if df is None or len(df) == 0:
            return None
        assert isinstance(df.index, pd.DatetimeIndex)

        if stackgroup is None:
            df["count"] = 0
            df_resampled = df.resample("D").count()

            if smooth <= 1:
                # fig = px.line(df_resampled, y="count")
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=df_resampled.index, y=df_resampled["count"], fill="tonexty"
                    )
                )

                fig_min = df_resampled["count"].min()
                fig_max = df_resampled["count"].max()
            else:
                df_resampled["smoothed_count"] = (
                    df_resampled["count"].rolling(window=smooth, center=True).mean()
                )
                # fig = px.line(df_resampled, y="smoothed_count")
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=df_resampled.index,
                        y=df_resampled["smoothed_count"],
                        fill="tonexty",
                    )
                )

                fig_min = df_resampled["smoothed_count"].min()
                fig_max = df_resampled["smoothed_count"].max()

        elif isinstance(stackgroup, str):
            df_norm = df.copy()
            df_norm.index = df_norm.index.normalize()

            df_grouped = (
                df_norm.groupby([df_norm.index, stackgroup])
                .size()
                .unstack(fill_value=0)
            )
            df_grouped = df_grouped.resample("D").asfreq().fillna(0)

            if smooth > 1:
                for col in df_grouped.columns:
                    df_grouped[col] = np.convolve(
                        df_grouped[col], np.ones(smooth) / smooth, mode="same"
                    )

            fig = go.Figure()
            for col in df_grouped.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df_grouped.index,
                        y=df_grouped[col],
                        stackgroup="one",
                        name=col,
                        fill="tonexty",
                    )
                )
            fig.update_layout(hovermode="x unified")
            total_counts = df_grouped.sum(axis=1)
            fig_min = total_counts.min()
            fig_max = total_counts.max()

        else:
            raise ValueError

        graph_udate_layout(fig, t)
        fig.update_yaxes(title_text="")
        fig.update_xaxes(title_text="")
        if title is not None:
            graph_annotate_title(fig, title)
        else:
            graph_annotate_title(fig, getattr(self, "name", None))
        if t is not None:
            graph_annotate_today(fig, t, (fig_min, fig_max))
            graph_annotate_annotations(fig, t, annotations, (fig_min, fig_max))
        return fig

    def plot_columns(
        self,
        t: Time_interval | None,
        columns: str | list[str],
        resample: str | None = None,
        smooth: int = 1,
        annotations: list | None = None,
        title: str | None = None,
        right_magin_on_plot: bool = False,
        resample_mode: str = "avg",
        # TODO_2: Add y range for weight plot in fitness
    ) -> go.Figure | None:
        # Start
        assert t is None or isinstance(t, Time_interval)
        assert isinstance(columns, (str, int))
        if isinstance(columns, str):
            columns = [columns]

        # df
        df = self.run(t)
        if df is None:
            return None
        assert isinstance(df, pd.DataFrame)
        assert df.empty or isinstance(df.index, pd.DatetimeIndex)
        assert isinstance(smooth, int)
        assert smooth >= 0
        assert isinstance(resample, str) or resample is None
        assert isinstance(resample_mode, str)

        title = (self.name or "???") + " : " + "+".join(columns)
        if title is None:
            title = title

        # Return an empty figure
        if df.empty:
            return plot_empty(t, title=title, annotations=annotations)

        # Resample
        if resample is not None:
            df = df[columns]
            df = operator_resample_stringified(df.resample(resample), resample_mode)
            df = df.dropna()  # TODO_2: Why Remove NaNs? Remove this and run tests

        # fig_min, fig_max = min(0, float(df[columns].min())), float(df[columns].max())
        fig_min, fig_max = df[columns[0]].min(), df[columns[0]].max()

        # Smooth smooth
        if smooth > 1:
            for col in columns:
                df[col] = np.convolve(df[col], np.ones(smooth) / smooth, mode="same")

        # Plot
        fig = px.line(df[columns])
        graph_udate_layout(fig, t)
        graph_annotate_title(fig, title)
        if t is not None:
            graph_annotate_today(fig, t, (fig_min, fig_max))
            graph_annotate_annotations(fig, t, annotations, (fig_min, fig_max))
        if right_magin_on_plot:
            fig.update_layout(margin={"r": 250})
        return fig

    def __add__(self, other: Node_pandas) -> Node_pandas:
        return Node_pandas_add([self, other])


class Node_pandas_add(Node_pandas):
    def __init__(self, value: list[Node_pandas]) -> None:
        super().__init__()
        self.value: list[Node_pandas] = value

    def _get_children(self) -> list[Node_pandas]:
        return self.value

    def _hashstr(self):
        return hashlib.md5(
            (super()._hashstr() + "".join([v._hashstr() for v in self.value])).encode()
        ).hexdigest()

    def _operation(
        self,
        value: list[pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]],
        t: Time_interval | None = None,
    ) -> pd.DataFrame:
        # TODO: t is not used

        value = [x for x in value if x is not None]
        if len(value) == 0:
            return pd.DataFrame()

        # Return a concatenation of all the dataframes
        return pd.concat(value)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        n_out = [
            self._get_value_from_context_or_makegraph(n, t, context) for n in self.value
        ]
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n_out,
            t=t,
        )

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        n_out = [self._get_value_from_context_or_run(n, t, context) for n in self.value]
        return self._operation(
            n_out,
            t=t,
        )

    def __add__(self, other: Node_pandas) -> Node_pandas:
        """This node keeps eating other nodes if you add them"""
        self.value.append(other)
        return self


class Node_pandas_generate(Node_0child, Node_pandas):
    """Created for debugging purposes and such"""

    # TODO_2: Can take callable in the init on top of a regular df
    def __init__(self, df: pd.DataFrame, datetime_column: str | None = None) -> None:
        assert isinstance(df, pd.DataFrame)
        # assert isinstance(df.index, pd.DatetimeIndex)
        assert datetime_column is None or datetime_column in df.columns

        super().__init__()
        # TODO_2: Add support for the case that this already has a datetime
        # index 😣
        if datetime_column is not None:
            df.set_index(datetime_column, inplace=True)
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
            return df[t.end : t.start]
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
        if len(n0) == 0:
            return n0
        return self.fn_operation(n0)


class Node_pandas_filter(Node_pandas_operation):
    """Filter rows of a dataframe based on `fn_filter`"""

    def __init__(self, n0: Node_pandas, fn_filter: Callable[[pd.Series], bool]) -> None:
        assert callable(fn_filter), "operation_main must be callable"
        assert isinstance(n0, Node_pandas)
        super().__init__(n0, lambda df: df[df.apply(fn_filter, axis=1)])  # type: ignore


class Node_pandas_remove_close(Node_pandas_operation):
    """Remove rows that are too close together in time"""

    def __init__(
        self,
        n0: Node_pandas,
        max_time: datetime.timedelta,  # TODO_1: Remove int and float
        column_name: str | None = None,
        keep: str = "first",
    ):
        """Max time is assumed to be in minutes if an int or float is given"""

        assert isinstance(n0, Node_pandas)
        assert isinstance(max_time, datetime.timedelta)
        assert column_name is None or isinstance(column_name, str)
        assert keep in ["first", "last"], "keep must be 'first' or 'last'"

        super().__init__(
            n0,
            lambda df: self._remove_dupe_rows_df(
                df,  # type: ignore
                max_time,
                column_name,
                keep,
            ),
        )

    @staticmethod
    def _remove_dupe_rows_df(
        df: pd.DataFrame,
        max_time: datetime.timedelta,
        column_name: str | None = None,
        keep: str = "first",
    ):
        if df.shape[0] == 0:
            return df

        # Maybe this should be temporary?
        assert isinstance(df.index, pd.DatetimeIndex)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert isinstance(column_name, str) or column_name is None
        assert isinstance(max_time, datetime.timedelta)

        if column_name is None and isinstance(df.index, pd.DatetimeIndex):
            # Deduplicate based on index
            df = df.sort_index()
            # df["time_diff"] = df.index.to_series().diff()
            df = df.drop_duplicates()
            time_diff = df.index.to_series().diff()
        elif column_name is not None:
            if df[column_name].dtype != "datetime64[ns]":
                df[column_name] = pd.to_datetime(df[column_name])
            df = df.sort_values(by=[column_name])
            # df["time_diff"] = df[column_name].diff()
            df = df.drop_duplicates()
            time_diff = df[column_name].diff()
        else:
            msg = (
                "Either column_name must be given or the index must be a DatetimeIndex"
            )
            raise ValueError(msg)

        # Calculate mask to determine rows to keep
        mask = time_diff.isnull() | (time_diff >= max_time)

        rows_to_keep = []
        current_row: int | None = None
        if keep == "first":
            for i, _ in df.iterrows():
                if current_row is None:
                    current_row = i

                # if mask[i]:
                #     rows_to_keep.append(i)
                #     current_row = None

                # if (isinstance(mask[i], (bool, np.bool_)) and mask[i]) or all(
                #     mask.loc[i]
                # ):
                #     rows_to_keep.append(i)
                #     current_row = None

                if isinstance(mask[i], (bool, np.bool_)):
                    if mask[i]:
                        rows_to_keep.append(i)
                        current_row = None
                else:
                    if all(mask.loc[i]):
                        rows_to_keep.append(i)
                        current_row = None
            df = df.loc[rows_to_keep]

        elif keep == "last":
            reversed_indexes = df.index[::-1]
            for i in reversed_indexes:
                if current_row is None:
                    current_row = i
                if mask.loc[i]:
                    rows_to_keep.append(current_row)
                    current_row = None
            rows_to_keep.reverse()
            df = df.loc[rows_to_keep]

        return df


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
        column_date_index: str | Callable | None = None,
        time_zone: None | datetime.tzinfo = None,
    ) -> None:
        # We parse info that should be specified by the subclasses
        self.file_extension = self._gen_file_extension()
        assert self.file_extension.startswith(".")
        assert len(self.file_extension) > 1
        self.reading_method = self._gen_reading_method()
        assert callable(self.reading_method)

        if Path(path_dir).is_file() and not path_dir.endswith(self.file_extension):
            msg = (
                f"File {path_dir} does not have the correct extension "
                f".This reader only reads {self.file_extension} files"
            )
            raise ValueError(msg)

        # The rest of the init!
        assert isinstance(path_dir, str)
        assert isinstance(column_date_index, (str, type(None)))
        if not path_dir.endswith(self.file_extension) and dated_name is None:
            warnings.warn(
                "No dated_name function provided,"
                " so the files will not be filtered by date",
                stacklevel=2,
            )
        super().__init__()
        self.path_dir = path_dir
        self.dated_name = dated_name
        self.column_date_index = column_date_index
        self.time_zone = time_zone

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr() + str(self.path_dir) + str(self.column_date_index)
            ).encode()
        ).hexdigest()

    def _available(self) -> bool:
        if self.path_dir.endswith(self.file_extension):
            return os.path.exists(self.path_dir)
        return (
            os.path.isdir(self.path_dir)
            and os.path.exists(self.path_dir)
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
        dated_name: Callable[[str], datetime.datetime],
    ) -> bool:
        if t is not None:
            try:
                filename_date = dated_name(os.path.split(filename)[1])
                if isinstance(self.time_zone, datetime.tzinfo):
                    filename_date = filename_date.astimezone(self.time_zone)
                    t.start = t.start.astimezone(self.time_zone)
                    t.end = t.end.astimezone(self.time_zone)
                if t is not None and not (t.start <= filename_date <= t.end):
                    return True
            except ValueError:
                return True

        return False

    def _operation_load_raw_data(
        self,
        t: Time_interval | None,
    ):
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
            # The fn thing is to shush pylance/mypy
            fn = self.dated_name
            assert callable(fn)
            files_to_read = sorted(
                files_to_read,
                key=lambda x: fn(os.path.split(x)[1]),
            )

        # Load them
        to_return = []
        for filename in files_to_read:
            # Filter by date
            if self.dated_name is not None and self._operation_filter_by_date(
                t, filename, self.dated_name
            ):
                continue

            # Read
            try:
                to_return.append(
                    self.reading_method(os.path.join(self.path_dir, filename))
                )
            except pd.errors.ParserError:
                print(f"[red]Error reading {filename}")
            except ValueError:
                print(f"[red]Error reading {filename} (value)")

        # Hehe, will I concat?? ╰(*°▽°*)╯
        if len(to_return) == 0:
            return pd.DataFrame()
        df = pd.concat(to_return, axis=0)

        # Has Nans check?
        # It can happen if you mix df's with different column manes, like "Date", "date"
        # if df.isnull().values.any():
        #     print(f"[red]Nans in {self.path_dir}")

        return df  # noqa: RET504

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        df = self._operation_load_raw_data(t)

        if df.shape[0] == 0:
            return df

        # If the user specifies a date column, use it to filter the t
        if self.column_date_index is not None:
            df[self.column_date_index] = pd.to_datetime(
                df[self.column_date_index],
                # infer_datetime_format=True,
                format="mixed",
            )
            # df = df.set_index(self.column_date_index) # Hums.....
            if t is not None:
                # TODO: Remove this after refactors of tzinfo
                if t.start.tzinfo is None and df[self.column_date_index].iloc[0].tzinfo:
                    t.start = t.start.replace(
                        tzinfo=df[self.column_date_index].iloc[0].tzinfo
                    )
                    t.end = t.end.replace(
                        tzinfo=df[self.column_date_index].iloc[0].tzinfo
                    )

                df = df[
                    (df[self.column_date_index] >= t.start)
                    & (df[self.column_date_index] <= t.end)
                ]
            df.set_index(self.column_date_index, inplace=True)
            df.sort_index(inplace=True)

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
        column_date_index: str | None | Callable = None,
    ) -> None:
        super().__init__(path_dir)
        self.criteria_to_select_file = criteria_to_select_file
        self.column_date_index = column_date_index

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.path_dir)
                + hash_method(self.criteria_to_select_file)
            ).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return (
            os.path.isdir(self.path_dir)
            and os.path.exists(self.path_dir)
            and any(
                True
                for i in os.listdir(self.path_dir)
                # if "i is date" # TODO_2: Add this
            )
            > 0
        )

    def _operation_generate_df(self, list_of_df) -> pd.DataFrame:
        # If to_return is empty, return an empty dataframe
        if len(list_of_df) == 0:
            return pd.DataFrame()

        # df
        df = pd.concat(list_of_df, axis=0)

        # set index or something
        if isinstance(self.column_date_index, str):
            df.set_index(self.column_date_index, inplace=True)
        elif callable(self.column_date_index):
            df = self.column_date_index(df)

        # Return
        return df

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        # Asserts
        assert t is None or isinstance(t, Time_interval)
        assert os.path.exists(self.path_dir)

        to_return: list = []
        for dirname in os.listdir(self.path_dir):
            # Filter the folders we are interested in
            path_dir = os.path.join(self.path_dir, dirname)
            if not os.path.isdir(path_dir):
                continue

            a = datetime.datetime.strptime(dirname, "%Y-%m-%d")
            if t is not None and t.start.tzinfo is not None:
                a = a.replace(tzinfo=t.start.tzinfo)

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
                        print(f"[red]Error reading {sub_file}")
                    break

        return self._operation_generate_df(to_return)


class Reader_filecreation(Node_0child, Node_pandas):
    """Reader_filecreation class is responsible for generating a DataFrame that contains
    information about the filenames and their date of creation present in a specified
    directory.

    When executed, the node retrieves a pd.DataFrame with the following columns:
    - **filename**: The name of the file.
    - **date**: The date of creation of the file.

    ## Parameters
    - **path_dir (str | Path)**: Path to the directory that contains the files.
    - **fn (Callable)**: A function that takes a filename as its argument and returns a
      datetime-like object (either a `pd.Timestamp` or any datetime object).
    - **valid_extensions (list[str] | str | None, optional)**: List of valid file
      extensions to consider. If `None`, all files are considered valid.

    ## Example
    ```python
    pipeline_photos_phone = Reader_filecreation(
        "C:/Backup/Camera",
        lambda x: pd.to_datetime(
            x.split(".")[0], format="%Y%m%d_%H%M%S", errors="coerce"
        ),
        valid_extensions=[".jpg", ".jpeg", ".png", ".JPEG", ".JPG", ".PNG"],
    )
    ```

    ## Raises
    - **AssertionError**:
        - If `fn` is not callable.
        - If any item in `valid_extensions` is not a string starting with a dot.
        - If `valid_extensions` is not a list after processing.
    """

    def __init__(
        self,
        path_dir: str | Path,
        fn: Callable[[Path], pd.Timestamp | Any],
        valid_extensions: list[str] | str | None = None,
    ):
        assert callable(fn)

        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        self.path_dir: Path = path_dir
        self.fn = fn
        self.valid_extensions: list[str] = []
        if isinstance(valid_extensions, str):
            self.valid_extensions = [valid_extensions]
        assert all(
            isinstance(x, str) and x.startswith(".") for x in self.valid_extensions
        )
        assert isinstance(self.valid_extensions, list)

    def _available(self) -> bool:
        return (
            self.path_dir.exists()
            and self.path_dir.is_dir()
            and len(list(self.path_dir.glob("*"))) > 0
        )

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                # TODO_2: Add hash of fn
                super()._hashstr()
                + str(self.path_dir)
                + str(self.valid_extensions)
            ).encode()
        ).hexdigest()

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        # df
        # files = os.listdir(self.path_dir)
        files = self.path_dir.glob("*")
        df = pd.DataFrame({"filename": files})

        # Remove the ones that do not end with the valid extensions
        if len(self.valid_extensions) > 0:
            df = df[
                df["filename"].apply(lambda x: x.endswith(tuple(self.valid_extensions)))
            ]

        # Date
        df["date"] = df["filename"].apply(self.fn)
        df = df.dropna(subset=["date"])
        df = df.set_index("date")

        # t filtering
        if t is not None:
            df = df[df.index >= t.start]
            df = df[df.index <= t.end]
        return df


class Reader_telegramchat(Node_0child, Node_pandas):
    # DOCS: This one should be easy :)

    def __init__(
        self,
        id_chat: int,
        path_to_data: str | Path | None = None,
    ):
        assert isinstance(id_chat, int)

        if path_to_data is None:
            path_to_data = Path.home() / "Downloads" / "Telegram Desktop"
        if isinstance(path_to_data, str):
            path_to_data = Path(path_to_data)
        self.path_to_data: Path = path_to_data

        self.id_chat = id_chat

    def _available(self) -> bool:
        # TODO_2: Available looks if there is at least one file the with id
        return self.path_to_data.exists() and self.path_to_data.is_dir()

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                # TODO_2: Add hash of fn
                super()._hashstr()
                + str(self.path_to_data)
            ).encode()
        ).hexdigest()

    # REFACTOR: All this stuff overlaps with: Social_telegram
    # REFACTOR: All of these use Path instead of str
    def _get_chat_exports_dirs(self, path_dir_root: str) -> list[str]:
        assert os.path.exists(path_dir_root)
        to_return = []
        for x in os.listdir(path_dir_root):
            if os.path.isdir(os.path.join(path_dir_root, x)) and x.startswith(
                "ChatExport"
            ):
                to_return.append(os.path.join(path_dir_root, x))
        return to_return

    def _get_datajsons(self, path_dir_root: str) -> list[str]:
        # TODO_2: Try this syntax instead
        # return [
        #     os.path.join(i, j)
        #     for i in self._get_chat_exports_dirs(path_dir_root)
        #     for j in os.listdir(i)
        #     if j.endswith(".json") and os.path.isfile(os.path.join(i, j))
        # ]

        to_return = []
        for i in self._get_chat_exports_dirs(path_dir_root):
            for j in os.listdir(i):
                if j.endswith(".json") and os.path.isfile(os.path.join(i, j)):
                    to_return.append(os.path.join(i, j))
        return to_return

    def get_most_recent_personal_chats(self) -> str | None:
        global_filename = None
        global_last_update = datetime.datetime.min
        for filename in self._get_datajsons(str(self.path_to_data)):
            with open(filename, encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.decoder.JSONDecodeError:
                    continue
            if data["id"] != self.id_chat:
                continue
            last_update = data["messages"][-1]["date"]
            last_update = pd.to_datetime(last_update, format="%Y-%m-%dT%H:%M:%S")
            if last_update <= global_last_update:
                continue
            global_last_update = last_update
            global_filename = filename
        return global_filename

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        # Chat + data
        chat = self.get_most_recent_personal_chats()
        if chat is None:
            return None
        with open(chat, encoding="utf-8") as f:
            data = json.load(f)

        # df time
        df = pd.DataFrame(data["messages"])
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%dT%H:%M:%S")
        return df.set_index("date")


class Reader_openAI_history(Node_0child, Node_pandas):
    """Receives an export of the openAI data and returns a dataframe with the author of
    the messages. Output columns are:

    - **id**: The id of the message.
    - **status**: The status of the message.
    - **content**: The content of the message.
    - **author**: The author of the message.
    """

    def __init__(
        self,
        path_to_data: str | Path | None = None,
    ):
        if isinstance(path_to_data, str):
            path_to_data = Path(path_to_data)
        assert isinstance(path_to_data, Path)
        assert path_to_data.name.endswith(".zip")
        assert path_to_data.exists(), f"{path_to_data} does not exist"
        self.path_to_data: Path = path_to_data

        # self.path_dir_tmp = Path.home() / "tmp"
        self.path_dir_tmp = Path(tempfile.gettempdir())
        self.path_dir_tmp.mkdir(exist_ok=True)
        self.path_dir_tmp = self.path_dir_tmp / (path_to_data.name[:-4])
        if not self.path_dir_tmp.exists():
            # print(f"Extracting {path_to_data} to {self.path_dir_tmp}...")
            with zipfile.ZipFile(path_to_data, "r") as zip_ref:
                zip_ref.extractall(self.path_dir_tmp)

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                # TODO_2: Add hash of fn
                super()._hashstr()
                + str(self.path_to_data)
            ).encode()
        ).hexdigest()

    def _parse_openai_conversation(
        self, dict_conversation: dict
    ) -> list[dict[str, Any]]:
        messages = []
        for v in dict_conversation["mapping"].values():
            if v["message"] is None:
                continue
            if v["message"]["create_time"] is None:
                continue
            messages.append(
                {
                    "id": v["message"]["id"],
                    "status": v["message"]["status"],
                    "content": v["message"]["content"],
                    "author": v["message"]["author"]["role"],
                    "date": datetime.datetime.utcfromtimestamp(
                        v["message"]["create_time"]
                    ),
                }
            )
        return messages

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        assert (self.path_dir_tmp / "conversations.json").exists()

        with open(self.path_dir_tmp / "conversations.json", encoding="utf-8") as f:
            data = json.load(f, strict=False)

            to_return = []
            for i in data:
                # TODO_3: Fix UTC stuff
                time_creation = datetime.datetime.utcfromtimestamp(i["create_time"])
                if t is not None and time_creation not in t:
                    continue
                to_return.extend(self._parse_openai_conversation(i))

            df = pd.DataFrame(to_return)
            return df.set_index("date")
