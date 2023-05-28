from __future__ import annotations

import datetime
import dis
import hashlib
import os
from typing import Any, Callable

import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval


class Node_pandas(Node[pd.DataFrame]):
    def __init__(self) -> None:
        super().__init__()

    def filter(self, f: Callable[[pd.Series], bool]) -> Node_pandas:
        return Node_pandas_filter(self, f)

    def operation(
        self,
        f: Callable[[pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]], pd.DataFrame],
    ) -> Node_pandas:
        return Node_pandas_operation(self, f)


class Node_pandas_generate(Node_pandas):
    """Really for debugging purposes I guess"""

    def __init__(self, df: pd.DataFrame) -> None:
        assert isinstance(df, pd.DataFrame)
        super().__init__()
        self.df = df

    def _get_children(self) -> list[Node]:
        return []

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

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


class Node_pandas_operation(Node_pandas):
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

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        instructions = list(dis.get_instructions(self.fn_operation))
        dis_output = "\n".join(
            [f"{i.offset} {i.opname} {i.argrepr}" for i in instructions]
        )
        return hashlib.md5((super()._hashstr() + str(dis_output)).encode()).hexdigest()

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        return self.fn_operation(n0)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(n0_out, t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync] | None:
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        if n0_out is None:
            return None
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out, t
        )


class Node_pandas_filter(Node_pandas_operation):
    def __init__(self, n0: Node_pandas, fn_filter: Callable[[pd.Series], bool]) -> None:
        assert callable(fn_filter), "operation_main must be callable"
        assert isinstance(n0, Node_pandas)
        super().__init__(n0, lambda df: df[df.apply(fn_filter, axis=1)])  # type: ignore


class Reader_csvs(Node_pandas):
    """Can be used to read a .csv or a directory of .csvs"""

    def __init__(self, path_dir: str) -> None:
        if not path_dir.endswith(".csv"):
            if not os.path.exists(path_dir):
                raise ValueError(f"{path_dir} does not exist")
        super().__init__()
        self.path_dir = path_dir

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        if self.path_dir.endswith(".csv"):
            return os.path.exists(self.path_dir)
        else:
            return (
                os.path.isdir(self.path_dir)
                and len([i for i in os.listdir(self.path_dir) if i.endswith(".csv")])
                > 0
            )

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        # Get files
        files_to_read = (
            [self.path_dir]
            if self.path_dir.endswith(".csv")
            else os.listdir(self.path_dir)
        )

        # Load them
        to_return: list = []
        for filename in files_to_read:
            if filename.endswith(".csv"):
                # Filter by date
                if not self.path_dir.endswith(".csv"):
                    filename_date = datetime.datetime.strptime(
                        filename.split("_")[-1], "%Y-%m-%d.csv"
                    )
                    if t is not None and not (t.start <= filename_date <= t.end):
                        continue
                # Read
                try:
                    to_return.append(pd.read_csv(os.path.join(self.path_dir, filename)))
                except pd.errors.ParserError:
                    print(f"Error reading {filename}")
        return pd.concat(to_return, axis=0)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


class Reader_csvs_datedsubfolders(Reader_csvs):
    def __init__(
        self,
        path_dir: str,
        criteria_to_select_file: Callable[[str], bool],
    ) -> None:
        super().__init__(path_dir)
        self.criteria_to_select_file = criteria_to_select_file

    def _hashstr(self) -> str:
        instructions = list(dis.get_instructions(self.criteria_to_select_file))
        dis_output = "\n".join(
            [f"{i.offset} {i.opname} {i.argrepr}" for i in instructions]
        )
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode() + dis_output.encode()
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
