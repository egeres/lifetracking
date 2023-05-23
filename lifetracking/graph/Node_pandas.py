from __future__ import annotations

import datetime
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

    def filter(self, f: Callable[[pd.DataFrame], pd.DataFrame]) -> Node_pandas:
        return Node_pandas_filter(self, f)


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


class Node_pandas_filter(Node_pandas):
    def __init__(self, n0: Node_pandas, fn_filter) -> None:
        assert isinstance(n0, Node_pandas)
        super().__init__()
        self.n0 = n0
        self.fn_filter = fn_filter

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.fn_filter)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return self.n0.available

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        # return self.fn_filter(self.n0[t])

        # if self.fn_filter is true, then we keep those rows
        return n0[n0.apply(self.fn_filter, axis=1)]

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


class Reader_csvs(Node_pandas):
    def __init__(self, path_dir: str) -> None:
        if not os.path.isdir(path_dir):
            raise ValueError(f"{path_dir} is not a directory")
        super().__init__()
        self.path_dir = path_dir

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return os.path.isdir(self.path_dir)

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)
        to_return: list = []
        for filename in os.listdir(self.path_dir):
            if filename.endswith(".csv"):
                filename_date = datetime.datetime.strptime(
                    filename.split("_")[-1], "%Y-%m-%d.csv"
                )
                if t is not None and not (t.start <= filename_date <= t.end):
                    continue
                try:
                    to_return.append(
                        pd.read_csv(
                            os.path.join(
                                self.path_dir,
                                filename,
                            )
                        )
                    )
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
