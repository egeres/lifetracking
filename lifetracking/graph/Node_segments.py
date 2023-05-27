from __future__ import annotations

import datetime
import hashlib
from typing import Any

import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Node_segments(Node[Segments]):
    def __init__(self) -> None:
        super().__init__()


class Node_segments_generate(Node_segments):
    def __init__(self, value: Segments) -> None:
        super().__init__()
        self.value: Segments = value

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self):
        return hashlib.md5(
            (super()._hashstr() + self.value._hashstr()).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return True

    def _operation(self, t: Time_interval | None = None) -> Segments:
        assert t is None or isinstance(t, Time_interval)
        if t is None:
            return self.value
        else:
            return self.value[t]

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        return self._operation(t)


class Node_segments_from_pdDataframe(Node_segments):
    def __init__(self, n0: Node_pandas, config) -> None:
        assert isinstance(n0, Node_pandas)
        super().__init__()
        self.n0 = n0
        self.config = config

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self):
        return hashlib.md5((super()._hashstr() + str(self.config)).encode()).hexdigest()

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(
            n0_out,
            t,
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )


class Node_segmentize_pandas(Node_segments):
    def __init__(
        self,
        n0: Node_pandas,
        config: Any,
        time_column_name: str,
        time_to_split_in_mins: float = 5.0,
    ) -> None:
        # assert isinstance(n0, Node_pandas)
        super().__init__()
        self.n0 = n0
        self.config = config
        self.time_column_name = time_column_name
        self.time_to_split_in_mins = time_to_split_in_mins

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)

        # Variable loading
        column_to_process = self.config[0]
        values_of_interest = self.config[1]
        df: pd.DataFrame = n0  # type: ignore
        df[self.time_column_name] = pd.to_datetime(
            df[self.time_column_name], format="ISO8601"
        )

        # Filtering
        df = df[df[column_to_process].isin(values_of_interest)]

        # Segmentizing
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        if not df.empty:
            start_time = df.iloc[0][self.time_column_name]
            end_time = df.iloc[0][self.time_column_name]

            for i in range(1, len(df)):
                current_time = df.iloc[i][self.time_column_name]
                if (current_time - end_time) > time_delta:
                    to_return.append(Seg(start_time, end_time))
                    start_time = current_time
                end_time = current_time

            to_return.append(Seg(start_time, end_time))

        return Segments(to_return)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(n0_out, t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync] | None:
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        if n0_out is None:
            return None
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out, t
        )
