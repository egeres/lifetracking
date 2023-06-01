from __future__ import annotations

import datetime
import dis
import hashlib
from typing import Any, Callable

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

    def operation(
        self,
        f: Callable[[Segments | PrefectFuture[Segments, Sync]], Segments],
    ) -> Node_segments:
        return Node_segments_operation(self, f)

    def merge(
        self,
        time_to_mergue_s: float,
        # TODO: Hehe, custom_rule would be nice
        custom_rule: None | Callable[[dict], bool] = None,
    ):
        # return Node_segments_operation(
        #     self,
        #     lambda x: Segments.merge(x, time_to_mergue_s),
        # )
        return Node_segments_merge(self, time_to_mergue_s)


class Node_segments_operation(Node_segments):
    def __init__(
        self,
        n0: Node_segments,
        fn_operation: Callable[[Segments | PrefectFuture[Segments, Sync]], Segments],
    ) -> None:
        assert isinstance(n0, Node_segments)
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
        n0: Segments | PrefectFuture[Segments, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)
        return self.fn_operation(n0)

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


class Node_segments_merge(Node_segments_operation):
    def __init__(self, n0: Node_segments, time_to_mergue_s: float) -> None:
        # assert callable(fn_filter), "operation_main must be callable"
        assert isinstance(n0, Node_segments)
        super().__init__(
            n0,
            lambda x: Segments.merge(x, time_to_mergue_s),
        )  # type: ignore


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
        min_count: int = 1,
    ) -> None:
        # assert isinstance(n0, Node_pandas) TODO: Merge with geopandas or something
        assert isinstance(time_column_name, str)
        assert isinstance(min_count, int)
        super().__init__()
        self.n0 = n0
        self.config = config
        self.time_column_name = time_column_name
        self.time_to_split_in_mins = time_to_split_in_mins
        self.min_count = min_count

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.config)
                + str(self.time_to_split_in_mins)
                + str(self.min_count)
            ).encode()
        ).hexdigest()

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
        if df[self.time_column_name].dtype == "object":
            df[self.time_column_name] = pd.to_datetime(
                df[self.time_column_name],
                format="ISO8601",
                # format="mixed",
            )

        # Filtering
        df = df[df[column_to_process].isin(values_of_interest)]

        # Pre
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        count = 1
        start = df[self.time_column_name].iloc[0]
        end = df[self.time_column_name].iloc[0]

        # Segmentizing
        for i in df[self.time_column_name].iloc[1:]:
            current_time = i
            if (current_time - end) < time_delta:
                count += 1
                end = current_time
            else:
                if count > self.min_count:
                    to_return.append(Seg(start, end))
                count = 1
                start = current_time
                end = current_time
        if count > self.min_count:
            to_return.append(Seg(start, end))

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


class Node_segmentize_pandas_duration(Node_segments):
    def __init__(
        self,
        n0: Node_pandas,
        name_column_date: str,
        name_column_duration: str,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        assert isinstance(name_column_date, str)
        assert isinstance(name_column_duration, str)
        assert segment_metadata is None or callable(segment_metadata)
        super().__init__()
        self.n0 = n0
        self.name_column_date = name_column_date
        self.name_column_duration = name_column_duration
        self.segment_metadata = segment_metadata

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.name_column_date)
                + str(self.name_column_duration)
            ).encode()
        ).hexdigest()

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)

        df: pd.DataFrame = n0  # type: ignore

        # Small preprocessing
        if df[self.name_column_date].dtype == "object":
            df[self.name_column_date] = pd.to_datetime(
                df[self.name_column_date],
                format="ISO8601",
                # format="mixed",
            )

        # TODO: Measure if there is an actual performance increase on this
        if self.segment_metadata is not None:
            iterable = df[[self.name_column_date, self.name_column_duration]].iterrows()
        else:
            iterable = df.iterrows()
        # TODO: Could this be removed by always ensuring an ordering in the dates?
        if len(df) > 0:
            if (
                df.iloc[0][self.name_column_duration]
                > df.iloc[1][self.name_column_duration]
            ):
                iterable = reversed(list(iterable))

        # Segmentizing
        to_return = []
        for _, i in iterable:
            d = i[self.name_column_date]
            to_return.append(
                Seg(
                    d,
                    d + datetime.timedelta(seconds=i[self.name_column_duration]),
                    None if self.segment_metadata is None else self.segment_metadata(i),
                )
            )
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
