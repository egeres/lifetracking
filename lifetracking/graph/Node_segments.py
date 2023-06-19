from __future__ import annotations

import datetime
import hashlib
from functools import reduce
from typing import Any, Callable

import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import hash_method


class Node_segments(Node[Segments]):
    def __init__(self) -> None:
        super().__init__()

    def operation(
        self,
        fn: Callable[[Segments], Segments],
    ) -> Node_segments:
        return Node_segments_operation(self, fn)  # type: ignore

    def merge(
        self,
        time_to_mergue_s: float,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ):
        """Merges Segs that are close to each other in time. So if we set
        `time_to_mergue_s` to be 1 minute, and we have two segments that are 30
        seconds apart, they will be merged."""

        return Node_segments_merge(self, time_to_mergue_s, custom_rule)

    def __add__(self, other: Node_segments) -> Node_segments:
        return Node_segments_add([self, other])

    def __sub__(self, other: Node_segments) -> Node_segments:
        return Node_segments_sub(self, [other])

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        hour_offset: float = 0.0,
        opacity: float = 1.0,
        tooltip: str | Callable[[Seg], str] | None = None,
        color: str | Callable[[Seg], str] | None = None,
        tooltip_shows_length: bool = False,
    ):
        assert isinstance(t, Time_interval) or t is None
        assert isinstance(path_filename, str)
        assert isinstance(hour_offset, (float, int))
        assert isinstance(opacity, float)
        assert tooltip is None or isinstance(tooltip, str) or callable(tooltip)
        assert color is None or isinstance(color, str) or callable(color)
        assert isinstance(tooltip_shows_length, bool)

        o = self.run(t)
        assert o is not None
        o.export_to_longcalendar(
            path_filename=path_filename,
            hour_offset=hour_offset,
            opacity=opacity,
            tooltip=tooltip,
            color=color,
            tooltip_shows_length=tooltip_shows_length,
        )


class Node_segments_operation(Node_1child, Node_segments):
    def __init__(
        self,
        n0: Node_segments,
        fn: Callable[[Segments | PrefectFuture[Segments, Sync]], Segments],
    ) -> None:
        assert isinstance(n0, Node_segments)
        assert callable(fn), "operation_main must be callable"
        super().__init__()
        self.n0 = n0
        self.fn = fn

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + hash_method(self.fn)).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        n0: Segments | PrefectFuture[Segments, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)
        o = self.fn(n0)
        assert isinstance(o, Segments), "The fn must return a Segments object!"
        return o


class Node_segments_merge(Node_segments_operation):
    def __init__(
        self,
        n0: Node_segments,
        time_to_mergue_s: float,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ) -> None:
        assert isinstance(time_to_mergue_s, (float, int))
        assert isinstance(n0, Node_segments)
        assert custom_rule is None or callable(
            custom_rule
        ), "operation_main must be callable"

        super().__init__(
            n0,
            lambda x: Segments.merge(x, time_to_mergue_s, custom_rule),  # type: ignore
        )


class Node_segments_generate(Node_0child, Node_segments):
    def __init__(self, value: Segments) -> None:
        super().__init__()
        self.value: Segments = value

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


class Node_segments_add(Node_segments):
    def __init__(self, value: list[Node_segments]) -> None:
        super().__init__()
        self.value: list[Node_segments] = value

    def _get_children(self) -> list[Node_segments]:
        return self.value

    def _hashstr(self):
        return hashlib.md5(
            (super()._hashstr() + "".join([v._hashstr() for v in self.value])).encode()
        ).hexdigest()

    def _operation(
        self,
        value: list[Segments | PrefectFuture[Segments, Sync]],
        t: Time_interval | None = None,
    ) -> Segments:
        # TODO: t is not used
        return reduce(
            lambda x, y: x + y,
            [x for x in value if x is not None],  # type: ignore
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync]:
        n_out = [
            self._get_value_from_context_or_makegraph(n, t, context) for n in self.value
        ]
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n_out,
            t=t,
        )

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        n_out = [self._get_value_from_context_or_run(n, t, context) for n in self.value]
        return self._operation(
            n_out,
            t=t,
        )

    def __add__(self, other: Node_segments) -> Node_segments:
        """This node keeps eating other nodes if you add them"""
        self.value.append(other)
        return self


class Node_segments_sub(Node_segments):
    def __init__(self, n0: Node_segments, value: list[Node_segments]) -> None:
        super().__init__()
        self.n0: Node_segments = n0
        self.value: list[Node_segments] = value

    def _get_children(self) -> list[Node_segments]:
        return self.value

    def _hashstr(self):
        return hashlib.md5(
            (super()._hashstr() + "".join([v._hashstr() for v in self.value])).encode()
        ).hexdigest()

    def _operation(
        self,
        n0: Segments | PrefectFuture[Segments, Sync],
        value: list[Segments | PrefectFuture[Segments, Sync]],
        t: Time_interval | None = None,
    ) -> Segments:
        # TODO: t is not used
        for i in value:
            n0 = n0 - i
        return n0

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync]:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        value_out = [
            self._get_value_from_context_or_run(n, t, context) for n in self.value
        ]
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out, value_out, t=t
        )

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        # TODO: Add these in all nodes with a child?
        if n0_out is None:
            return None
        value_out = [
            self._get_value_from_context_or_run(n, t, context) for n in self.value
        ]
        return self._operation(n0_out, value_out, t=t)


# TODO: Re... remove?
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


class Node_segmentize_pandas_by_density(Node_1child, Node_segments):
    """We have a df with events tagged with a time column and we want to create
    segments if "many of these events haven closely" in time"""

    def __init__(
        self,
        n0: Node_pandas,
        name_column_time: str,
        time_to_split_in_mins: float = 5.0,
        min_count: int = 1,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        # assert isinstance(n0, Node_pandas) TODO: Merge with geopandas or something
        assert isinstance(name_column_time, str)
        assert isinstance(min_count, int)
        super().__init__()
        self.n0 = n0
        self.name_column_time = name_column_time
        self.time_to_split_in_mins = time_to_split_in_mins
        self.min_count = min_count

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + self.name_column_time
                + str(self.time_to_split_in_mins)
                + str(self.min_count)
            ).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)

        # Variable loading
        df: pd.DataFrame = n0  # type: ignore
        if df[self.name_column_time].dtype == "object":
            df[self.name_column_time] = pd.to_datetime(
                df[self.name_column_time],
                # format="ISO8601",
                # format="mixed",
            )

        # Pre
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        count = 1
        start = df[self.name_column_time].iloc[0]
        end = df[self.name_column_time].iloc[0]

        # Segmentizing
        for i in df[self.name_column_time].iloc[1:]:
            current_time = i
            if (current_time - end) < time_delta:
                count += 1
                end = current_time
            else:
                if count > self.min_count:
                    to_return.append(Seg(start, end, {"operation_count": count}))
                count = 1
                start = current_time
                end = current_time
        if count > self.min_count:
            to_return.append(Seg(start, end, {"operation_count": count}))

        return Segments(to_return)


class Node_segmentize_pandas(Node_1child, Node_segments):
    # DOCS: Add docstring

    def __init__(
        self,
        n0: Node_pandas,
        config: Any,
        name_column_time: str,
        time_to_split_in_mins: float = 5.0,
        min_count: int = 1,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        # assert isinstance(n0, Node_pandas) TODO: Merge with geopandas or something
        assert isinstance(name_column_time, str)
        assert isinstance(min_count, int)
        super().__init__()
        self.n0 = n0
        self.config = config
        self.name_column_time = name_column_time
        self.time_to_split_in_mins = time_to_split_in_mins
        self.min_count = min_count

        if segment_metadata is not None:
            raise NotImplementedError

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.config)
                + self.name_column_time
                + str(self.time_to_split_in_mins)
                + str(self.min_count)
            ).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

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
        if df[self.name_column_time].dtype == "object":
            df[self.name_column_time] = pd.to_datetime(
                df[self.name_column_time],
                format="ISO8601",
                # format="mixed",
            )

        # Filtering
        df = df[df[column_to_process].isin(values_of_interest)]

        # Pre
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        count = 1
        start = df[self.name_column_time].iloc[0]
        end = df[self.name_column_time].iloc[0]

        # Segmentizing
        for i in df[self.name_column_time].iloc[1:]:
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


class Node_segmentize_pandas_duration(Node_1child, Node_segments):
    """Our df has a start and a duration column. We want to generate a Segments
    object with various Seg instances corresponding to this"""

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

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                # TODO: Missing self.segment_metadata
                super()._hashstr()
                + str(self.name_column_date)
                + str(self.name_column_duration)
            ).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        # TODO: t is doing nothing? :[
        assert t is None or isinstance(t, Time_interval)

        df: pd.DataFrame = n0  # type: ignore

        # Small preprocessing
        if df[self.name_column_date].dtype == "object":
            df[self.name_column_date] = pd.to_datetime(
                df[self.name_column_date],
                format="ISO8601",
                # format="mixed",
            )

        # TODO: Is iterrows really needed? Add tests first then remove if possible
        # TODO: Measure if there is an actual performance increase on this
        if self.segment_metadata is None:
            iterable = df[[self.name_column_date, self.name_column_duration]].iterrows()
        else:
            iterable = df.iterrows()
        # TODO: Could this be removed by always ensuring an ordering in the dates?
        if len(df) > 1:
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


# TODO: Probs should refactor w/ the above n stuff
class Node_segmentize_pandas_startend(Node_1child, Node_segments):
    """Our df has a start and end column, we want to segmentize it creating a
    Segments with Seg objects according to these"""

    def __init__(
        self,
        n0: Node_pandas,
        name_column_start: str,
        name_column_end: str,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        assert isinstance(name_column_start, str)
        assert isinstance(name_column_end, str)
        assert segment_metadata is None or callable(segment_metadata)
        super().__init__()
        self.n0 = n0
        self.name_column_start = name_column_start
        self.name_column_end = name_column_end
        self.segment_metadata = segment_metadata

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                # TODO: Missing self.segment_metadata
                super()._hashstr()
                + str(self.name_column_start)
                + str(self.name_column_end)
            ).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        t: Time_interval | None = None,
    ) -> Segments:
        assert t is None or isinstance(t, Time_interval)

        df: pd.DataFrame = n0  # type: ignore

        # Small preprocessing
        if df[self.name_column_start].dtype == "object":
            df[self.name_column_start] = pd.to_datetime(
                df[self.name_column_start],
                # format="ISO8601",
                # format="mixed",
            )
        if df[self.name_column_end].dtype == "object":
            df[self.name_column_end] = pd.to_datetime(df[self.name_column_end])

        # TODO: Measure if there is an actual performance increase on this
        if self.segment_metadata is None:
            iterable = df[[self.name_column_start, self.name_column_end]].iterrows()
        else:
            iterable = df.iterrows()
        # TODO: Could this be removed by always ensuring an ordering in the dates?
        if len(df) > 0:
            if df.iloc[0][self.name_column_start] > df.iloc[1][self.name_column_start]:
                iterable = reversed(list(iterable))

        # Segmentizing
        to_return = []
        for _, i in iterable:
            to_return.append(
                Seg(
                    i[self.name_column_start],
                    i[self.name_column_end],
                    None if self.segment_metadata is None else self.segment_metadata(i),
                )
            )
        return Segments(to_return)
