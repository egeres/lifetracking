from __future__ import annotations

import datetime
import hashlib
import json
from functools import reduce
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.Node_cache import Node_cache
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import hash_method


class Node_segments(Node[Segments]):

    sub_type = Segments

    def __init__(self) -> None:
        super().__init__()

    def apply(self, fn: Callable[[Segments], Segments]) -> Node_segments:
        return Node_segments_operation(self, fn)  # type: ignore

    def assign_value_all(self, key: str, value: Any) -> Node_segments:
        """Assigns a value to all the Seg objects inside an instance of Segments"""

        # TODO_2": Since this actually calls Segments.__setitem__() maybe it should just
        # be Node_segments.__setitem__()

        def fn(segments: Segments) -> Segments:
            segments[key] = value
            return segments

        return self.apply(fn)

    # TODO_3
    # def filter(self):
    #     ...

    # TODO_3
    # def filter(self, fn: Callable[[pd.Series], bool]) -> Node_pandas:
    #     return Node_segment_filter(self, fn)

    # TODO_3
    # def clone():
    # ...

    def merge(
        self,
        time_to_mergue: datetime.timedelta,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ):
        """Merges Segs that are close to each other in time. So if we set
        `time_to_mergue_s` to be 1 minute, and we have two segments that are 30
        seconds apart, they will be merged."""

        return Node_segments_merge(self, time_to_mergue, custom_rule)

    def __add__(self, other: Node_segments) -> Node_segments:
        return Node_segments_add([self, other])

    def __sub__(self, other: Node_segments) -> Node_segments:
        return Node_segments_sub(self, [other])

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str | Path,
        hour_offset: float = 0.0,
        opacity: float = 1.0,
        tooltip: str | Callable[[Seg], str] | None = None,
        color: str | Callable[[Seg], str] | None = None,
        tooltip_shows_length: bool = False,
    ):
        if isinstance(path_filename, Path):
            path_filename = str(path_filename)
        assert isinstance(t, Time_interval) or t is None
        assert isinstance(path_filename, str)
        assert isinstance(hour_offset, (float, int))
        assert isinstance(opacity, float)
        assert tooltip is None or isinstance(tooltip, str) or callable(tooltip)
        assert color is None or isinstance(color, str) or callable(color)
        assert isinstance(tooltip_shows_length, bool)

        o = self.run(t)
        if o is None:
            print("🔺 Could not export to long calendar...")
            return
        assert o is not None
        o.export_to_longcalendar(
            path_filename=path_filename,
            hour_offset=hour_offset,
            opacity=opacity,
            tooltip=tooltip,
            color=color,
            tooltip_shows_length=tooltip_shows_length,
        )

    def plot_hours(
        self,
        t: Time_interval | None = None,
        yaxes: tuple[float, float] | None = None,
        smooth: int = 1,
        annotations: list | None = None,
        stackgroup: str | dict | None = None,
    ) -> go.Figure | None:
        assert t is None or isinstance(t, Time_interval)
        assert yaxes is None or isinstance(yaxes, tuple)
        assert isinstance(smooth, int)
        assert smooth > 0
        assert isinstance(annotations, list) or annotations is None

        o = self.run(t)
        if o is None:
            return None
        assert isinstance(o, Segments)
        return o.plot_hours(
            t=t,
            yaxes=yaxes,
            smooth=smooth,
            annotations=annotations,
            title=self.name,
            stackgroup=stackgroup,
        )

    # TODO_4: Segments: plot_count_by_day
    def plot_count_by_day(self):
        raise NotImplementedError


class Node_cache_segments(Node_cache[Segments], Node_segments):
    pass


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
        time_to_mergue: datetime.timedelta,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ) -> None:
        assert isinstance(time_to_mergue, datetime.timedelta)
        assert isinstance(n0, Node_segments)
        assert custom_rule is None or callable(
            custom_rule
        ), "operation_main must be callable"

        super().__init__(
            n0,
            lambda x: Segments.merge(x, time_to_mergue, custom_rule),  # type: ignore
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

        # How should the system react when there are None values to subtract?
        n_out = [x for x in n_out if x is not None]
        if len(n_out) == 0:
            return None

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
        # How should the system react when there are None values to subtract?
        value_out = [x for x in value_out if x is not None]
        if len(value_out) == 0:
            return None
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
        time_to_split_in_mins: float = 5.0,
        min_count: int = 1,
        # TODO: Next line is unused
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        # assert isinstance(n0, Node_pandas) TODO: Merge with geopandas or something
        assert isinstance(min_count, int)
        super().__init__()
        self.n0 = n0
        self.time_to_split_in_mins = time_to_split_in_mins
        self.min_count = min_count

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
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
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)

        # Sort index of df
        df = df.sort_index()

        # Pre
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        count = 1
        if len(df) == 0:
            return Segments(to_return)
        start = df.index[0]
        end = df.index[0]
        it = df.index[1:]

        # Segmentizing
        for i in it:
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
        time_to_split_in_mins: float = 5.0,
        min_count: int = 1,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        # assert isinstance(n0, Node_pandas) TODO: Merge with geopandas or something
        assert isinstance(min_count, int)
        super().__init__()
        self.n0 = n0
        self.config = config
        self.time_to_split_in_mins = time_to_split_in_mins
        self.min_count = min_count

        if segment_metadata is not None:
            raise NotImplementedError

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.config)
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

        # Filtering
        df = df[df[column_to_process].isin(values_of_interest)]
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)

        if df.shape[0] == 0:
            return Segments([])

        # Pre
        to_return = []
        time_delta = pd.Timedelta(minutes=self.time_to_split_in_mins)
        count = 1
        start = df.index[0]
        end = df.index[0]
        it = df.index[1:]

        # Segmentizing
        for i in it:
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
        name_column_duration: str,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        assert isinstance(name_column_duration, str)
        assert segment_metadata is None or callable(segment_metadata)
        super().__init__()
        self.n0 = n0
        self.name_column_duration = name_column_duration
        self.segment_metadata = segment_metadata

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.name_column_duration)
                # TODO: Missing self.segment_metadata
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

        # Stuff
        df: pd.DataFrame = n0  # type: ignore
        if df.shape[0] == 0:
            return Segments([])
        assert isinstance(df.index, pd.DatetimeIndex)
        iterable = df.iterrows()

        # TODO: Could this be removed by always ensuring an ordering in the dates?
        if len(df) > 1 and (
            df.iloc[0][self.name_column_duration]
            > df.iloc[1][self.name_column_duration]
        ):
            # iterable = reversed(list(iterable))
            iterable = df[::-1].iterrows()

        # Segmentizing
        to_return = []
        for n, i in iterable:
            # Time delta is calculated
            time_delta = i[self.name_column_duration]
            if isinstance(time_delta, (float, int, np.number)):
                time_delta = datetime.timedelta(seconds=float(time_delta))
            else:
                raise NotImplementedError
            assert isinstance(time_delta, datetime.timedelta)

            # Seg is created
            to_return.append(
                Seg(
                    n,
                    n + time_delta,
                    None if self.segment_metadata is None else self.segment_metadata(i),
                )
            )
        return Segments(to_return)


# TODO: Probs should refactor w/ the above n stuff
# TEST
class Node_segmentize_pandas_startend(Node_1child, Node_segments):
    """Our df has a start and end column, we want to segmentize it creating a
    Segments with Seg objects according to these"""

    def __init__(
        self,
        n0: Node_pandas,
        name_column_end: str,
        segment_metadata: Callable[[pd.Series], dict[str, Any]] | None = None,
    ) -> None:
        assert isinstance(name_column_end, str)
        assert segment_metadata is None or callable(segment_metadata)
        super().__init__()
        self.n0 = n0
        self.name_column_end = name_column_end
        self.segment_metadata = segment_metadata

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.name_column_end)
                # TODO: Missing self.segment_metadata
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
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)

        # Pre
        if df[self.name_column_end].dtype == "object":
            df[self.name_column_end] = pd.to_datetime(df[self.name_column_end])
        # TODO: Measure if there is an actual performance increase on this,
        # basically, the optimization says "if I don't have a method that needs
        # the full row into, only iterate over the column of interest", is it
        # worth it, or negligible?
        if self.segment_metadata is None:
            iterable = df[self.name_column_end].iterrows()
        else:
            iterable = df.iterrows()
        # TODO: Could this be removed by always ensuring an ordering in the dates?
        if len(df) > 1 and df.index[0] > df.index[1]:
            iterable = df[::-1].iterrows()

        # Segmentizing
        to_return = []
        for n, i in iterable:
            to_return.append(
                Seg(
                    n,
                    i[self.name_column_end],
                    None if self.segment_metadata is None else self.segment_metadata(i),
                )
            )
        return Segments(to_return)


class Reader_segmentsinjson(Node_0child, Node_segments):
    """Gets a folder with json files and creates a Segments object with them. The
    structure must be as follows:

    [
        {"start":2023-01-01T00:00:00, "end":2023-01-01T00:00:00, "value":{}},
        ...
    ]
    """

    def __init__(
        self,
        path_dir: str | Path,
    ):
        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        self.path_dir: Path = path_dir

    def _available(self) -> bool:
        return (
            self.path_dir.exists()
            and self.path_dir.is_dir()
            and len(list(self.path_dir.glob("*"))) > 0
        )

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _operation(self, t: Time_interval | None = None) -> Segments:
        assert t is None or isinstance(t, Time_interval)

        files = self.path_dir.glob("*")

        if t is not None:
            files = [
                f for f in files if datetime.datetime.strptime(f.stem, "%Y-%m-%d") in t
            ]

        to_return = []
        for f in files:
            with f.open() as f:
                data = json.load(f)
            for i in data:
                to_return.append(
                    Seg(
                        datetime.datetime.strptime(i["start"], "%Y-%m-%d %H:%M:%S"),
                        datetime.datetime.strptime(i["end"], "%Y-%m-%d %H:%M:%S"),
                        i.get("value"),
                    )
                )

        return Segments(to_return)
