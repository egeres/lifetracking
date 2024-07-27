from __future__ import annotations

import copy
import hashlib
import inspect
import json
from datetime import datetime, timedelta

# from bisect import insort  # TODO_2: Python 3.11 because of key=
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, overload

import numpy as np
import pandas as pd
from typing_extensions import Self

from lifetracking.datatypes.Seg import Seg
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.plots.graphs import (
    graph_annotate_annotations,
    graph_annotate_title,
    graph_annotate_today,
    graph_udate_layout,
)
from lifetracking.utils import _lc_export_prepare_dir

if TYPE_CHECKING:
    import plotly.graph_objects as go


class Segments:
    def __init__(self, content: list[Seg]) -> None:
        assert all(isinstance(seg, Seg) for seg in content)
        assert all(
            seg.start <= seg.end for seg in content
        ), "Segments must be ordered in time"

        # self.content = content # TODO : sort shouldn't actually be neccesary :[
        self.content: list[Seg] = sorted(content, key=lambda x: x.start)

    def __copy__(self) -> Segments:
        return Segments([copy.copy(seg) for seg in self.content])

    def __sub__(self, other: Segments) -> Segments:
        # Self and other must be sorted!
        assert all(
            seg.start <= seg.end for seg in self.content
        ), "Segments must be ordered in time"
        assert all(
            seg.start <= seg.end for seg in other.content
        ), "Segments must be ordered in time"

        content = self.content
        content_index = 0
        while content_index < len(content):
            s: Seg = content[content_index]
            for t in other.content:
                if t.overlaps(s):  # There is an overlap
                    # t completely covers s
                    if t.start <= s.start and t.end >= s.end:
                        content.pop(content_index)
                        content_index -= 1
                        break

                    # t covers the right side
                    if t.end >= s.end:
                        s.end = t.start
                        break

                    # t covers the left side
                    if t.start <= s.start:
                        s.start = t.end
                        break

                    content.append(Seg(t.end, s.end, s.value))
                    content = sorted(content, key=lambda x: x.start)
                    # TODO: Benchmark the difference in speed, but needs
                    # python >= 3.10
                    # insort(content, Seg(t.end, s.end, s.value), key=lambda
                    # x: x.start)
                    s.end = t.start
                    break
            content_index += 1
        return Segments(sorted(content))

    def _hashstr(self) -> str:
        return hashlib.md5(self.__class__.__name__.encode()).hexdigest()

    @overload
    def __getitem__(self, index: int) -> Seg: ...  # pragma: no cover

    @overload
    def __getitem__(self, index: Time_interval) -> Segments: ...  # pragma: no cover

    def __getitem__(self, index: Time_interval | int) -> Segments | Seg:
        if isinstance(index, int):
            return self.content[index]
        if isinstance(index, Time_interval):
            return Segments(
                [
                    seg
                    for seg in self.content
                    if index.start <= seg.start and seg.end <= index.end
                ]
            )
        msg = "index must be Time_interval or int"
        raise TypeError(msg)

    def __setitem__(self, property_name: str, value: Any) -> Self:
        """Sets a property of all the segments"""
        # TODO_3: Refactor to a pandas dataframe and make this a columns assignment

        for seg in self.content:
            seg[property_name] = value
        return self

    def __iter__(self):
        return iter(self.content)

    def __len__(self) -> int:
        return len(self.content)

    def min(self) -> datetime:
        # TODO_3: Refactor, self.content is a pandas dataframe or an index of
        # datetimes instead of a list we have to iterate
        return min(seg.start for seg in self.content)

    def max(self) -> datetime:
        # TODO_3: Refactor, self.content is a pandas dataframe or an index of
        # datetimes instead of a list we have to iterate
        return max(seg.end for seg in self.content)

    def __add__(self, other: Segments) -> Segments:
        # FIX: (TZ) Pls, the thing with the timezone discrepancies!! 🥺
        if len(self.content) > 0 and len(other.content) > 0:
            if (
                other.content[0].start.tzinfo is None
                and self.content[0].start.tzinfo is not None
            ):
                tz = self.content[0].start.tzinfo
                for i in other.content:
                    i.start = i.start.replace(tzinfo=tz)
                    i.end = i.end.replace(tzinfo=tz)

            if (
                self.content[0].start.tzinfo is None
                and other.content[0].start.tzinfo is not None
            ):
                tz = other.content[0].start.tzinfo
                for i in self.content:
                    i.start = i.start.replace(tzinfo=tz)
                    i.end = i.end.replace(tzinfo=tz)

        # TODO: After coverage has been extended, test if the 'sorted' could be removed

        # TODO: Maybe the sorted thing can be skipped if we add a property "sorted
        # segments" which is a boolean and we set it to True after sorting, if we add
        # together 2 Segments objects, and both are sorted, and, they don't overlap we
        # can do a simple concatenation
        return Segments(sorted(other.content + self.content))

    def _export_to_longcalendar_edit_dict(
        self,
        x: dict,
        s: Seg,
        tooltip,
        tooltip_shows_length: bool = False,
        color: str | Callable[[Seg], str] | None = None,
        opacity: float | Callable[[Seg], float] = 1.0,
    ) -> None:
        # Tooltip
        if tooltip is not None:
            if callable(tooltip):
                x["tooltip"] = tooltip(s)
            else:
                x["tooltip"] = str(s[tooltip])

        # Tooltip length
        # Jesus christ bro 🤦🏻‍♂️, just write another if inside and stop doing
        # parkour, you're not raymond belle 🙄
        if tooltip_shows_length:
            if s.length_h() < 0.1:
                x["tooltip"] = x.get("tooltip", "") + f" ({round(s.length_m(), 1)}m)"
            else:
                x["tooltip"] = x.get("tooltip", "") + f" ({round(s.length_h(), 1)}h)"
            x["tooltip"] = x["tooltip"].strip()

        # Color & opacity
        if callable(color):
            x["color"] = color(s)
        if callable(opacity):
            x["opacity"] = opacity(s)

    def export_to_longcalendar(
        self,
        path_filename: str | Path,
        hour_offset: float = 0.0,
        color: str | Callable[[Seg], str] | None = None,
        opacity: float | Callable[[Seg], float] = 1.0,
        tooltip: str | Callable[[Seg], str] | None = None,
        tooltip_shows_length: bool = False,
    ) -> None:
        """Long calendar is a custom application of mine that I use to visualize
        my data."""

        # Assertions
        if isinstance(path_filename, str):
            path_filename = Path(path_filename)
        assert isinstance(path_filename, Path)
        if path_filename.suffix != ".json":
            msg = "path_filename must end with .json"
            raise ValueError(msg)
        assert path_filename.name != "config.json"

        # Assertion of color, opacity and tooltip
        assert color is None or isinstance(color, str) or callable(color)
        assert (
            color is None
            or isinstance(color, str)
            or len(inspect.signature(color).parameters) == 1
        )
        assert isinstance(opacity, float) or callable(opacity)
        assert (
            opacity is None
            or isinstance(opacity, float)
            or len(inspect.signature(opacity).parameters) == 1
        )

        # Other assertions
        assert isinstance(hour_offset, (float, int))
        assert isinstance(tooltip_shows_length, bool)
        assert tooltip is None or isinstance(tooltip, str) or callable(tooltip)
        assert (
            tooltip is None
            or isinstance(tooltip, str)
            or len(inspect.signature(tooltip).parameters) == 1
        )

        # Dir setup
        _lc_export_prepare_dir(path_filename)

        # Changes at config.json
        if isinstance(color, str) or isinstance(opacity, float):
            path_fil_config = path_filename.parent / "config.json"
            with path_fil_config.open() as f:
                data = json.load(f)
            assert isinstance(data, dict)
            key_name = path_filename.name.split(".")[0]
            if key_name not in data["data"]:
                data["data"][key_name] = {}

            # We write color and opacity
            if isinstance(color, str):
                data["data"][key_name]["color"] = str(color)
            if isinstance(opacity, float) and opacity != 1.0:
                data["data"][key_name]["opacity"] = opacity

            with path_fil_config.open("w") as f:
                json.dump(data, f, indent=4, default=str)

        # Export itself
        to_export = []
        for seg in self.content:
            seg += timedelta(hours=hour_offset)
            splitted_segs = seg.split_into_segments_per_day()
            for s in splitted_segs:
                to_export.append(
                    {
                        "start": s.start.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end": s.end.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                )
                self._export_to_longcalendar_edit_dict(
                    to_export[-1],
                    s,
                    tooltip,
                    tooltip_shows_length,
                    color,
                    opacity,
                )
        with path_filename.open("w") as f:
            json.dump(to_export, f, indent=4, default=str)

    @staticmethod
    def merge(
        segs: Segments,
        time_to_mergue: timedelta,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ) -> Segments:
        """Merges segments that are close to each other in time. So if we set
        `time_to_mergue_s` to be 1 minute, and we have two segments that are 30
        seconds apart, they will be merged."""

        assert isinstance(segs, Segments)
        assert isinstance(time_to_mergue, timedelta)

        if len(segs) < 2:
            return segs

        to_return = []

        if custom_rule is None:
            to_return.append(segs.content[0])
            for seg in segs.content[1:]:
                if seg.start - to_return[-1].end < time_to_mergue:
                    to_return[-1].end = seg.end
                    # TODO: Maybe adds a counter with "operation_merge_count"?
                else:
                    to_return.append(seg)
        else:
            to_return.append(segs.content[0])
            for seg in segs.content[1:]:
                if seg.start - to_return[-1].end < time_to_mergue and custom_rule(
                    to_return[-1], seg
                ):
                    to_return[-1].end = max(seg.end, to_return[-1].end)
                    # TODO: Maybe adds a counter with "operation_merge_count"?
                else:
                    to_return.append(seg)

        return Segments(to_return)

    def remove(self, condition: Callable[[Seg], bool]) -> Segments:
        """Removes segments that satisfy a condition."""
        return Segments([seg for seg in self.content if not condition(seg)])

    def remove_if_shorter_than(self, interval: timedelta) -> Segments:
        """Removes segments that are shorter than a given time."""

        assert isinstance(interval, timedelta)
        return self.remove(lambda seg: seg.length() < interval)

    def set_property(self, property_name: str, value: Any) -> Segments:
        """Sets a property of all the children Seg"""
        for seg in self.content:
            seg[property_name] = value
        return self

    def _plot_hours_generatedata(
        self,
        t: Time_interval | None,
        data: list[Seg],
        smooth: int,
    ) -> list[float]:
        # a, b calculation
        if t is None:
            a, b = self.min(), self.max()
        else:
            a, b = t.start, t.end

        # TODO_3: (TZ) Do something about tz infos pls 🥺
        # if len(self.content) > 0:
        #     if a.tzinfo is None:
        #         a = a.replace(tzinfo=self.content[0].start.tzinfo)
        #         b = b.replace(tzinfo=self.content[0].start.tzinfo)
        if len(data) > 0 and a.tzinfo is None:
            a = a.replace(tzinfo=data[0].start.tzinfo)
            b = b.replace(tzinfo=data[0].start.tzinfo)

        # Data itself
        c: list[float] = [0] * ((b - a).days + 1)
        # for s in self.content:
        for s in data:
            o = s.split_into_segments_per_day()
            for j in o:
                index = (j.start - a).days
                if index < 0 or index >= len(c):
                    continue
                c[index] += j.length_h()
        if smooth > 1:
            c = np.convolve(c, np.ones(smooth) / smooth, mode="same").tolist()
        return c

    def _plot_hours_generatedata_with_stackgroup(
        self,
        t: Time_interval | None,
        smooth: int,
        stackgroup: str,
    ) -> dict[str, list[float]]:
        """Retrieves something like:
        {
            "study": [0.1, 0.1, 1.4, 0.9...],
            "personal": [0.1, 0.1, 1.4, 0.9...],
            ...
        }
        """

        # Uniques
        unique_categories = set()
        for s in self.content:
            unique_categories.add(s[stackgroup])

        # Return
        to_return = {}
        for category in unique_categories:
            to_return[category] = self._plot_hours_generatedata(
                t,
                [s for s in self.content if s[stackgroup] == category],
                smooth,
            )
        return to_return

    def plot_hours(
        self,
        t: Time_interval | None = None,
        yaxes: tuple[float, float] | None = (0, 24),
        smooth: int = 1,
        annotations: list | None = None,
        title: str | None = None,
        stackgroup: str | dict | None = None,
    ) -> go.Figure:
        """Plots the hours of the segments in the interval t

        Parameters:
        stackgroup
        It can be a string or a dict with the key "label" and optionally "colors"
        """
        import plotly.express as px
        import plotly.graph_objects as go

        assert t is None or isinstance(t, Time_interval)
        assert isinstance(yaxes, tuple) or yaxes is None
        assert isinstance(smooth, int)
        assert smooth > 0
        if isinstance(stackgroup, dict):
            assert "label" in stackgroup
        assert title is None or isinstance(title, str)

        # Pre
        fig_min, fig_max = (0, 24) if yaxes is None else yaxes

        # Plot
        if stackgroup is None:
            c = self._plot_hours_generatedata(t, self.content, smooth)
            fig_index = (
                t.to_datetimeindex()
                if t is not None
                else pd.date_range(self.min(), self.max(), freq="D")
            )
            fig = px.line(x=fig_index, y=c)

        else:
            stackgroupname: str = (
                stackgroup if isinstance(stackgroup, str) else stackgroup["label"]
            )
            c = self._plot_hours_generatedata_with_stackgroup(t, smooth, stackgroupname)
            fig_index = (
                t.to_datetimeindex()
                if t is not None
                else pd.date_range(self.min(), self.max(), freq="D")
            )
            df = pd.DataFrame(c, index=fig_index)
            fig = go.Figure()
            for col in df.columns:
                # The user should be able to specify colors/etc for these plots...
                extra_args = {}
                if isinstance(stackgroup, dict) and col in stackgroup.get("colors", {}):
                    extra_args["marker_color"] = stackgroup["colors"][col]

                # Fig
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[col],
                        stackgroup="one",
                        fill="tonexty",
                        name=col,
                        **extra_args,
                    )
                )
            fig.update_layout(hovermode="x unified")

        # Fig format
        graph_udate_layout(fig, t)
        fig.update_yaxes(title_text="", range=yaxes)
        fig.update_xaxes(title_text="")
        graph_annotate_title(fig, title)
        if t is not None:
            graph_annotate_today(fig, t, (fig_min, fig_max))
            graph_annotate_annotations(fig, t, annotations, (fig_min, fig_max))
        return fig

    def increment_hours(
        self,
        hours: float,
    ) -> Self:
        assert isinstance(hours, (float, int))

        for seg in self.content:
            seg.start += timedelta(hours=hours)
            seg.end += timedelta(hours=hours)

        return self
