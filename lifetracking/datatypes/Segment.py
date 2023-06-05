from __future__ import annotations

import datetime
import hashlib
import inspect
import json
import os
from typing import Any, Callable, overload

from typing_extensions import Self

from lifetracking.datatypes.Seg import Seg
from lifetracking.graph.Time_interval import Time_interval


class Segments:
    def __init__(self, content: list[Seg]) -> None:
        assert all(isinstance(seg, Seg) for seg in content)
        assert all(
            seg.start <= seg.end for seg in content
        ), "Segments must be ordered in time"

        # self.content = content # TODO, sort shouldn't actually be neccesary :[
        self.content: list[Seg] = sorted(content, key=lambda x: x.start)

    def _hashstr(self) -> str:
        return hashlib.md5(self.__class__.__name__.encode()).hexdigest()

    @overload
    def __getitem__(self, index: int) -> Seg:
        ...  # pragma: no cover

    @overload
    def __getitem__(self, index: Time_interval) -> Segments:
        ...  # pragma: no cover

    def __getitem__(self, index: Time_interval | int) -> Segments | Seg:
        if isinstance(index, int):
            return self.content[index]
        elif isinstance(index, Time_interval):
            return Segments(
                [
                    seg
                    for seg in self.content
                    if index.start <= seg.start and seg.end <= index.end
                ]
            )
        else:
            raise TypeError("index must be Time_interval or int")

    def __setitem__(self, property_name: str, value: Any) -> Self:
        """Sets a property of all the segments"""
        for seg in self.content:
            seg[property_name] = value
        return self

    def __iter__(self):
        return iter(self.content)

    def __len__(self) -> int:
        return len(self.content)

    def min(self) -> datetime.datetime:
        return min(seg.start for seg in self.content)

    def max(self) -> datetime.datetime:
        return max(seg.end for seg in self.content)

    def __add__(self, other: Segments) -> Segments:
        return Segments(sorted(other.content + self.content))

    def _edit_dict(self, x: dict, s: Seg, tooltip, tooltip_shows_length, color):
        # Tooltip
        if tooltip is not None:
            if callable(tooltip):
                x["tooltip"] = tooltip(s)
            else:
                # x["tooltip"] = str(seg[tooltip])
                x["tooltip"] = str(s[tooltip])

        # Tooltip length
        # Jesus christ bro 🤦🏻‍♂️, just write another if inside and
        # stop doing parkour, you're not raymond belle 🙄
        if tooltip_shows_length:
            x["tooltip"] = x.get("tooltip", "") + f" ({round(s.length_h(), 1)}h)"
            x["tooltip"] = x["tooltip"].strip()

        # Color
        if color is not None:
            if callable(color):
                x = color(s)
            else:
                # TODO: Shouldn't be per element, but per day at the
                # config.json
                x = color

    def export_to_longcalendar(
        self,
        path_filename: str,
        hour_offset: float = 0.0,
        opacity: float = 1.0,
        tooltip: str | Callable[[Seg], str] | None = None,
        color: str | Callable[[Seg], str] | None = None,
        tooltip_shows_length: bool = False,
    ) -> None:
        """Long calendar is a custom application of mine that I use to visualize
        my data."""

        # Asserts n stuff
        assert isinstance(hour_offset, (float, int))
        assert isinstance(opacity, float)
        if not path_filename.endswith(".json"):
            raise ValueError("path_filename must end with .json")

        # If color is callable, it has one argument, the Seg itself
        assert color is None or isinstance(color, str) or callable(color)
        assert (
            color is None
            or isinstance(color, str)
            or len(inspect.signature(color).parameters) == 1
        )

        # Export itself
        to_export = []
        for seg in self.content:
            seg += datetime.timedelta(hours=hour_offset)
            splitted_segs = seg.split_into_segments_per_day()
            for s in splitted_segs:
                to_export.append(
                    {
                        "start": s.start.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end": s.end.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                )
                self._edit_dict(
                    to_export[-1],
                    s,
                    tooltip,
                    tooltip_shows_length,
                    color,
                )

        # Opacity setting
        if opacity != 1.0:
            for seg in to_export:
                seg["opacity"] = opacity

        # Export process
        if not os.path.exists(os.path.split(path_filename)[0]):
            os.makedirs(os.path.split(path_filename)[0])
        with open(path_filename, "w") as f:
            json.dump(to_export, f, indent=4, default=str)

    # TODO: time_to_mergue_s also accepts a datetime.timedelta
    # TODO: Or... is just a timedeleta :[
    @staticmethod
    def merge(
        segs: Segments,
        time_to_mergue_s: float,
        custom_rule: None | Callable[[Seg, Seg], bool] = None,
    ) -> Segments:
        """Merges segments that are close to each other in time."""

        to_return = []

        if custom_rule is None:
            to_return.append(segs.content[0])
            for seg in segs.content[1:]:
                if seg.start - to_return[-1].end < datetime.timedelta(
                    seconds=time_to_mergue_s
                ):
                    to_return[-1].end = seg.end
                    # TODO: Maybe adds a counter with "operation_merge_count"?
                else:
                    to_return.append(seg)
        else:
            to_return.append(segs.content[0])
            for seg in segs.content[1:]:
                if seg.start - to_return[-1].end < datetime.timedelta(
                    seconds=time_to_mergue_s
                ) and custom_rule(to_return[-1], seg):
                    to_return[-1].end = seg.end
                    # TODO: Maybe adds a counter with "operation_merge_count"?
                else:
                    to_return.append(seg)

        return Segments(to_return)
