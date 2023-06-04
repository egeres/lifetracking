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

    def export_to_longcalendar(
        self,
        path_filename: str,
        hour_offset: float = 0.0,
        opacity: float = 1.0,
        tooltip: str | None = None,
        color: str | Callable[[Seg], str] | None = None,
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
                if tooltip is not None:
                    to_export[-1]["tooltip"] = str(seg[tooltip])
                if color is not None:
                    if callable(color):
                        to_export[-1]["color"] = color(s)
                    else:
                        # TODO: Shouldn't be per element, but per day at the config.json
                        to_export[-1]["color"] = color

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
    def merge(segs: Segments, time_to_mergue_s: float) -> Segments:
        """Merges segments that are close to each other in time."""

        to_return = []
        for seg in segs.content:
            if len(to_return) == 0:
                to_return.append(seg)
            else:
                if seg.start - to_return[-1].end < datetime.timedelta(
                    seconds=time_to_mergue_s
                ):
                    to_return[-1].end = seg.end
                else:
                    to_return.append(seg)
        return Segments(to_return)
