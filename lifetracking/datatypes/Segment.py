from __future__ import annotations

import datetime
import hashlib
import json
import os
from typing import overload

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

    def __len__(self) -> int:
        return len(self.content)

    def _split_seg_into_dicts(self, seg, hour_offset: float = 0) -> list[dict]:
        """Helper function to split segments into separate dicts for each day."""

        # Date modification
        # TODO: Review and avoid changing timezone when everything is more stable
        start = seg.start.replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(
            hours=hour_offset
        )
        end = seg.end.replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(
            hours=hour_offset
        )

        # Splitting itself
        split_dicts = []
        while start < end:
            next_day = datetime.datetime(
                start.year, start.month, start.day, tzinfo=datetime.timezone.utc
            ) + datetime.timedelta(days=1)
            if next_day > end:
                next_day = end
            split_dicts.append(
                {
                    "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                    "end": (next_day - datetime.timedelta(seconds=1)).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    "tooltip": seg.value,
                }
            )
            start = next_day
        return split_dicts

    def export_to_longcalendar(
        self, path_filename: str, hour_offset: float = 0.0, opacity: float = 1.0
    ) -> None:
        """Long calendar is a custom application of mine that I use to visualize
        my data."""

        # Asserts n stuff
        assert isinstance(hour_offset, (float, int))
        assert isinstance(opacity, float)
        if not path_filename.endswith(".json"):
            raise ValueError("path_filename must end with .json")

        # Export itself
        to_export = []
        for seg in self.content:
            to_export += self._split_seg_into_dicts(seg, hour_offset)

        # Opacity setting
        if opacity != 1.0:
            for seg in to_export:
                seg["opacity"] = opacity

        # Export process
        if not os.path.exists(os.path.split(path_filename)[0]):
            os.makedirs(os.path.split(path_filename)[0])
        with open(os.path.join(path_filename), "w") as f:
            json.dump(to_export, f, indent=4, default=str)

    def min(self) -> datetime.datetime:
        return min(seg.start for seg in self.content)

    def max(self) -> datetime.datetime:
        return max(seg.end for seg in self.content)

    def __add__(self, other: Segments) -> Segments:
        return Segments(sorted(other.content + self.content))

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
