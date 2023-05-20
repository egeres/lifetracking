from __future__ import annotations

import datetime
import hashlib
import json
import os
from functools import reduce

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
        assert all(
            seg.start <= seg.end for seg in self.content
        ), "Segments must be ordered in time"
        hshed_content = [seg._hashstr() for seg in self.content]
        reduced = reduce(lambda x, y: x + y, hshed_content, "")
        return hashlib.md5(reduced.encode()).hexdigest()

    def __getitem__(self, index: Time_interval | int) -> Segments:
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

    def export_to_longcalendar(
        self, path_filename: str, hour_offset: float = 0, opacity: float = 1.0
    ) -> None:
        """Long calendar is a custom application of mine that I use to visualize
        my data."""

        if not path_filename.endswith(".json"):
            raise ValueError("path_filename must end with .json")

        to_export = [
            {
                "start": (seg.start + datetime.timedelta(hours=hour_offset)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "end": (seg.end + datetime.timedelta(hours=hour_offset)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                "tooltip": seg.value,
            }
            for seg in self.content
        ]

        if opacity != 1.0:
            for seg in to_export:
                seg["opacity"] = opacity

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
