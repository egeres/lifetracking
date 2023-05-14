from __future__ import annotations

import datetime
import json
import os
from typing import Any


class Seg:
    def __init__(
        self, start: datetime.datetime, end: datetime.datetime, value: Any | None = None
    ):
        self.start = start
        self.end = end
        self.value = value

    def __repr__(self) -> str:
        if self.value is None:
            return f"[{self.start}|{self.end}]"
        else:
            return f"[{self.start}|{self.end}, {self.value}]"


class Segments:
    def __init__(self, content: list[Seg]) -> None:
        self.content = content

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
            os.makedirs(path_filename)

        with open(os.path.join(path_filename), "w") as f:
            json.dump(to_export, f, indent=4, default=str)
