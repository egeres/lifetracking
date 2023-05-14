from __future__ import annotations

import datetime
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
