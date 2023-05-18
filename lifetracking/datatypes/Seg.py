from __future__ import annotations

import datetime
import hashlib
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
            return (
                f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
                + f",{self.end.strftime('%Y-%m-%d %H:%M')}>"
            )
        else:
            return (  # Thank god these line up 😌
                f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
                + f",{self.end.strftime('%Y-%m-%d %H:%M')}, {self.value}>"
            )

    def __lt__(self, other: Seg) -> bool:
        return self.start < other.start

    def __add__(self, other: datetime.timedelta) -> Seg:
        if not isinstance(other, datetime.timedelta):
            raise TypeError(
                f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'"
            )
        return Seg(self.start + other, self.end + other)

    def __sub__(self, other: datetime.timedelta) -> Seg:
        if not isinstance(other, datetime.timedelta):
            raise TypeError(
                f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'"
            )
        return Seg(self.start - other, self.end - other)

    def __eq__(self, other: Seg) -> bool:
        return (
            self.start == other.start
            and self.end == other.end
            and self.value == other.value
        )

    def _hashstr(self) -> str:
        # TODO this needs some work... 🥵
        a = (
            self.start.strftime("%Y-%m-%d %H:%M"),
            self.end.strftime("%Y-%m-%d %H:%M"),
            self.value,
        )
        return hashlib.md5(str(a).encode()).hexdigest()
