from __future__ import annotations

import datetime
import hashlib
from typing import Any


class Seg:
    def __init__(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        value: dict | None = None,
    ):
        assert start <= end
        assert isinstance(value, dict) or value is None
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
        return Seg(self.start + other, self.end + other, self.value)

    def __sub__(self, other: datetime.timedelta) -> Seg:
        if not isinstance(other, datetime.timedelta):
            raise TypeError(
                f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'"
            )
        return Seg(self.start - other, self.end - other, self.value)

    def __eq__(self, other: Seg) -> bool:
        return (
            self.start == other.start
            and self.end == other.end
            and self.value == other.value
        )

    def __getitem__(self, key: Any) -> Any:
        if self.value is None:
            self.value = {}
        return self.value[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        if self.value is None:
            self.value = {}
        self.value[key] = value

    def _hashstr(self) -> str:
        # TODO this needs some work... 🥵
        a = (
            self.start.strftime("%Y-%m-%d %H:%M"),
            self.end.strftime("%Y-%m-%d %H:%M"),
            self.value,
        )
        return hashlib.md5(str(a).encode()).hexdigest()

    def split_into_segments_per_day(self) -> list[Seg]:
        """If a segment spans multiple days, split it into multiple segments,
        one per day."""

        splits = []
        while self.start < self.end:
            next_day = datetime.datetime(
                self.start.year,
                self.start.month,
                self.start.day,
                tzinfo=self.end.tzinfo,
            ) + datetime.timedelta(days=1)
            remove_seconds = datetime.timedelta(seconds=1)
            if next_day > self.end:
                next_day = self.end
                remove_seconds = datetime.timedelta(seconds=0)
            splits.append(
                Seg(
                    self.start,
                    next_day - remove_seconds,
                    self.value,
                )
            )
            self.start = next_day
        return splits
