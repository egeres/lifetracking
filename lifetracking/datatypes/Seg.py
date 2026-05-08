from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta
from typing import Any


class Seg:
    def __init__(
        self,
        start: datetime,
        end: datetime,
        value: dict | None = None,
    ):
        assert start <= end
        assert isinstance(value, dict) or value is None
        self.start = start
        self.end = end
        self.value = value

    def get(self, key: Any, default: Any = None) -> Any:
        if self.value is None:
            return None
        return self.value.get(key, default)

    def overlaps(self, other: Seg) -> bool:
        """Returns true if the two segments overlap in time"""
        return (
            (self.start < other.end and self.start > other.start)
            or (self.end < other.end and self.end > other.start)
            or (other.start < self.end and other.start > self.start)
            or (other.end < self.end and other.end > self.start)
        )

    def copy(self) -> Seg:
        """🤷🏻‍♂️ like, bruh, why do we need to import copy?"""
        return copy.copy(self)

    def __copy__(self):
        return Seg(self.start, self.end, copy.copy(self.value))

    def __repr__(self) -> str:
        if self.value is None:
            return (
                f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
                f",{self.end.strftime('%Y-%m-%d %H:%M')}>"
            )

        return (  # Thank god these line up 😌
            f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
            f",{self.end.strftime('%Y-%m-%d %H:%M')}, {self.value}>"
        )

    def __lt__(self, other: Seg) -> bool:
        return self.start < other.start

    def __add__(self, other: timedelta) -> Seg:
        if not isinstance(other, timedelta):
            msg = (
                f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'"
            )
            raise TypeError(msg)
        return Seg(self.start + other, self.end + other, self.value)

    def __sub__(self, other: timedelta) -> Seg:
        if not isinstance(other, timedelta):
            msg = (
                f"unsupported operand type(s) for -: '{type(self)}' and '{type(other)}'"
            )
            raise TypeError(msg)
        return Seg(self.start - other, self.end - other, self.value)

    def __eq__(self, other: Seg) -> bool:
        if not isinstance(other, Seg):
            return False
        return (
            self.start == other.start
            and self.end == other.end
            and self.value == other.value
        )

    def __getitem__(self, key: Any) -> Any:
        assert self.value is not None, "Current Seg has no value"
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

        temp_start = self.start
        splits = []
        while temp_start < self.end:
            next_day = datetime(
                temp_start.year,
                temp_start.month,
                temp_start.day,
                tzinfo=self.end.tzinfo,
            ) + timedelta(days=1)

            splits.append(
                Seg(
                    temp_start,
                    (
                        self.end
                        if next_day > self.end
                        else next_day - timedelta(microseconds=1)
                    ),
                    self.value,
                )
            )

            temp_start = next_day

        return splits

    def length_days(self) -> float:
        return (self.end - self.start).total_seconds() / 86400.0

    def length_h(self) -> float:
        return (self.end - self.start).total_seconds() / 3600.0

    def length_m(self) -> float:
        return (self.end - self.start).total_seconds() / 60.0

    def length_s(self) -> float:
        return (self.end - self.start).total_seconds()

    def length(self) -> timedelta:
        return self.end - self.start
