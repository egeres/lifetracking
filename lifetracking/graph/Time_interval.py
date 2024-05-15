from __future__ import annotations

import datetime
import warnings
from enum import Enum, auto
from typing import Iterable

import pandas as pd
from typing_extensions import Self

from lifetracking.datatypes.Seg import Seg


class Time_resolution(Enum):
    HOUR = auto()
    DAY = auto()


# TODO_2: Maybe just rename as Interval ?


class Time_interval:
    """Used to get the time interval between two dates (inclusively)."""

    def __init__(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
    ):
        assert start <= end
        self.start: datetime.datetime = start
        self.end: datetime.datetime = end

    def __eq__(self, other: Time_interval) -> bool:
        assert isinstance(other, Time_interval)
        return self.start == other.start and self.end == other.end

    def __add__(self, other: datetime.timedelta) -> Time_interval:
        assert isinstance(other, datetime.timedelta)
        return Time_interval(self.start + other, self.end + other)

    def __sub__(self, other: datetime.timedelta) -> Time_interval:
        assert isinstance(other, datetime.timedelta)
        return Time_interval(self.start - other, self.end - other)

    def __contains__(self, another: datetime.datetime | Time_interval) -> bool:
        assert isinstance(another, (datetime.datetime, Time_interval))

        if isinstance(another, datetime.datetime):
            return self.start <= another <= self.end
        if isinstance(another, Time_interval):
            return self.start <= another.start and another.end <= self.end

        msg = f"Unsupported type: {type(another)}"
        raise TypeError(msg)

    def __copy__(self) -> Time_interval:
        return Time_interval(self.start, self.end)

    def overlaps(self, another: Time_interval) -> bool:
        """Touching intervals are NOT considered as overlapping (for now I guess)"""
        return self.start < another.end and another.start < self.end

    @classmethod
    def merge(cls, intervals: list[Time_interval]) -> list[Time_interval]:
        """Given a list of Time_intervals, it returns a list of Time_intervals
        where overlapping intervals are merged."""

        if not intervals:
            return []
        intervals.sort(key=lambda x: x.start)
        merged = [intervals[0]]
        for current in intervals[1:]:
            last = merged[-1]
            if last.end >= current.start:
                last.end = max(last.end, current.end)
            else:
                merged.append(current)
        return merged

    def to_json(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict[str, str]) -> Time_interval:
        return Time_interval(
            start=datetime.datetime.fromisoformat(data["start"]),
            end=datetime.datetime.fromisoformat(data["end"]),
        )

    def __repr__(self) -> str:
        return (
            f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
            + f",{self.end.strftime('%Y-%m-%d %H:%M')}>"
        )

    def tz_convert(self, tz: datetime.tzinfo) -> Time_interval:
        return Time_interval(self.start.astimezone(tz), self.end.astimezone(tz))

    def truncate(self, time_res: Time_resolution) -> Time_interval:
        assert isinstance(time_res, Time_resolution)

        if time_res == Time_resolution.HOUR:
            return Time_interval(
                self.start.replace(minute=0, second=0, microsecond=0),
                self.end.replace(minute=59, second=59, microsecond=999999),
            )
        if time_res == Time_resolution.DAY:
            return Time_interval(
                self.start.replace(hour=0, minute=0, second=0, microsecond=0),
                self.end.replace(hour=23, minute=59, second=59, microsecond=999999),
            )
        msg = f"Unsupported time resolution: {time_res}"
        raise ValueError(msg)

    def get_overlap_innerouter(
        self, another: Time_interval
    ) -> tuple[list[Time_interval], list[Time_interval]]:
        """Given another time interval (B), it returns a tuple of two lists of
        overlaps expressed in time intervals against the instance (A). The first
        list contains the overlap of B against A, the second list, the
        non-overlapping part of B against A.

        [Overlapping, Non-overlapping]"""

        # If "another" interval is within self interval
        if another.start >= self.start and another.end <= self.end:
            return [another], []

        # "another" has 0 overlap
        if another.end <= self.start:
            return [], [another]

        # "another" has 0 overlap
        if another.start >= self.end:
            return [], [another]

        # If "another" starts before self and ends within self
        if another.start < self.start and another.end <= self.end:
            return [Time_interval(self.start, another.end)], [
                Time_interval(another.start, self.start)
            ]

        # If "another" starts within self and ends after self
        if another.start >= self.start and another.end > self.end:
            return [Time_interval(another.start, self.end)], [
                Time_interval(self.end, another.end)
            ]

        # If "another" starts before self and ends after self
        if another.start < self.start and another.end > self.end:
            return [self], [
                Time_interval(another.start, self.start),
                Time_interval(self.end, another.end),
            ]

        msg = f"Unhandled case??: {self} {another}"
        raise ValueError(msg)

    def get_overlap_innerouter_list(
        self, another: list[Time_interval]
    ) -> tuple[list[Time_interval], list[Time_interval]]:
        """
        You give me a list of Time_interval like this:
        |---|      |---|   |-----------|
        And if the interval is like this:
               |--------------|

        It retrieves 2 lists, the first one with overlapping invervals and the second
        one with non-overlapping intervals.
                   |---|   |--|
               |---|   |---|

        """
        # TODO_2: Assert that the input is in order

        current_seg = self.__copy__()
        overlapping_intervals = []
        non_overlapping_intervals = []
        for s in another:
            if current_seg.overlaps(s):  # type: ignore
                sub_overlap, sub_non_overlap = s.get_overlap_innerouter(current_seg)  # type: ignore
                if len(sub_non_overlap) == 2:
                    non_overlapping_intervals.append(sub_non_overlap[0])
                current_seg = sub_non_overlap[-1] if len(sub_non_overlap) > 0 else None
                overlapping_intervals.extend(sub_overlap)
        if current_seg is not None:
            non_overlapping_intervals.append(current_seg)
        return overlapping_intervals, non_overlapping_intervals

    def normalize_ends(self) -> Self:
        self.start = self.start.replace(hour=0, minute=0, second=0, microsecond=0)
        self.end = self.end.replace(hour=23, minute=59, second=59, microsecond=999999)
        return self

    def to_seg(self) -> Seg:
        return Seg(self.start, self.end)

    def to_datetimeindex(self) -> pd.DatetimeIndex:
        return pd.date_range(self.start, self.end, freq="D")

    def iterate_over_interval(
        self, resolution: Time_resolution = Time_resolution.DAY
    ) -> Iterable[Time_interval]:
        """Iterates over the interval according to the resolution (inclusively)."""
        current = self.start

        if resolution == Time_resolution.DAY:
            while current <= self.end:
                yield Time_interval(
                    current.replace(hour=0, minute=0, second=0, microsecond=0),
                    current.replace(hour=23, minute=59, second=59, microsecond=999999),
                )
                current += datetime.timedelta(days=1)

        elif resolution == Time_resolution.HOUR:
            while current <= self.end:
                # yield current
                yield Time_interval(
                    current.replace(minute=0, second=0, microsecond=0),
                    current.replace(minute=59, second=59, microsecond=999999),
                )
                current += datetime.timedelta(hours=1)

        else:
            msg = f"Unsupported time resolution: {resolution}"
            raise ValueError(msg)

    @property
    def duration_days(self) -> float:
        """Careful! things like .last_week() are usually longer than 7 days,
        this is because it's not a week per se, but the last 7 days, starting
        and ending at 00:00 and 23:59 respectively."""

        warnings.warn(
            "duration_days is deprecated and will be removed in a future version",
            DeprecationWarning,
            stacklevel=2,
        )

        return (self.end - self.start).total_seconds() / 86400

    @property
    def duration(self) -> datetime.timedelta:
        return self.end - self.start

    @staticmethod
    def last_n_days(n: int, now: datetime.datetime | None = None) -> Time_interval:
        if now is None:
            now = datetime.datetime.now()
        return Time_interval(
            start=(datetime.datetime.now() - datetime.timedelta(days=n)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=datetime.datetime.now().replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )

    # The following methods are shortcuts to create time spans of common lengths

    @staticmethod
    def today():
        return Time_interval.last_n_days(0)  # next_n_days(0) is also valid!

    @staticmethod
    def tomorrow():
        return Time_interval(
            start=(datetime.datetime.now() + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=(datetime.datetime.now() + datetime.timedelta(days=1)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )

    @staticmethod
    def yesterday():
        return Time_interval(
            start=(datetime.datetime.now() - datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=(datetime.datetime.now() - datetime.timedelta(days=1)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )

    @staticmethod
    def last_day() -> Time_interval:
        return Time_interval.last_n_days(1)

    @staticmethod
    def last_week() -> Time_interval:
        return Time_interval.last_n_days(7)

    @staticmethod
    def last_month():
        return Time_interval.last_n_days(30)

    @staticmethod
    def last_trimester():
        return Time_interval.last_n_days(30 * 3 + 1)

    @staticmethod
    def last_semester():
        return Time_interval.last_n_days(30 * 6 + 3)

    @staticmethod
    def last_year():
        n = datetime.datetime.now()
        return Time_interval(start=n.replace(year=n.year - 1), end=n).normalize_ends()

    @staticmethod
    def last_decade():
        n = datetime.datetime.now()
        return Time_interval(start=n.replace(year=n.year - 10), end=n).normalize_ends()

    @staticmethod
    def next_n_days(n: int) -> Time_interval:
        return Time_interval(
            start=datetime.datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=(datetime.datetime.now() + datetime.timedelta(days=n)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )
