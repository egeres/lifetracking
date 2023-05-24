from __future__ import annotations

import datetime
from enum import Enum, auto
from typing import Iterable

from typing_extensions import Self

from lifetracking.datatypes.Seg import Seg


class Time_resolution(Enum):
    HOUR = auto()
    DAY = auto()


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
        return self.start == other.start and self.end == other.end

    def __add__(self, other: datetime.timedelta) -> Time_interval:
        return Time_interval(self.start + other, self.end + other)

    def __sub__(self, other: datetime.timedelta) -> Time_interval:
        return Time_interval(self.start - other, self.end - other)

    def __contains__(self, another: datetime.datetime) -> bool:
        assert isinstance(another, datetime.datetime)
        return self.start <= another <= self.end

    def __repr__(self) -> str:
        return (
            f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
            + f",{self.end.strftime('%Y-%m-%d %H:%M')}>"
        )

    def truncate(self, time_res: Time_resolution) -> Time_interval:
        if time_res == Time_resolution.HOUR:
            return Time_interval(
                self.start.replace(minute=0, second=0, microsecond=0),
                self.end.replace(minute=59, second=59, microsecond=999999),
            )
        elif time_res == Time_resolution.DAY:
            return Time_interval(
                self.start.replace(hour=0, minute=0, second=0, microsecond=0),
                self.end.replace(hour=23, minute=59, second=59, microsecond=999999),
            )
        else:
            raise ValueError(f"Unsupported time resolution: {time_res}")

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
        elif another.end <= self.start:
            return [], [another]

        # "another" has 0 overlap
        elif another.start >= self.end:
            return [], [another]

        # If "another" starts before self and ends within self
        elif another.start < self.start and another.end <= self.end:
            return [Time_interval(self.start, another.end)], [
                Time_interval(another.start, self.start)
            ]

        # If "another" starts within self and ends after self
        elif another.start >= self.start and another.end > self.end:
            return [Time_interval(another.start, self.end)], [
                Time_interval(self.end, another.end)
            ]

        # If "another" starts before self and ends after self
        elif another.start < self.start and another.end > self.end:
            return [self], [
                Time_interval(another.start, self.start),
                Time_interval(self.end, another.end),
            ]

        else:
            raise ValueError("Unhandled case ??")

    def normalize_ends(self) -> Self:
        self.start = self.start.replace(hour=0, minute=0, second=0, microsecond=0)
        self.end = self.end.replace(hour=23, minute=59, second=59, microsecond=999999)
        return self

    def to_seg(self) -> Seg:
        return Seg(self.start, self.end)

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
            raise ValueError(f"Unsupported time resolution: {resolution}")

    @property
    def duration_days(self) -> float:
        """Careful! things like .last_week() are usually longer than 7 days,
        this is because it's not a week per se, but the last 7 days, starting
        and ending at 00:00 and 23:59 respectively."""

        return (self.end - self.start).total_seconds() / 86400

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
