from __future__ import annotations

import datetime
from enum import Enum, auto
from typing import Iterable

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
        self.start: datetime.datetime = start
        self.end: datetime.datetime = end

    def __add__(self, other: datetime.timedelta) -> Time_interval:
        return Time_interval(self.start + other, self.end + other)

    def __sub__(self, other: datetime.timedelta) -> Time_interval:
        return Time_interval(self.start - other, self.end - other)

    def __contains__(self, another: datetime.datetime) -> bool:
        assert isinstance(another, datetime.datetime)
        return self.start <= another <= self.end

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

    @staticmethod
    def last_n_days(n: int) -> Time_interval:
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
        year_days = 365
        # We take into account if it's a leap year c:
        if datetime.datetime.now().year % 4 == 0:
            year_days = 366
        return Time_interval.last_n_days(year_days)

    @staticmethod
    def last_decade():
        n = datetime.datetime.now()
        return Time_interval(start=n.replace(year=n.year - 10), end=n)

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

    def __eq__(self, other: Time_interval) -> bool:
        return self.start == other.start and self.end == other.end
