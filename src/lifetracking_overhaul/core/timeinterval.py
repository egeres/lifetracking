from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


class TimeInterval:
    """Used to get the time interval between two dates (inclusively)."""

    def __init__(
        self,
        start: datetime,
        end: datetime,
    ):
        if start > end:
            msg = "Start date must be before end date"
            raise ValueError(msg)
        self.start: datetime = start
        self.end: datetime = end

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TimeInterval):
            return False
        return self.start == other.start and self.end == other.end

    def __hash__(self) -> int:
        return hash((self.start, self.end))

    def __len__(self) -> int:
        return (self.end - self.start).days + 1

    # def __contains__(self, another: datetime | Time_interval | Seg) -> bool:

    def __copy__(self) -> TimeInterval:
        return TimeInterval(self.start, self.end)

    def __deepcopy__(self, memo: dict[int, Any]) -> TimeInterval:
        return TimeInterval(self.start, self.end)

    def __repr__(self) -> str:
        return (
            f"<{self.start.strftime('%Y-%m-%d %H:%M')}"
            f",{self.end.strftime('%Y-%m-%d %H:%M')}>"
        )

    # CUSTOM METHODS ###################################################################

    def to_json(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    # EASY ACCESS ######################################################################

    @staticmethod
    def last_n_days(n: float, now: datetime | None = None) -> TimeInterval:
        now = now or datetime.now()

        return TimeInterval(
            start=(datetime.now() - timedelta(days=n)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=datetime.now().replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )

    @staticmethod
    def last_day() -> TimeInterval:
        return TimeInterval.last_n_days(1)

    @staticmethod
    def last_week() -> TimeInterval:
        return TimeInterval.last_n_days(7)

    @staticmethod
    def last_month() -> TimeInterval:
        return TimeInterval.last_n_days(30)

    @staticmethod
    def last_trimester() -> TimeInterval:
        return TimeInterval.last_n_days(30 * 3 + 1)

    @staticmethod
    def last_semester() -> TimeInterval:
        return TimeInterval.last_n_days(30 * 6 + 3)

    @staticmethod
    def last_year() -> TimeInterval:
        n = datetime.now()
        return TimeInterval(start=n.replace(year=n.year - 1), end=n).normalize_ends()

    @staticmethod
    def last_decade() -> TimeInterval:
        n = datetime.now()
        return TimeInterval(start=n.replace(year=n.year - 10), end=n).normalize_ends()

    @staticmethod
    def next_n_days(n: int) -> TimeInterval:
        return TimeInterval(
            start=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            end=(datetime.now() + timedelta(days=n)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )
