from __future__ import annotations

import datetime


class Time_interval:
    """Used to get the time interval between two dates, in an inclusive way."""

    def __init__(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
    ):
        self.start: datetime.datetime = start
        self.end: datetime.datetime = end

    @staticmethod
    def last_n_days(n: int) -> Time_interval:
        return Time_interval(
            start=(datetime.datetime.now() - datetime.timedelta(days=n)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            end=(datetime.datetime.now()).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )

    @staticmethod
    def today():
        return Time_interval.last_n_days(0)

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

    # Undecided about the implementation...
    # @staticmethod
    # def next_n_days(n: int) -> Time_interval:
    #     return Time_interval(
    #         start=datetime.datetime.now().replace(
    #             hour=0, minute=0, second=0, microsecond=0
    #         ),
    #         end=(
    #             datetime.datetime.now() + datetime.timedelta(days=n)
    #         ).replace(hour=23, minute=59, second=59),
    #     )

    @staticmethod
    def tomorrow():
        return Time_interval(
            start=(datetime.datetime.now() + datetime.timedelta(days=1)).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            ),
            end=(datetime.datetime.now() + datetime.timedelta(days=1)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            ),
        )
