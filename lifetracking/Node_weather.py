from __future__ import annotations

from typing import TYPE_CHECKING

import meteostat

from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval

if TYPE_CHECKING:
    import pandas as pd


class Node_weather(Node_pandas, Node_0child):
    def __init__(
        self,
        loc_lat: tuple[float, float] = (41.39843372360185, 2.1690425312566184),
    ) -> None:
        super().__init__()
        assert isinstance(loc_lat, tuple)
        assert len(loc_lat) == 2
        self.loc_lat = loc_lat

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _available(self) -> bool:
        return True

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        if t is None:
            t = Time_interval.last_year()

        location = meteostat.Point(*self.loc_lat, 70)
        data = meteostat.Daily(
            location,
            t.start,
            t.end,
        )
        return data.fetch()
