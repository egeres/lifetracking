from __future__ import annotations

from typing import Any

import meteostat
import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Node_weather(Node_pandas):
    def __init__(self, loc_lat: tuple[float, float]) -> None:
        super().__init__()
        self.loc_lat = loc_lat

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _available(self) -> bool:
        return True

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        if t is None:
            t = Time_interval.last_year()

        location = meteostat.Point(41.39843372360185, 2.1690425312566184, 70)
        data = meteostat.Daily(
            location,
            t.start,
            t.end,
        )
        data = data.fetch()

        return data

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)
