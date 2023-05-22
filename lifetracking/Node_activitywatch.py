from __future__ import annotations

import datetime
import hashlib
import os
from typing import Any

import pandas as pd
import requests
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_activitywatch(Node_pandas):
    def _get_buckets(self, url_base: str = "http://localhost:5600") -> list[dict]:
        """Extracts a particular type of bucket"""

        out = requests.get(
            f"{url_base}/api/0/buckets",
        )

        if out.status_code == 200:
            content = out.json()
            return list(
                filter(lambda x: x["client"] == "aw-watcher-window", content.values())
            )
        else:
            raise Exception("The connection had a problem!")

    def __init__(self, config) -> None:
        super().__init__()
        self.config = config

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + self.config).encode()).hexdigest()

    def _available(self) -> bool:
        raise NotImplementedError

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)
