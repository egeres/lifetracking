from __future__ import annotations

import datetime
import hashlib
import os
from typing import Any

import ankipandas
import pandas as pd
from browser_history.browsers import (
    Brave,
    Chrome,
    Chromium,
    Edge,
    Firefox,
    Opera,
    OperaGX,
    Safari,
    Vivaldi,
)
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_browserhistory(Node_pandas):
    def __init__(self) -> None:
        super().__init__()
        self.browsers = [
            Brave(),
            Chrome(),
            Chromium(),
            Edge(),
            Firefox(),
            Opera(),
            OperaGX(),
            Vivaldi(),
        ]
        if not os.name == "nt":
            self.browsers.append(Safari())

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _available(self) -> bool:
        return any(
            any(os.path.exists(y) for y in x.paths(profile_file=x.history_file))
            for x in self.browsers
        )

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        dfs_to_concat = []
        for i in self.browsers:
            # if we are in windows, there are some conds where we might skip
            if os.name == "nt":
                if i.windows_path is None:
                    continue
                if not os.path.exists(
                    os.path.join(os.environ["USERPROFILE"], i.windows_path)
                ):
                    continue

            # TODO: Maybe extend the library to allow for a time interval
            histories = i.fetch_history().histories
            if len(histories) == 0:
                continue
            df = pd.DataFrame(histories, columns=["date", "url"])
            df["date"] = df["date"].dt.tz_localize(None)  # TODO: Pls, fix this 🙄
            if t is not None:
                df = df[df["date"] >= t.start]
                df = df[df["date"] <= t.end]
            df["browser"] = i.name

            dfs_to_concat.append(df)

        df = pd.concat(dfs_to_concat)

        return df

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)
