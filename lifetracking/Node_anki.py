from __future__ import annotations

import datetime
import hashlib
import os
from typing import Any

import ankipandas
import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_anki(Node_pandas):
    path_dir_datasource: str = rf"C:/Users/{os.getlogin()}/AppData/Roaming/Anki2"

    def __init__(self, path_dir: str | None = None) -> None:
        if path_dir is None:
            path_dir = self.path_dir_datasource
        if not os.path.isdir(path_dir):
            raise ValueError(f"{path_dir} is not a directory")
        super().__init__()
        self.path_dir = path_dir

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        # We load the collection
        col = ankipandas.Collection(rf"{self.path_dir}\User 1\collection.anki2")
        revisions = col.revs.copy()

        # We parse the revisions column
        revisions["timestamp"] = revisions["cid"] / 1e3
        revisions["timestamp"] = revisions["timestamp"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )
        return revisions

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


if __name__ == "__main__":
    a = Parse_anki()
    o = a.run()
    p = 0
