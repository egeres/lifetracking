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


class Parse_anki_study(Node_pandas):
    path_dir_datasource: str = os.path.join(
        os.path.expanduser("~"), "AppData", "Roaming", "Anki2"
    )

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

    def _available(self) -> bool:
        return os.path.exists(self.path_dir)

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        # Data gathering
        col = ankipandas.Collection(
            os.path.join(self.path_dir, "User 1", "collection.anki2")
        )
        revisions = col.revs.copy()

        # We add the deck name
        cards = col.cards[["cdeck"]]
        revisions = revisions.join(cards, on="cid")
        # Date parsing
        revisions["timestamp"] = revisions.index / 1e3
        revisions["timestamp"] = revisions["timestamp"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )

        # Sort by timestamp
        # revisions = revisions.sort_values("timestamp", ascending=True)

        # We filter by time interval
        if t is None:
            return revisions
        return revisions[
            (revisions["timestamp"] >= t.start) & (revisions["timestamp"] <= t.end)
        ]

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


class Parse_anki_creation(Parse_anki_study):
    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        # Data gathering
        col = ankipandas.Collection(
            os.path.join(self.path_dir, "User 1", "collection.anki2")
        )
        cards = col.cards.copy()

        # Rename deck column
        cards = cards.rename(columns={"cdeck": "deck"})
        # Date parsing
        cards["timestamp"] = cards.index / 1e3
        cards["timestamp"] = cards["timestamp"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )

        # We filter by time interval
        if t is None:
            return cards
        return cards[(cards["timestamp"] >= t.start) & (cards["timestamp"] <= t.end)]
