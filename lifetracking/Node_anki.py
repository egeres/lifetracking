from __future__ import annotations

import datetime
import hashlib
import multiprocessing
import warnings
from pathlib import Path

import ankipandas
import pandas as pd
from pandas.errors import DatabaseError

from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_anki_study(Node_pandas, Node_0child):
    path_dir_datasource = Path.home() / "AppData" / "Roaming" / "Anki2"
    path_file_anki = path_dir_datasource / "User 1" / "collection.anki2"

    def __init__(self, path_dir: Path | str | None = None) -> None:
        if path_dir is None:
            path_dir = self.path_dir_datasource
        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert path_dir.exists()
        assert path_dir.is_dir()
        super().__init__()
        self.path_dir = path_dir

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return self.path_dir.exists()

    def _get_raw_data(self, path_file, return_dict):
        try:
            col = ankipandas.Collection(path_file)
            return_dict[0] = col.cards[["cdeck"]].copy()
            return_dict[1] = col.revs.copy()
        except DatabaseError as e:
            warnings.warn(f"Anki databaseError: {e}", stacklevel=2)
            return_dict[0] = None
            return_dict[1] = None

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        # Data gathering
        # return_dict = {}
        # self._get_raw_data(self.path_file_anki, return_dict)
        # cards = return_dict[0]
        # revisions = return_dict[1]

        # Data gathering (multiprocessing)
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(
            target=self._get_raw_data,
            args=(self.path_file_anki, return_dict),
        )
        p.start()
        p.join()
        cards = return_dict.values()[0]
        revisions = return_dict.values()[1]

        if cards is None or revisions is None:
            return None

        revisions = revisions.join(cards, on="cid")
        # Date parsing
        revisions["timestamp"] = revisions.index / 1e3
        revisions["timestamp"] = revisions["timestamp"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )

        # Set index
        revisions["timestamp"] = pd.to_datetime(revisions["timestamp"])
        revisions = revisions.set_index("timestamp")

        # We filter by time interval
        if t is None:
            return revisions
        return revisions[(revisions.index >= t.start) & (revisions.index <= t.end)]


class Parse_anki_creation(Parse_anki_study):
    def _get_raw_data(self, path_file, return_dict) -> None:
        try:
            col = ankipandas.Collection(path_file)
            return_dict[0] = col.cards.copy()
        except DatabaseError as e:
            warnings.warn(f"Anki databaseError: {e}", stacklevel=2)
            return_dict[0] = None

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        # Data gathering
        # return_dict = {}
        # self._get_raw_data(self.path_file_anki, return_dict)
        # cards = return_dict[0]

        # Data gathering (multiprocessing)
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(
            target=self._get_raw_data,
            args=(self.path_file_anki, return_dict),
        )
        p.start()
        p.join()
        cards = return_dict.values()[0]

        if cards is None:
            return None

        # Rename deck column
        cards = cards.rename(columns={"cdeck": "deck"})
        # Date parsing
        cards["timestamp"] = cards.index / 1e3
        cards["timestamp"] = cards["timestamp"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )

        cards["timestamp"] = pd.to_datetime(cards["timestamp"])
        cards = cards.set_index("timestamp")

        # We filter by time interval
        if t is None:
            return cards
        return cards[(cards.index >= t.start) & (cards.index <= t.end)]
