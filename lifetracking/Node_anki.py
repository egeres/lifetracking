from __future__ import annotations

import datetime
import hashlib
import multiprocessing
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import ankipandas
import pandas as pd
from pandas.errors import DatabaseError

from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_pandas import Node_pandas

if TYPE_CHECKING:
    from lifetracking.graph.Time_interval import Time_interval


class Parse_anki_study(Node_pandas, Node_0child):
    dir_anki_default = Path.home() / "AppData" / "Roaming" / "Anki2"

    def __init__(self, dir_anki: Path | str | None = None) -> None:
        if dir_anki is None:
            dir_anki = self.dir_anki_default
        if isinstance(dir_anki, str):
            dir_anki = Path(dir_anki)
        assert dir_anki.exists()
        assert dir_anki.is_dir()
        super().__init__()
        self.dir_anki = dir_anki

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.dir_anki)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return self.dir_anki.exists()

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

        if len(list(self.dir_anki.glob("User*"))) > 1:
            msg = "There is still not an implementation for multi-user in the anki node"
            raise NotImplementedError(msg)

        assert (self.dir_anki / "User 1" / "collection.anki2").exists()

        # Data gathering (multiprocessing)
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(
            target=self._get_raw_data,
            args=(self.dir_anki / "User 1" / "collection.anki2", return_dict),
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

        if len(list(self.dir_anki.glob("User*"))) > 1:
            msg = "There is still not an implementation for multi-user in the anki node"
            raise NotImplementedError(msg)

        assert (self.dir_anki / "User 1" / "collection.anki2").exists()

        # Data gathering (multiprocessing)
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(
            target=self._get_raw_data,
            args=(self.dir_anki / "User 1" / "collection.anki2", return_dict),
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
