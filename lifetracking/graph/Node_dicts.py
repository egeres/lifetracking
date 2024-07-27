from __future__ import annotations

import hashlib
import json
from datetime import datetime, tzinfo
from pathlib import Path
from typing import Any, Callable, Literal

from lifetracking.graph.Node import Node, Node_0child, Node_1child

# TODO_2: We whould refactor the names of the readers, right now it is
#
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import hash_method

# TODO_2: Compare this node with other nodes and add missing features

# TEST: Add at least one test for this node??


class Node_dicts(Node[list[dict]]):
    def __init__(self) -> None:
        super().__init__()

    def export_to_longcalendar(
        self,
        t: Time_interval | int | None,
        path_filename: str,
        hour_offset: float = 0.0,
        opacity: float = 1.0,
        tooltip: str | Callable[[list[dict]], str] | None = None,
        color: str | Callable[[list[dict]], str] | None = None,
        tooltip_shows_length: bool = False,
    ):
        msg = "This method is not implemented yet hehe"
        raise NotImplementedError(msg)


class Reader_dicts(Node_0child, Node_dicts):
    """Base class for reading pandas dataframes from files of varied formats"""

    file_extension: Literal[".json"] = ".json"

    def __init__(
        self,
        path_dir: str | Path,
        dated_name: Callable[[str], datetime] | None = None,
        column_date_index: str | Callable | None = None,
        time_zone: None | tzinfo = None,
    ) -> None:
        super().__init__()

        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert path_dir.exists()
        assert dated_name is None or callable(dated_name)
        assert (
            column_date_index is None
            or isinstance(column_date_index, str)
            or callable(column_date_index)
        )
        assert time_zone is None or isinstance(time_zone, tzinfo)

        self.path_dir = path_dir
        self.dated_name = dated_name
        self.column_date_index = column_date_index
        self.time_zone = time_zone

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr() + str(self.path_dir) + str(self.column_date_index)
            ).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return self.path_dir.exists() and any(
            self.path_dir.glob(f"*{self.file_extension}")
        )

    def _operation(self, t: Time_interval | None = None) -> list[dict]:
        assert t is None or isinstance(t, Time_interval)

        return [
            json.loads(i.read_text())
            for i in self.path_dir.glob(f"*{self.file_extension}")
        ]

    def apply(
        self,
        fn: Callable[[dict], Any],
    ) -> Node_dicts:
        return Node_dicts_operation(self, fn)  # type: ignore


class Node_dicts_operation(Node_1child, Node_dicts):
    """Node to apply an operation to a pandas dataframe"""

    def __init__(
        self,
        n0: Node_dicts,
        fn_operation: Callable,
        # fn_operation: Callable[
        #     [pd.DataFrame | PrefectFuture[pd.DataFrame, Sync]], pd.DataFrame
        # ],
    ) -> None:
        assert isinstance(n0, Node_dicts)
        assert callable(fn_operation), "operation_main must be callable"
        super().__init__()
        self.n0 = n0
        self.fn_operation = fn_operation

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + hash_method(self.fn_operation)).encode()
        ).hexdigest()

    @property
    def child(self) -> Node:
        return self.n0

    def _operation(
        self,
        # n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        n0: list[dict],
        t: Time_interval | None = None,
    ) -> list[dict]:
        assert t is None or isinstance(t, Time_interval)
        if len(n0) == 0:
            return n0
        return self.fn_operation(n0)
