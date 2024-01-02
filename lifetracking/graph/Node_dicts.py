from __future__ import annotations

import datetime
import hashlib
import json
import os
from typing import Any, Callable, Literal

from lifetracking.graph.Node import Node, Node_0child, Node_1child

# TODO_2: We whould refactor the names of the readers, right now it is
from lifetracking.graph.Node_dicts import Reader_dicts
from lifetracking.graph.Node_pandas import Reader_jsons
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import hash_method

# TODO_2: Compare this node with other nodes and add missing features

# TEST: Add at least one test for this node??


class Node_dicts(Node[list[dict]]):
    def __init__(self) -> None:
        super().__init__()


class Reader_dicts(Node_0child, Node_dicts):
    """Base class for reading pandas dataframes from files of varied formats"""

    file_extension: Literal[".json"] = ".json"

    def __init__(
        self,
        path_dir: str,
        dated_name: Callable[[str], datetime.datetime] | None = None,
        column_date_index: str | Callable | None = None,
        time_zone: None | datetime.tzinfo = None,
    ) -> None:
        super().__init__()
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
        return True

        # TODO_2: Do this with pathlib,

        # TODO_2: For detecting the dir is not empty, instead of len([]) > 0, do a any()
        # Also, apply that to more codes

        # if self.path_dir.endswith(self.file_extension):
        #     return os.path.exists(self.path_dir)
        # return (
        #     os.path.isdir(self.path_dir)
        #     and os.path.exists(self.path_dir)
        #     and len(
        #         [
        #             i
        #             for i in os.listdir(self.path_dir)
        #             if i.endswith(self.file_extension)
        #         ]
        #     )
        #     > 0
        # )

    def _operation(self, t: Time_interval | None = None) -> list[dict]:
        assert t is None or isinstance(t, Time_interval)

        # Get files
        files_to_read = (
            [self.path_dir]
            if self.path_dir.endswith(self.file_extension)
            else (
                x for x in os.listdir(self.path_dir) if x.endswith(self.file_extension)
            )
        )

        to_return: list[dict] = []
        for filename in files_to_read:
            with open(os.path.join(self.path_dir, filename)) as f:
                to_return.append(json.load(f))

        return to_return

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
