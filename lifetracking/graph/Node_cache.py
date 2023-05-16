from __future__ import annotations

import datetime
import json
import os
import pickle
from functools import reduce
from typing import TypeVar

from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_int import Node_int
from lifetracking.graph.Time_interval import Time_interval, Time_resolution

T = TypeVar("T")


class Node_cache(Node[T]):
    def __init__(
        self,
        n0: Node[T],
        resolution: Time_resolution = Time_resolution.DAY,
        days_to_expire: float = 20,
    ) -> None:
        # Gives an error if the input node is constant (refactor this in the future)
        if isinstance(n0, Node_int):
            raise ValueError("The input node cannot be constant (to be improved)")

        super().__init__()
        self.n0 = n0
        self.path_dir_caches = r"C:\Temp\lifetracking\caches"
        self.resolution = resolution
        self.days_to_expire = days_to_expire
        if not os.path.isdir(self.path_dir_caches):
            os.makedirs(self.path_dir_caches)

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _operation(
        self, n0: T | PrefectFuture[T, Sync], t: Time_interval | None = None
    ) -> T | None:
        # TODO further ivnestigate this (In this case is empty (?))
        pass

    def _validate_cache(
        self,
        path_fil_cache,
        hash_node,
    ) -> bool:
        if not os.path.exists(path_fil_cache):
            return False
        try:
            with open(path_fil_cache) as f:
                cache_info = json.load(f)
                if Time_resolution(cache_info["resolution"]) != self.resolution:
                    return False
                if cache_info["hash_node"] != hash_node:
                    return False
                if (
                    datetime.datetime.now()
                    - datetime.datetime.fromisoformat(cache_info["date_creation"])
                ).days > self.days_to_expire:
                    return False
        except Exception:
            return False

        return True

    def _cache(
        self, n0: Node, t: Time_interval | None = None, context=None, prefect=False
    ):
        # Cache folder management
        hash_node = self.hash_tree(True)
        path_dir_cache = os.path.join(self.path_dir_caches, hash_node)
        if not os.path.isdir(path_dir_cache):
            os.makedirs(path_dir_cache)

        # Cache validation
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        cache_info = {
            "resolution": self.resolution.value,
            "hash_node": hash_node,
            "date_creation": datetime.datetime.now().isoformat(),
        }
        cache_is_valid = self._validate_cache(path_fil_cache, hash_node)
        if not cache_is_valid:
            with open(path_fil_cache, "w") as f:
                json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

        # Cache processing itself
        if t is None:
            raise ValueError("The time interval must be specified")

        gathered_data = []
        for i in t.iterate_over_interval(self.resolution):
            # We assess the name of the file by the time res
            if self.resolution == Time_resolution.DAY:
                day_name = i.start.strftime("%Y-%m-%d")
            else:
                raise NotImplementedError  # ¯\_(ツ)_/¯

            # Filename
            path_fil_cache_slice = os.path.join(path_dir_cache, f"{day_name}.pickle")

            # Cache exists 🤤
            if os.path.exists(path_fil_cache_slice) and cache_is_valid:
                with open(path_fil_cache_slice, "rb") as f:
                    data = pickle.load(f)
                    gathered_data.append(data)

            # Cache does not exist 🥺
            else:
                if not prefect:
                    data = n0._run_sequential(i, context)
                else:
                    data = n0._make_prefect_graph(i, context).result()
                gathered_data.append(data)
                with open(path_fil_cache_slice, "wb") as f:
                    pickle.dump(data, f)

        return reduce(lambda x, y: x + y, gathered_data)

    def _run_sequential(self, t=None, context=None) -> T | None:
        return self._cache(self.n0, t, context)

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        return self._cache(self.n0, t, context, prefect=True)
