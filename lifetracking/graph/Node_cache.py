from __future__ import annotations

import copy
import datetime
import json
import os
import pickle
from functools import reduce
from typing import Any, TypeVar

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
        days_to_expire: float = 20.0,
        path_dir_caches: str | None = None,
    ) -> None:
        # TODO Currently it gives an error if the input node is constant
        # (refactor this in the future pls)
        if isinstance(n0, Node_int):
            raise ValueError("The input node cannot be constant (to be improved)")

        super().__init__()
        self.n0 = n0
        self.resolution = resolution
        self.days_to_expire = days_to_expire
        self.path_dir_caches = path_dir_caches or r"C:\Temp\lifetracking\caches"
        if not os.path.isdir(self.path_dir_caches):
            os.makedirs(self.path_dir_caches)

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _operation(
        self,
        n0: Node,
        t: Time_interval | None = None,
        context: dict | None = None,
        prefect=False,
    ):
        # Cache folder management
        hash_node = self.hash_tree()
        path_dir_cache = os.path.join(self.path_dir_caches, hash_node)
        if not os.path.isdir(path_dir_cache):
            os.makedirs(path_dir_cache)

        # Cache validation
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        cache_is_valid = self._validate_cache(path_fil_cache, hash_node, t)

        # We make a cache
        if not cache_is_valid:
            return self._save_cache(
                t,
                n0,
                path_dir_cache,
                hash_node,
                context,
                prefect,
            )

        # We load what we can from a currently existing cache
        else:
            return self._load_cache(
                t,
                n0,
                path_dir_cache,
                hash_node,
                context,
                prefect,
            )

    def _run_sequential(self, t=None, context=None) -> T | None:
        return self._operation(self.n0, t, context)

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        return self._operation(self.n0, t, context, prefect=True)

    def _validate_cache(
        self,
        path_fil_cache,
        hash_node,
        t,
    ) -> bool:
        if not os.path.exists(path_fil_cache):
            return False
        try:
            with open(path_fil_cache) as f:
                cache_info = json.load(f)

                # Is the right resolution
                if Time_resolution(cache_info["resolution"]) != self.resolution:
                    return False

                # Is the right hash
                if cache_info["hash_node"] != hash_node:
                    return False

                # Hasn't expired
                if (
                    datetime.datetime.now()
                    - datetime.datetime.fromisoformat(cache_info["date_creation"])
                ).days > self.days_to_expire:
                    return False

                # Cache is a slice, but they ask for the full data
                if cache_info["type"] == "slice" and t is None:
                    return True

        except Exception:
            return False

        return True

    def _save_cache(
        self,
        t: Time_interval | None,
        n0: Node,
        path_dir_cache: str,
        hash_node: str,
        context: dict | None = None,
        prefect: bool = False,
    ):
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")

        cache_info = {
            "resolution": self.resolution.value,
            "hash_node": hash_node,
            "date_creation": datetime.datetime.now().isoformat(),
            "start": None,
            "end": None,
            "data": {},  # Per-resolution-data
            "type": None,
        }

        # We get the data
        if prefect is False:
            o = n0._run_sequential(t, context)
            if o is None:
                return None
        else:
            raise NotImplementedError

        # We save the data
        cache_info["start"] = o.min()
        cache_info["end"] = o.max()
        cache_info["type"] = "full" if t is None else "slice"
        cache_metadata: dict[str, dict] = {
            # "2023-01-01": {
            #     "creation_time" : "2023-05-21T00:00:00",
            #     "data_count" : 6,
            # }
        }
        if self.resolution == Time_resolution.DAY:
            data_to_save = []
            current_day: str = None  # type: ignore
            current_data: dict[str, Any] = {
                "creation_time": None,
                "data_count": None,
            }
            for na, aa in enumerate(o.content):
                if na == 0:
                    current_day = aa.start.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                    current_data["creation_time"] = datetime.datetime.now().isoformat()
                    current_data["data_count"] = 1
                    data_to_save.append(aa)
                    continue

                if (
                    aa.start.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                    == current_day
                ):
                    current_data["data_count"] += 1
                    data_to_save.append(aa)
                    continue
                else:
                    # Saving data in pickle & metadata
                    filename_slice = os.path.join(
                        path_dir_cache, f"{current_day.split('T')[0]}.pickle"
                    )
                    with open(filename_slice, "wb") as f:
                        pickle.dump(type(o)(data_to_save), f)
                    cache_metadata[current_day] = current_data

                    # We reset stuff
                    current_day = aa.start.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                    data_to_save = [aa]
                    current_data = {
                        "creation_time": datetime.datetime.now().isoformat(),
                        "data_count": 1,
                    }

            # "After for"
            if len(data_to_save) != 0:
                # Saving data in pickle & metadata
                filename_slice = os.path.join(
                    path_dir_cache, f"{current_day.split('T')[0]}.pickle"
                )
                with open(filename_slice, "wb") as f:
                    pickle.dump(type(o)(data_to_save), f)
                cache_metadata[current_day] = current_data

            # Saving the metadata
            cache_info["data"] = cache_metadata
            with open(path_fil_cache, "w") as f:
                json.dump(cache_info, f, indent=4, default=str, sort_keys=True)
        else:
            raise NotImplementedError

        return o

    def _slices_to_get_rename_this(
        self,
        input_slices: list[Time_interval],
        path_dir_cache: str,
    ):
        to_return = []
        for t_sub in input_slices:
            # TODO: This actually creates a data overlap
            t_sub_truncated = copy.copy(t_sub).truncate(self.resolution)
            for t_sub_sub in t_sub_truncated.iterate_over_interval(self.resolution):
                if self.resolution == Time_resolution.DAY:
                    # REFACTOR: Rename this bs name for the var
                    filename_slice_date = (
                        t_sub_sub.start.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        .isoformat()
                        .split("T")[0]
                    )
                    filename_slice = os.path.join(
                        path_dir_cache,
                        f"{filename_slice_date}.pickle",
                    )

                    # If the file does not exist, we recompute
                    if not os.path.exists(filename_slice):
                        # TODO: Recompute & save
                        continue
                    # If the file has old data, we recompute
                    if False:
                        # TODO: Recompute & save
                        pass
                    with open(filename_slice, "rb") as f:
                        data_slice = pickle.load(f)[t_sub_sub]
                    # to_return.extend(data_slice.content)
                    to_return.append(data_slice)
                else:
                    raise NotImplementedError
        return to_return

    def _slices_to_compute_rename_this(
        self,
        input_slices: list[Time_interval],
        path_dir_cache: str,
        prefect: bool,
        n0: Node,
        cache_info: dict[str, Any],
        context: dict | None = None,
    ):
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        to_return = []

        # Then, slices to compute
        # TODO: This is a for used with 0, 1 or 2 elements... maybe we can optimize it?
        for t_sub in input_slices:
            # TODO: This actually creates a data overlap
            t_sub_truncated = copy.copy(t_sub).truncate(self.resolution)
            for t_sub_sub in t_sub_truncated.iterate_over_interval(self.resolution):
                if not prefect:
                    # This redundancy is a bit dumb, I need to think about it
                    o = n0._run_sequential(t_sub_sub, context)  # [t_sub]
                    if o is None:
                        raise ValueError("Check this!")
                    o = o[t_sub]
                    # TODO: I think it should be this one, but test fail
                    # o = o[t_sub_sub]
                else:
                    raise NotImplementedError
                if o is None:
                    continue
                to_return.append(o)

                # Saving data in pickle
                if len(o) > 0:
                    filename_slice = os.path.join(
                        path_dir_cache,
                        t_sub_sub.start.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        .isoformat()
                        .split("T")[0]
                        + ".pickle",
                    )
                    with open(filename_slice, "wb") as f:
                        pickle.dump(o, f)
                    cache_info["data"][
                        t_sub_sub.start.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat()
                    ] = {
                        "creation_time": datetime.datetime.now().isoformat(),
                        "data_count": len(o),
                    }

            # Saving the metadata
            # cache_info["start"] = # TODO: Finish this
            # cache_info["end"] = # TODO: Finish this
            with open(path_fil_cache, "w") as f:
                json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

        return to_return

    def _load_cache(
        self,
        t: Time_interval | None,
        n0: Node,
        path_dir_cache: str,
        hash_node: str,
        context: dict | None = None,
        prefect: bool = False,
    ):
        to_return = []

        # Cache data loading
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        with open(path_fil_cache) as f:
            cache_info = json.load(f)
        t_cache = Time_interval(
            datetime.datetime.fromisoformat(cache_info["start"]),
            datetime.datetime.fromisoformat(cache_info["end"]),
        )
        t = t_cache if t is None else t

        # Slices gonna slice
        slices_to_get, slices_to_compute = t_cache.get_overlap_innerouter(t)
        to_return += self._slices_to_get_rename_this(
            slices_to_get,
            path_dir_cache,
        )
        to_return += self._slices_to_compute_rename_this(
            slices_to_compute,
            path_dir_cache,
            prefect,
            n0,
            cache_info,
            context,
        )

        return reduce(lambda x, y: x + y, to_return)
