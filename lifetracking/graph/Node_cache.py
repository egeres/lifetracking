from __future__ import annotations

import copy
import datetime
import json
import os
import pickle
from functools import reduce
from typing import Any, Callable, TypeVar

import pandas as pd
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_int import Node_int
from lifetracking.graph.Time_interval import Time_interval, Time_resolution

T = TypeVar("T")


class Node_cache(Node[T]):
    """Cache for nodes, meant to be subclassed for type hints!!

    Small note, "resolution" is stored as an int, but its an enum
    """

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
            msg = "The input node cannot be constant (to be improved)"
            raise TypeError(msg)

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
    ) -> T | None:
        # Cache dir management
        hash_node = self.hash_tree()
        if self.name is not None:
            hash_node += "_" + self.name.replace(" ", "-")
        path_dir_cache = os.path.join(self.path_dir_caches, hash_node)
        if not os.path.isdir(path_dir_cache):
            os.makedirs(path_dir_cache)

        # Cache validation
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        cache_is_valid = self._is_cache_valid(path_fil_cache, hash_node, t)

        # We have nothing? -> Make cache
        if not cache_is_valid:
            return self._save_cache(
                t,
                n0,
                path_dir_cache,
                hash_node,
                context,
                prefect,
            )

        # We have something? -> Load what we can and recompute anything missing
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

    def _is_cache_valid(
        self,
        path_fil_cache: str,
        hash_node: str,
        t: Time_interval | None,
    ) -> bool:
        """Can we use this cache?"""

        assert self.resolution is not None
        assert isinstance(path_fil_cache, str)
        assert isinstance(hash_node, str)
        assert isinstance(t, Time_interval) or t is None

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

        # No data, no cache saving!
        if len(o.content) == 0:
            return o

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

    def _gather_existing_slices(
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

    def _compute_nonexistent_slices(
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
                        msg = "Check this!"
                        raise ValueError(msg)
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

            with open(path_fil_cache, "w") as f:
                json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

        return to_return

    def _update_metadata_bounds(
        self,
        generated_data: list[T],
        path_dir_cache: str,
        t: Time_interval,
    ) -> None:
        assert isinstance(generated_data, list)
        assert isinstance(path_dir_cache, str)

        if len(generated_data) == 0:
            return

        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        with open(path_fil_cache) as f:
            cache_info = json.load(f)

        pre = [min(x).start for x in generated_data if len(x.content) > 0]
        if len(pre) > 0:
            start_time = cache_info["start"]
            if isinstance(start_time, str):
                start_time = datetime.datetime.fromisoformat(start_time)
            min_pre = min(pre)
            if isinstance(min_pre, pd.Timestamp):
                min_pre = min_pre.to_pydatetime()
            cache_info["start"] = min(min_pre, start_time)

        pre = [max(x).end for x in generated_data if len(x.content) > 0]
        if len(pre) > 0:
            time_end = cache_info["end"]
            if isinstance(time_end, str):
                time_end = datetime.datetime.fromisoformat(time_end)
            max_pre = max(pre)
            if isinstance(max_pre, pd.Timestamp):
                max_pre = max_pre.to_pydatetime()
            cache_info["end"] = max(max_pre, time_end)

        if t is None:
            cache_info["type"] = "full"

        with open(path_fil_cache, "w") as f:
            json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

    def _load_cache(
        self,
        t: Time_interval | None,
        n0: Node,
        path_dir_cache: str,
        hash_node: str,
        context: dict | None = None,
        prefect: bool = False,
    ) -> T | None:
        to_return: list[T] = []

        # Cache data loading
        path_fil_cache = os.path.join(path_dir_cache, "cache.json")
        with open(path_fil_cache) as f:
            cache_info = json.load(f)
        t_cache = Time_interval(
            datetime.datetime.fromisoformat(cache_info["start"]),
            datetime.datetime.fromisoformat(cache_info["end"]),
        )

        if t is None and cache_info["type"] == "slice":
            to_return += self._gather_existing_slices(
                [t_cache],
                path_dir_cache,
            )

            o_0 = n0._run_sequential(
                Time_interval(
                    datetime.datetime.min,
                    datetime.datetime.fromisoformat(cache_info["start"]),
                ),
                context,
            )
            if o_0 is not None:
                to_return.append(o_0)
            o_1 = n0._run_sequential(
                Time_interval(
                    datetime.datetime.fromisoformat(cache_info["end"]),
                    datetime.datetime.max,
                ),
                context,
            )
            if o_1 is not None:
                to_return.append(o_1)

        else:
            t = t_cache if t is None else t
            # Slices gonna slice
            slices_to_get, slices_to_compute = t_cache.get_overlap_innerouter(t)

            to_return += self._gather_existing_slices(
                slices_to_get,
                path_dir_cache,
            )
            to_return += self._compute_nonexistent_slices(
                slices_to_compute,
                path_dir_cache,
                prefect,
                n0,
                cache_info,
                context,
            )

        self._update_metadata_bounds(to_return, path_dir_cache, t)

        if len(to_return) == 0:
            return None

        # TODO_2: Fix this weird issue with the type after project is python 3.11 (?)
        return reduce(lambda x, y: x + y, to_return)  # type: ignore

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        color: str | Callable[[None], str] | None = None,
        opacity: float | Callable[[None], float] = 1.0,
        hour_offset: float = 0,
    ):
        raise NotImplementedError
