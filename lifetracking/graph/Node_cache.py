from __future__ import annotations

import copy
import json
import os
import pickle
import tempfile
from abc import ABC
from datetime import datetime, timezone
from enum import Enum
from functools import reduce
from pathlib import Path
from typing import Any, Callable, TypeVar

import pandas as pd
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node import Node
from lifetracking.graph.Node_int import Node_int
from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def to_day(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


class Cache_type(Enum):
    NONE = 0
    SLICE = 1
    FULL = 2


class Cache_AAA(ABC):

    def __init__(
        self,
        dir: Path,
        hash_node: str,
        type: Cache_type,
        resolution: Time_resolution,
        # start: datetime | None = None,
        # end: datetime | None = None,
        covered_slices: None | list[Time_interval] = None,
    ):
        assert isinstance(dir, Path)
        # assert isinstance(start, datetime) or start is None
        # assert isinstance(end, datetime) or end is None

        if type == Cache_type.SLICE:
            # assert start is not None
            # assert end is not None
            assert isinstance(covered_slices, list)
            assert len(covered_slices) > 0

        self.dir = dir
        self.hash_node = hash_node
        self.type = type
        self.resolution = resolution
        # self.start = start
        # self.end = end
        self.covered_slices = covered_slices

    @staticmethod
    def load_from_dir(
        dir: Path,
        hash_node: str,
        resolution: Time_resolution,
        # ) -> Cache_AAA_slice | Cache_AAA_full | None:
    ) -> Cache_AAA:

        assert isinstance(dir, Path)
        assert isinstance(hash_node, str)

        if not (dir / "cache.json").exists():
            return Cache_AAA(dir, hash_node, Cache_type.NONE, resolution)

        # TODO_2: Assert the hash_node is valid

        # TODO_2: Assert the resolution matches with the node

        # TODO_2: Assert that it hasnt expired

        c = json.loads((dir / "cache.json").read_text())

        if len(list(dir.iterdir())) != len(c["data"]) + 1:
            print("Cache is invalid...")
            return Cache_AAA(dir, hash_node, Cache_type.NONE, resolution)

        if c["type"] == Cache_type.FULL.value:
            return Cache_AAA(dir, hash_node, Cache_type.FULL, resolution)
        elif c["type"] == Cache_type.SLICE.value:
            # raise NotImplementedError
            return Cache_AAA(
                dir,
                hash_node,
                Cache_type.SLICE,
                resolution,
                covered_slices=[
                    Time_interval.from_json(x)
                    for x in c["covered_slices"]
                    # Time_interval(
                    #     datetime.fromisoformat(c["start"]),
                    #     datetime.fromisoformat(c["end"]),
                    # )
                ],
            )
        else:
            raise NotImplementedError

        raise NotImplementedError

    @property
    def data(self) -> dict[datetime, Any]:
        raise NotImplementedError

    def update(
        self,
        data: Any,
        type_of_cache: Cache_type = Cache_type.FULL,
        # slice: Time_interval | None = None,
        slices: list[Time_interval] | None = None,
    ) -> None:

        assert isinstance(type_of_cache, Cache_type)
        if type_of_cache == Cache_type.SLICE:
            assert isinstance(slices, list)
            assert all(isinstance(x, Time_interval) for x in slices)
        else:
            assert slices is None

        self.dir.mkdir(parents=True, exist_ok=True)

        if not self.resolution == Time_resolution.DAY:
            print("For now I just have DAY!")
            raise NotImplementedError
        if len(data.content) == 0:
            print("Make a special case for this!")
            raise NotImplementedError

        cache_info = {
            "date_creation": datetime.now(timezone.utc).isoformat(),
            "hash_node": self.hash_node,
            "type": type_of_cache.value,
            "resolution": self.resolution.value,
            "data": {},
            "datatype": data.__class__.__name__,
        }

        def save_data(currentdata, data_to_save):
            # TODO_2: Depending on the data that comes, avoid doing a .pickle
            # TODO_2: Check the speed of https://github.com/ijl/orjson
            # TODO_2: Actually, maybe my node types should include a .save() and .load()
            e = self.dir / f"{currentdata['creation_time']}.pickle"
            with open(e, "wb") as f:
                pickle.dump(data_to_save, f)
            c = copy.copy(currentdata)  # REFACTOR: Huh...
            del c["creation_time"]
            c["creation_time"] = datetime.now(timezone.utc).isoformat()
            cache_info["data"][currentdata["creation_time"]] = c

        data_to_save = []
        currentdata: dict[str, Any] = {
            "data_count": 0,
            "creation_time": to_day(data.content[0].start).strftime("%Y-%m-%d"),
        }
        for seg in data.content:
            if to_day(seg.start).strftime("%Y-%m-%d") == currentdata["creation_time"]:
                currentdata["data_count"] += 1
                data_to_save.append(seg)
                continue

            # Saving data in pickle & metadata
            save_data(currentdata, data_to_save)

            # We reset stuff
            data_to_save = [seg]
            currentdata = {
                "data_count": 1,
                "creation_time": to_day(seg.start).strftime("%Y-%m-%d"),
            }

        if data_to_save:
            save_data(currentdata, data_to_save)

        # Saving the metadata
        if type_of_cache == Cache_type.SLICE:
            assert slices is not None
            # cache_info["start"] = slice.start
            # cache_info["end"] = slice.end
            cache_info["covered_slices"] = [x.to_json() for x in slices]
        with open(self.dir / "cache.json", "w") as f:
            json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

    def load_cache_all(self):

        # Load cache descriptor
        f = self.dir / "cache.json"
        if not f.exists():
            msg = f"Cache file '{f}' does not exist"
            raise ValueError(msg)
        c = json.loads(f.read_text())

        to_return = []
        for k, v in c["data"].items():
            with open(self.dir / f"{k}.pickle", "rb") as f:
                to_return.append(pickle.load(f))

        if c["datatype"] == "Segments":
            a = reduce(lambda x, y: x + y, to_return)
            return Segments(a)

        raise NotImplementedError

    def load_cache_slice(self, t: Time_interval):

        # Load cache descriptor
        f = self.dir / "cache.json"
        if not f.exists():
            msg = f"Cache file '{f}' does not exist"
            raise ValueError(msg)
        c = json.loads(f.read_text())

        to_return = []
        for k, v in c["data"].items():
            d = datetime.fromisoformat(k)
            if self.resolution == Time_resolution.DAY:
                corresponding_time_interval = Time_interval(
                    d.replace(hour=0, minute=0, second=0, microsecond=0),
                    d.replace(hour=23, minute=59, second=59, microsecond=999999),
                )
            else:
                raise NotImplementedError

            if t in corresponding_time_interval:

                with open(self.dir / f"{k}.pickle", "rb") as f:
                    to_return.append(pickle.load(f))

                p = 0
            p = 0

        if c["datatype"] == "Segments":
            a = reduce(lambda x, y: x + y, to_return)
            return Segments(a)

        raise NotImplementedError


# class Cache_AAA_slice(Cache_AAA):
#     def __init__(self, dir: Path, hash_node: str):
#         super().__init__(dir, hash_node)
#         self.start = None
#         self.end = None


# class Cache_AAA_full(Cache_AAA):
#     def __init__(self, dir: Path, hash_node: str):
#         super().__init__(dir, hash_node)


T = TypeVar("T")


# REFACTOR: Remove the .strftime("%Y-%m-%d %H:%M:%S") thing
# TODO_1: all .now() should include timezone I guess


class Node_cache(Node[T]):
    """Cache for nodes, meant to be subclassed for type hints!!

    Small note, "resolution" is stored as an int, but its an enum
    """

    def __init__(
        self,
        n0: Node[T],
        resolution: Time_resolution = Time_resolution.DAY,
        days_to_expire: float = 20.0,
        path_dir_caches: str | Path | None = None,
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

        hash_node = self.hash_tree()
        if self.name is not None:
            hash_node += "_" + self.name.replace(" ", "-")

        path_dir_caches = path_dir_caches or tempfile.gettempdir()
        if isinstance(path_dir_caches, str):
            path_dir_caches = Path(path_dir_caches)
        self.path_dir_caches = path_dir_caches / hash_node
        self.path_dir_caches.mkdir(parents=True, exist_ok=True)

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

        # assert isinstance(self.path_dir_caches, Path)  # TODO_2: Remove in the future
        # dir_cache = self.path_dir_caches / hash_node
        # dir_cache.mkdir(parents=True, exist_ok=True)

        # TODO_2: Actually, it's pretty dumb to pass
        # - dir_cache since it's self.path_dir_caches / hash_node
        # Like, why not get it via self.path_dir_caches / self.hash_tree()?

        # We have nothing? -> Make cache
        # We have something? -> Load what we can and recompute anything missing

        # if not self._is_cache_valid(hash_node, t):
        #     return self._save_cache(t, n0, hash_node, context, prefect)
        # return self._load_cache(t, n0, hash_node, context, prefect)

        # Get data
        # (takes into account expiration date!!)
        # valid_caches, backuptype : tuple[list, Cache_type] = self._get_valid_caches(hash_node, t)

        # 🍑 Load cache
        c = Cache_AAA.load_from_dir(self.path_dir_caches, hash_node, self.resolution)

        # 🍑 What to compute
        to_return: Any | None = None
        to_compute: Time_interval | str | list | None = None

        if c.type == Cache_type.NONE and t is None:
            to_compute = "all"
        elif c.type == Cache_type.NONE and isinstance(t, Time_interval):
            to_compute = t
        elif c.type == Cache_type.FULL and t is None:
            to_return = c.load_cache_all()
            p = 0
        elif c.type == Cache_type.SLICE and isinstance(t, Time_interval):
            # to_return = ... lo ya existente
            # to_compute = ... lo que falta

            # assert isinstance(c.start, datetime)
            # assert isinstance(c.end, datetime)
            # c_time_interval = Time_interval(c.start, c.end)
            # overlapping, non_overlapping = c_time_interval.get_overlap_innerouter(t)
            # if len(overlapping) == 1:
            #     to_return = c.load_cache_slice(overlapping[0])
            # else:
            #     raise NotImplementedError
            # to_compute = non_overlapping

            assert isinstance(c.covered_slices, list)
            overlap, non_overlap = t.get_overlap_innerouter_list(c.covered_slices)
            to_return_pre = overlap
            to_compute = non_overlap

            to_return = []
            for t_sub in to_return_pre:
                to_return.append(c.load_cache_slice(t_sub))
            to_return = reduce(lambda x, y: x + y, to_return)

            p = 0

        else:
            raise NotImplementedError

        # # What to compute
        # to_return, to_compute = []
        # if backuptype == Cache_type.NONE:
        #     to_compute = [t]
        # elif backuptype == Cache_type.FULL and t is None:
        #     to_return = valid_caches
        # elif backuptype == Cache_type.FULL and t is not None:
        #     to_return = valid_caches[t]
        # elif backuptype == Cache_type.SLICE and t is None:
        #     to_return = valid_caches
        #     ...
        #     to_compute = [left, right]
        # elif backuptype == Cache_type.SLICE and t is not None:
        #     ...
        #     to_return = overlap
        #     to_compute = [non_overlap_left, non_overlap_right]
        # else:
        #     raise NotImplementedError

        # 🍑 Compute missing data
        if isinstance(to_compute, str) and to_compute == "all":
            to_return = n0._run_sequential(t, context)
            p = 0
        elif isinstance(to_compute, Time_interval):
            to_return = n0._run_sequential(t, context)
            p = 0
        elif to_compute is None:
            p = 0
        elif to_compute == []:
            p = 0
        # TODO_1: Make this into a TypeGuard when I upgrade to python 3.10 or 3.11
        elif all(isinstance(x, Time_interval) for x in to_compute):
            for t_sub in to_compute:
                o = n0._run_sequential(t_sub, context)
                if type(o) == type(to_return):
                    to_return += o
        else:
            raise NotImplementedError

        # 🍑 Update cache
        if isinstance(to_compute, str) and to_compute == "all":
            c.update(to_return, Cache_type.FULL)
        elif isinstance(to_compute, Time_interval):
            c.update(to_return, Cache_type.SLICE, [t])
        elif to_compute is None:
            p = 0
        elif c.type == Cache_type.SLICE and isinstance(t, Time_interval):
            assert isinstance(c.covered_slices, list)
            new_slices = Time_interval.merge(c.covered_slices + [t])
            p = 0
            # c.covered_slices = new_slices
            c.update(to_return, Cache_type.SLICE, new_slices)

            # s, e = c.start, c.end
            # assert isinstance(s, datetime)
            # assert isinstance(e, datetime)
            # for t_sub in to_compute:
            #     if isinstance(t_sub, Time_interval):
            #         s = min(t_sub.start, s)
            #         e = max(t_sub.end, e)
            #     else:
            #         raise NotImplementedError
            # c.update(to_return, Cache_type.SLICE, Time_interval(s, e))

            # # We expand the currenrly covered slices with the ones we needed to compute
            # c.covered_slices

        else:
            raise NotImplementedError

        # 🍑 Return data
        # return to_return + to_compute ??
        return to_return

    def _run_sequential(self, t=None, context=None) -> T | None:
        return self._operation(self.n0, t, context)

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        return self._operation(self.n0, t, context, prefect=True)

    # Actual custom methods

    def _is_cache_valid(self, hash_node: str, t: Time_interval | None) -> bool:
        """Can we use this cache?"""

        assert self.resolution is not None
        assert isinstance(hash_node, str)
        assert isinstance(t, Time_interval) or t is None

        file_cache = self.path_dir_caches / "cache.json"

        if not file_cache.exists():
            return False

        # Load data
        try:
            with open(file_cache) as f:
                cache_info = json.load(f)
        except Exception:
            return False

        if not isinstance(cache_info, dict):
            return False

        # Is the right resolution
        if Time_resolution(cache_info["resolution"]) != self.resolution:
            return False

        # Is the right hash
        if cache_info["hash_node"] != hash_node:
            return False

        # Hasn't expired
        if (
            datetime.now() - datetime.fromisoformat(cache_info["date_creation"])
        ).days > self.days_to_expire:
            return False

        # Cache is a slice, but they ask for the full data
        if cache_info["type"] == "slice" and t is None:
            return True

        return True

    def _save_cache(
        self,
        t: Time_interval | None,
        n0: Node,
        hash_node: str,
        context: dict | None = None,
        prefect: bool = False,
    ):
        assert isinstance(hash_node, str)
        assert isinstance(prefect, bool)

        path_fil_cache = self.path_dir_caches / "cache.json"

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

        # REFACTOR: Maybe we should have a class for this `cache_info` thing?

        # We save the data
        cache_info = {
            "resolution": self.resolution.value,
            "hash_node": hash_node,
            "date_creation": datetime.now().isoformat(),
            "start": o.min(),
            "end": o.max(),
            "data": {},  # Per-resolution-data
            "type": "full" if t is None else "slice",
        }
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
                    current_data["creation_time"] = datetime.now().isoformat()
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
                filename_slice = (
                    self.path_dir_caches / f"{current_day.split('T')[0]}.pickle"
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
                    "creation_time": datetime.now().isoformat(),
                    "data_count": 1,
                }

            # "After for"
            if len(data_to_save) != 0:
                # Saving data in pickle & metadata
                filename_slice = (
                    self.path_dir_caches / f"{current_day.split('T')[0]}.pickle"
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
                        "creation_time": datetime.now().isoformat(),
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
                start_time = datetime.fromisoformat(start_time)
            min_pre = min(pre)
            if isinstance(min_pre, pd.Timestamp):
                min_pre = min_pre.to_pydatetime()
            cache_info["start"] = min(min_pre, start_time)

        pre = [max(x).end for x in generated_data if len(x.content) > 0]
        if len(pre) > 0:
            time_end = cache_info["end"]
            if isinstance(time_end, str):
                time_end = datetime.fromisoformat(time_end)
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
        hash_node: str,
        context: dict | None = None,
        prefect: bool = False,
    ) -> T | None:

        assert isinstance(hash_node, str)
        assert isinstance(prefect, bool)

        to_return: list[T] = []

        # Cache data loading
        path_fil_cache = self.path_dir_caches / "cache.json"
        cache_info = json.loads(path_fil_cache.read_text())
        t_cache = Time_interval(
            datetime.fromisoformat(cache_info["start"]),
            datetime.fromisoformat(cache_info["end"]),
        )

        # If we ask for everything, but we have a segment we compute left and right
        #  <------| [data] |------>
        if t is None and cache_info["type"] == "slice":
            to_return += self._gather_existing_slices(
                [t_cache], str(self.path_dir_caches)
            )

            data_left = n0._run_sequential(
                Time_interval(
                    datetime.min,
                    datetime.fromisoformat(cache_info["start"]),
                ),
                context,
            )
            if data_left is not None:
                to_return.append(data_left)

            data_right = n0._run_sequential(
                Time_interval(
                    datetime.fromisoformat(cache_info["end"]),
                    datetime.max,
                ),
                context,
            )
            if data_right is not None:
                to_return.append(data_right)

        else:
            t = t_cache if t is None else t
            # Slices gonna slice
            slices_to_get, slices_to_compute = t_cache.get_overlap_innerouter(t)

            to_return += self._gather_existing_slices(
                slices_to_get,
                str(self.path_dir_caches),
            )
            to_return += self._compute_nonexistent_slices(
                slices_to_compute,
                str(self.path_dir_caches),
                prefect,
                n0,
                cache_info,
                context,
            )

        self._update_metadata_bounds(to_return, str(self.path_dir_caches), t)

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

    # Complete rework
