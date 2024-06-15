from __future__ import annotations

import copy
import json
import pickle
import tempfile
from datetime import datetime, timezone
from enum import Enum
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node import Node
from lifetracking.graph.Node_int import Node_int
from lifetracking.graph.Time_interval import Time_interval, Time_resolution

if TYPE_CHECKING:
    from prefect.futures import PrefectFuture
    from prefect.utilities.asyncutils import Sync


# TODO_2: The dont_save_after_or_eq_resolution_interval cannot be set from Node_cache
# itself, how do I want to handle this?


def to_day(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


class Cache_type(Enum):
    NONE = 0
    SLICE = 1
    FULL = 2


class CacheData:
    """Pretty much only used by Node_cache"""

    def __init__(
        self,
        dir_cache: Path | str,
        hash_node: str,
        type_cache: Cache_type,
        resolution: Time_resolution,
        datatype: str | Any | None = None,
        covered_slices: None | list[Time_interval] = None,
        dont_save_after_or_eq_resolution_interval: bool = True,
    ):
        if isinstance(dir_cache, str):
            dir_cache = Path(dir_cache)
        assert isinstance(dir_cache, Path)
        if type_cache == Cache_type.SLICE:
            assert isinstance(covered_slices, list)
            assert len(covered_slices) > 0

        if (
            type_cache is not Cache_type.NONE
            and isinstance(datatype, str)
            and datatype == "Segments"
        ):
            datatype = Segments
        elif isinstance(datatype, str):
            raise NotImplementedError

        self.dir_cache = dir_cache
        self.hash_node = hash_node
        self.type_cache = type_cache
        self.resolution = resolution
        self.datatype = datatype
        self.covered_slices = covered_slices
        self.dont_save_after_or_eq_resolution_interval = (
            dont_save_after_or_eq_resolution_interval
        )

        self._data: None | dict = None
        if type_cache is not Cache_type.NONE:
            cache = json.loads((dir_cache / "cache.json").read_text())
            self._data = cache["data"]

    @staticmethod
    def load_from_dir(
        folder: Path,
        hash_node: str,
        resolution: Time_resolution,
        datatype: str | Any,
    ) -> CacheData:

        assert isinstance(folder, Path)
        assert isinstance(hash_node, str)

        if not (folder / "cache.json").exists():
            return CacheData(folder, hash_node, Cache_type.NONE, resolution)

        # TODO_2: Assert the hash_node is valid

        # TODO_2: Assert the resolution matches with the node

        # TODO_2: Assert that it hasnt expired

        c = json.loads((folder / "cache.json").read_text())

        if len(list(folder.iterdir())) != len(c["data"]) + 1:
            return CacheData(folder, hash_node, Cache_type.NONE, resolution, datatype)
        if c["type"] == Cache_type.FULL.value:
            return CacheData(folder, hash_node, Cache_type.FULL, resolution, datatype)
        if c["type"] == Cache_type.SLICE.value:
            return CacheData(
                folder,
                hash_node,
                Cache_type.SLICE,
                resolution,
                datatype,
                [Time_interval.from_json(x) for x in c["covered_slices"]],
            )
        raise NotImplementedError

    @property
    def data(self) -> dict[datetime, Any]:
        assert self._data is not None
        return self._data

    @data.setter
    def data(self, value: dict[datetime, Any]) -> None:
        self._data = value

    def update(
        self,
        data: Any,
        type_of_cache: Cache_type = Cache_type.FULL,
        slices: list[Time_interval] | None = None,
    ) -> None:

        assert isinstance(type_of_cache, Cache_type)
        if type_of_cache == Cache_type.SLICE:
            assert isinstance(slices, list)
            assert all(isinstance(x, Time_interval) for x in slices)
        else:
            assert slices is None
        if self.resolution != Time_resolution.DAY:
            print("For now I just have DAY! 😭")
            raise NotImplementedError

        self.dir_cache.mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        # if len(data.content) > 0:
        #     now = datetime.now(data.content[0].start.tzinfo)
        seg_truncated = Time_interval(now, now).truncate(self.resolution)
        # TODO_3: Do the tzinfo thingy
        if len(data.content) > 0:
            seg_truncated.start = seg_truncated.start.replace(
                tzinfo=data.content[0].start.tzinfo
            )
            seg_truncated.end = seg_truncated.end.replace(
                tzinfo=data.content[0].start.tzinfo
            )

        cache_info = {
            "date_creation": datetime.now(timezone.utc).isoformat(),
            "hash_node": self.hash_node,
            "type": type_of_cache.value,
            "resolution": self.resolution.value,
            "data": self.data if self._data is not None else {},
            "datatype": data.__class__.__name__,
        }

        def save_data(currentdata, data_to_save):
            # TODO_2: Depending on the data that comes, avoid doing a .pickle
            # TODO_2: Check the speed of https://github.com/ijl/orjson
            # TODO_2: Actually, maybe my node types should include a .save() and .load()
            e = self.dir_cache / f"{currentdata['creation_time']}.pickle"
            with e.open("wb") as f:
                pickle.dump(data_to_save, f)
            c = copy.copy(currentdata)  # REFACTOR: Huh...
            del c["creation_time"]
            c["creation_time"] = datetime.now(timezone.utc).isoformat()
            cache_info["data"][currentdata["creation_time"]] = c

        data_to_save = []
        currentdata: dict[str, Any] = {
            "data_count": 0,
            "creation_time": (
                to_day(data.content[0].start).strftime("%Y-%m-%d")
                if len(data.content) > 0
                else None
            ),
        }
        for seg in data.content:

            if self.dont_save_after_or_eq_resolution_interval:
                if seg in seg_truncated:
                    continue
                if seg.start >= seg_truncated.end:
                    continue

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

        if (
            self.dont_save_after_or_eq_resolution_interval
            and type_of_cache == Cache_type.SLICE
        ):
            # TODO_3: Do the tzinfo thingy
            if isinstance(slices, list) and len(slices) > 0:
                seg_truncated.start = seg_truncated.start.replace(
                    tzinfo=slices[0].start.tzinfo
                )
                seg_truncated.end = seg_truncated.end.replace(
                    tzinfo=slices[0].start.tzinfo
                )

            slices = [x for x in slices if x.start < seg_truncated.start]  # Filter
            for i, s in enumerate(slices):  # Crop the slices
                if s.end > seg_truncated.start:
                    slices[i] = Time_interval(s.start, seg_truncated.start)

        # Saving the metadata
        self.data = cache_info["data"]
        if type_of_cache == Cache_type.SLICE:
            assert slices is not None
            cache_info["covered_slices"] = [x.to_json() for x in slices]
        with (self.dir_cache / "cache.json").open("w") as f:
            json.dump(cache_info, f, indent=4, default=str, sort_keys=True)

    def load_cache_all(self):

        # Load cache descriptor
        f = self.dir_cache / "cache.json"
        if not f.exists():
            msg = f"Cache file '{f}' does not exist"
            raise ValueError(msg)
        c = json.loads(f.read_text())

        to_return = []
        for k in c["data"]:
            with (self.dir_cache / f"{k}.pickle").open("rb") as f:
                to_return.append(pickle.load(f))

        if c["datatype"] == "Segments":
            a = reduce(lambda x, y: x + y, to_return)
            return Segments(a)

        raise NotImplementedError

    def load_cache_slice(self, t: Time_interval):

        # Load cache descriptor
        f = self.dir_cache / "cache.json"
        if not f.exists():
            msg = f"Cache file '{f}' does not exist"
            raise ValueError(msg)
        c = json.loads(f.read_text())

        to_return = []
        for k in c["data"]:
            d = datetime.fromisoformat(k)
            if self.resolution == Time_resolution.DAY:
                corresponding_time_interval = Time_interval(
                    d.replace(hour=0, minute=0, second=0, microsecond=0),
                    d.replace(hour=23, minute=59, second=59, microsecond=999999),
                )
            else:
                raise NotImplementedError

            # if t in corresponding_time_interval:
            if t.overlaps(corresponding_time_interval):
                with (self.dir_cache / f"{k}.pickle").open("rb") as f:
                    to_return.append(pickle.load(f))

        if c["datatype"] == "Segments":
            if len(to_return) == 0:
                return Segments([])
            return Segments(reduce(lambda x, y: x + y, to_return))

        raise NotImplementedError


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
        path_dir_caches: str | Path | None = None,
    ) -> None:
        assert isinstance(n0, Node)
        assert isinstance(resolution, Time_resolution)
        assert isinstance(days_to_expire, (int, float))

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

        if prefect:
            raise NotImplementedError  # pragma: no cover

        # Cache dir management
        hash_node = self.hash_tree()
        if self.name is not None:
            hash_node += "_" + self.name.replace(" ", "-")

        # 🍑 Load cache
        c = CacheData.load_from_dir(
            self.path_dir_caches,
            hash_node,
            self.resolution,
            n0.__class__.sub_type,
        )

        # 🍑 What to compute
        to_return: Any | None = None
        to_compute: Time_interval | str | list | None = None

        if False:
            pass  # I have OCD and I add this so that the next lines are all aligned :(
        elif c.type_cache == Cache_type.NONE and t is None:
            to_compute = "all"
        elif c.type_cache == Cache_type.NONE and isinstance(t, Time_interval):
            to_compute = t
        elif c.type_cache == Cache_type.FULL and t is None:
            to_return = c.load_cache_all()
        elif c.type_cache == Cache_type.FULL and isinstance(t, Time_interval):
            to_return = c.load_cache_slice(t)
        elif c.type_cache == Cache_type.SLICE and t is None:
            to_compute = "all"  # TODO_2: Optimize this
        elif c.type_cache == Cache_type.SLICE and isinstance(t, Time_interval):
            assert isinstance(c.covered_slices, list)
            overlap, to_compute = t.get_overlap_innerouter_list(c.covered_slices)
            gathered = [c.load_cache_slice(t_sub) for t_sub in overlap]
            if len(gathered) == 0:
                assert isinstance(c.datatype, type)
                to_return = c.datatype([])
            else:
                to_return = reduce(lambda x, y: x + y, gathered)
        else:  # pragma: no cover
            raise NotImplementedError  # pragma: no cover

        # 🍑 Compute missing data
        if (isinstance(to_compute, list) and to_compute == []) or to_compute is None:
            pass
        elif isinstance(to_compute, Time_interval):
            to_return = n0._run_sequential(t, context)
        elif all(isinstance(x, Time_interval) for x in to_compute):
            for t_sub in to_compute:
                o = n0._run_sequential(t_sub, context)
                if type(o) == type(to_return):
                    to_return += o  # type: ignore
        elif to_compute == "all":
            to_return = n0._run_sequential(t, context)
        else:  # pragma: no cover
            raise NotImplementedError  # pragma: no cover

        # 🍑 Update cache
        if to_compute is None:
            pass
        elif isinstance(to_compute, str) and to_compute == "all":
            c.update(to_return, Cache_type.FULL)
        elif isinstance(to_compute, Time_interval):
            c.update(to_return, Cache_type.SLICE, [t])
        elif c.type_cache == Cache_type.SLICE and isinstance(t, Time_interval):
            assert isinstance(c.covered_slices, list)
            new_slices = Time_interval.merge([*c.covered_slices, t])
            c.update(to_return, Cache_type.SLICE, new_slices)
        else:
            raise NotImplementedError

        # 🍑 Return data
        return to_return

    def _run_sequential(self, t=None, context=None) -> T | None:
        return self._operation(self.n0, t, context)

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        return self._operation(self.n0, t, context, prefect=True)

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        color: str | Callable[[None], str] | None = None,
        opacity: float | Callable[[None], float] = 1.0,
        hour_offset: float = 0,
    ):
        return self.n0.export_to_longcalendar(
            t=t,
            path_filename=path_filename,
            color=color,
            opacity=opacity,
            hour_offset=hour_offset,
        )
