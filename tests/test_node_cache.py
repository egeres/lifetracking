from __future__ import annotations

import io
import json
import os
import pickle
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from dateutil.parser import parse
from freezegun import freeze_time

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node_cache import Node_cache
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def count_files_ending_with_x(path: Path, suffix: str) -> int:
    return len([x for x in path.iterdir() if x.suffix.endswith(suffix)])


# @pytest.fixture
def make_node_cache(path_dir_caches: Path):
    assert path_dir_caches.exists()

    # Data generation
    a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
    b = Segments(
        [
            a - timedelta(days=0),
            a - timedelta(days=1),
            a - timedelta(days=2),
            a - timedelta(days=3),
            a - timedelta(days=4),
            a - timedelta(days=5),
        ]
    )
    c = Node_segments_generate(b)
    return Node_cache(c, path_dir_caches=path_dir_caches)


def test_node_cache_0():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        # 🔮 We run this with t=None
        path_dir_caches = Path(path_dir_caches)
        node_cache = make_node_cache(path_dir_caches)
        o = node_cache.run()
        # 🥭 Evaluate: Data
        assert isinstance(o, Segments)
        assert len(o) == len(node_cache.children[0].value)

        # 🥭 Evaluate: Cache folder
        assert len(list(path_dir_caches.iterdir())) == 1
        dirsubcache = next(path_dir_caches.iterdir())  # First dir
        assert len(list(dirsubcache.iterdir())) == len(node_cache.children[0].value) + 1
        assert count_files_ending_with_x(dirsubcache, ".json") == 1

        # 🔮 We run this with t=None, again
        o = node_cache.run()
        # 🥭 Evaluate: Data
        assert isinstance(o, Segments)
        assert len(o) == len(node_cache.children[0].value)


def test_node_cache_1():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        # 🔮 We run this with t=something
        path_dir_caches = Path(path_dir_caches)
        node_cache = make_node_cache(path_dir_caches)
        t = Time_interval(datetime(2024, 3, 5, 12, 0), datetime(2024, 3, 5, 13, 0))
        o = node_cache.run(t=t)
        # 🥭 Evaluate: Data
        assert isinstance(o, Segments)
        assert len(o) == 1

        # 🥭 Evaluate: Cache folder
        assert len(list(path_dir_caches.iterdir())) == 1
        dirsubcache = next(path_dir_caches.iterdir())  # First dir
        assert len(list(dirsubcache.iterdir())) < len(node_cache.children[0].value) + 1
        assert count_files_ending_with_x(dirsubcache, ".json") == 1

        # 🔮 We run this with t=something
        o = node_cache.run(t=t)
        # 🥭 Evaluate: Data
        assert isinstance(o, Segments)
        assert len(o) == len(node_cache.children[0].value)


def test_node_cache_nodata():
    """Just checking it doesn't crash"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(days=100),
                a - timedelta(days=101),
                a - timedelta(days=102),
                a - timedelta(days=103),
                a - timedelta(days=104),
                a - timedelta(days=105),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=Time_interval(a.start - timedelta(days=2), a.end))
        assert isinstance(o, Segments)
        assert len(o) == 0


def test_node_cache_save_tisnone():
    """A pipeline for `Segments` gets cached, then queried with t=None"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(hours=0.0),
                a - timedelta(hours=0.5),
                a - timedelta(hours=1.3),
                a - timedelta(hours=950.0),  # Belongs to another day!
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=None)  # ✨

        # 🍑 Validation for the data
        assert o is not None
        assert len(o) == len(b)

        # 🍑 Validation for the cache
        assert len(list(path_dir_caches.iterdir())) == 1
        dir_specific_cache = next(path_dir_caches.iterdir())  # First dir
        assert len(os.listdir(dir_specific_cache)) == 3
        assert count_files_ending_with_x(dir_specific_cache, ".json") == 1
        assert count_files_ending_with_x(dir_specific_cache, ".pickle") == 2

        # 🍑 Validation for the json file
        path_fil_json = next(
            dir_specific_cache / x
            for x in dir_specific_cache.iterdir()
            if x.suffix == ".json"
        )
        data = json.loads(path_fil_json.read_text())
        assert data["start"] == b.min().strftime("%Y-%m-%d %H:%M:%S")
        assert data["end"] == b.max().strftime("%Y-%m-%d %H:%M:%S")
        assert data["hash_node"] == d.hash_tree()
        assert data["resolution"] == Time_resolution.DAY.value
        assert data["type"] == "full"

        # 🍑 Validation for the pickle files
        all_data_count = 0
        for f in (
            dir_specific_cache / x
            for x in dir_specific_cache.iterdir()
            if x.suffix == ".pickle"
        ):
            data = pickle.load(io.BytesIO(f.read_bytes()))
            assert isinstance(data, Segments)
            assert len(data) < len(b)
            all_data_count += len(data)
        assert all_data_count == len(b)


def test_node_cache_save_tissomething():
    """We first get data with an offseted week. Then with 1 month"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(days=0),
                a - timedelta(days=1),
                a - timedelta(days=2),
                a - timedelta(days=3),
                a - timedelta(days=4),
                a - timedelta(days=5),
                a - timedelta(days=5.1),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)

        # 🍑 Step 1: First data gathering ----------------------------------------------
        o = d.run(Time_interval(a.start - timedelta(days=9), a.end - timedelta(days=3)))

        # Data validation
        assert o is not None
        assert len(o) == 4

        # Cache folder validation
        dir_specific_cache = next(path_dir_caches.iterdir())  # First dir
        assert len(os.listdir(dir_specific_cache)) == 4

        # cache.json validation
        path_fil_json = next(
            dir_specific_cache / x
            for x in dir_specific_cache.iterdir()
            if x.suffix == ".json"
        )
        data = json.loads(path_fil_json.read_text())
        # assert data["start"] == b.min().strftime("%Y-%m-%d %H:%M:%S") # 🤔
        # assert data["end"] == b.max().strftime("%Y-%m-%d %H:%M:%S") # 🤔
        assert data["hash_node"] == d.hash_tree()
        assert data["resolution"] == Time_resolution.DAY.value
        assert data["type"] == "slice"
        # We assert all data_creation_dates are equal
        data_creation_dates = [
            v["creation_time"].split(".")[0] for k, v in data["data"].items()
        ]
        assert len(set(data_creation_dates)) == 1
        # They all sum correctly, e.g: "data_count": 2
        data_creation_dates = [int(v["data_count"]) for k, v in data["data"].items()]
        assert sum(data_creation_dates) == len(o)

        # 🍑 Step 2: New data gathering ------------------------------------------------
        freezer = freeze_time(datetime.now() + timedelta(days=1))  # Tweaks .now()
        freezer.start()
        o = d.run(Time_interval(a.start - timedelta(days=30), a.end))

        # Validation: data
        assert o is not None
        assert len(o) == 7

        # Validation: cache folder
        dir_specific_cache = next(path_dir_caches.iterdir())  # First dir
        assert len(os.listdir(dir_specific_cache)) == 7

        # Validation: cache.json
        path_fil_json = next(
            dir_specific_cache / x
            for x in dir_specific_cache.iterdir()
            if x.suffix == ".json"
        )
        data = json.loads(path_fil_json.read_text())
        # assert data["start"] == b.min().strftime("%Y-%m-%d %H:%M:%S") # 🤔
        # assert data["end"] == b.max().strftime("%Y-%m-%d %H:%M:%S") # 🤔
        assert data["hash_node"] == d.hash_tree()
        assert data["resolution"] == Time_resolution.DAY.value
        assert data["type"] == "slice"
        # We assert all data_creation_dates are equal
        data_creation_dates = [
            v["creation_time"].split(".")[0] for k, v in data["data"].items()
        ]
        assert len(set(data_creation_dates)) == 2
        # They all sum correctly, e.g: "data_count": 2
        data_creation_dates = [int(v["data_count"]) for k, v in data["data"].items()]
        assert sum(data_creation_dates) == len(o)
        freezer.stop()


def test_node_cache_load_tisnone():  # Difficult!
    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(hours=0.0),
                a - timedelta(hours=0.5),
                a - timedelta(hours=1.3),
                a - timedelta(hours=950.0),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        _ = d.run(t=None)

        # Running it again should yield the same data ✨
        o = d.run(t=None)
        assert o is not None
        assert isinstance(o, Segments)
        assert len(o) == len(b)
        assert o.min() == b.min()
        assert o.max() == b.max()


def test_node_cache_load_tissomething():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(hours=0.0),
                a - timedelta(hours=0.5),
                a - timedelta(hours=1.3),
                a - timedelta(hours=950.0),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        _ = d.run(t=None)

        # The part that we're actually interested in ✨
        t = Time_interval(a.start - timedelta(days=2), a.end + timedelta(days=2))
        o = d.run(t)
        assert o is not None
        assert isinstance(o, Segments)
        assert len(o) == 3
        assert len(o[t]) == 3


def test_node_cache_dataisextended():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 5, 12, 0, 0), datetime(2024, 3, 5, 13, 0, 0))
        b = Segments(
            [
                a - timedelta(days=0),
                a - timedelta(days=1),
                a - timedelta(days=2),
                a - timedelta(days=3),
                a - timedelta(days=4),
                a - timedelta(days=5),
                a - timedelta(days=6),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=Time_interval(a.start - timedelta(days=2), a.end))

        # We get `start`
        dir_specific_cache = next(path_dir_caches.iterdir())  # First dir
        data = json.loads((dir_specific_cache / "cache.json").read_text())
        date_start = parse(data["start"])
        assert o is not None
        assert data["type"] == "slice"

        # We get new `start`
        o = d.run(t=Time_interval(a.start - timedelta(days=4), a.end))
        data = json.loads((dir_specific_cache / "cache.json").read_text())
        new_date_start = parse(data["start"])
        assert o is not None
        assert data["type"] == "slice"
        assert new_date_start < date_start

        # We get all
        date_start = new_date_start
        o = d.run()
        data = json.loads((dir_specific_cache / "cache.json").read_text())
        new_date_start = parse(data["start"])
        assert o is not None
        assert len(o) == len(b)
        assert data["type"] == "full"
        assert new_date_start < date_start

        # We get less
        o = d.run(t=Time_interval(a.start - timedelta(days=2), a.end))
        date_start = new_date_start
        data = json.loads((dir_specific_cache / "cache.json").read_text())
        new_date_start = parse(data["start"])
        assert o is not None
        assert len(o) == 3
        assert data["type"] == "full"
        assert new_date_start == date_start


def test_node_cache_slicethenall():
    """We first get a slice, then we get all"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        path_dir_caches = Path(path_dir_caches)

        # Step 0: Data gen ✨
        a = Seg(datetime(2024, 3, 10, 12, 0), datetime(2024, 3, 10, 13, 0))
        b = Segments(
            [
                a - timedelta(days=0),
                a - timedelta(days=1),
                a - timedelta(days=2),
                a - timedelta(days=3),
                a - timedelta(days=4),
                a - timedelta(days=5),
                a - timedelta(days=6),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(
            t=Time_interval(a.start - timedelta(days=2), a.end - timedelta(days=2))
        )
        assert o is not None
        assert len(o) == 1

        o = d.run()

        p = 0
