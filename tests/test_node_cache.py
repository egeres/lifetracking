from __future__ import annotations

import datetime
import json
import os
import pickle
import tempfile
import time

from dateutil.parser import parse

from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Node_cache import Node_cache
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def test_node_cache_save_tisnone():
    """A pipeline for `Segments` gets cached, then queried with t=None"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        # Hehe, a date
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=13, minute=0, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(hours=0.0),
                a - datetime.timedelta(hours=0.5),
                a - datetime.timedelta(hours=1.3),
                a - datetime.timedelta(hours=950.0),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=None)  # ✨

        # Validation for the data
        assert o is not None
        assert len(o) == len(b)

        # Validation for the cache
        assert len(os.listdir(path_dir_caches)) == 1
        path_dir_specific_cache = os.path.join(
            path_dir_caches, os.listdir(path_dir_caches)[0]
        )
        assert len(os.listdir(path_dir_specific_cache)) == 3
        assert (
            len([x for x in os.listdir(path_dir_specific_cache) if x.endswith(".json")])
            == 1
        )
        assert (
            len(
                [
                    x
                    for x in os.listdir(path_dir_specific_cache)
                    if x.endswith(".pickle")
                ]
            )
            == 2
        )

        # Validation for the json file
        path_fil_json = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".json")
        ][0]
        with open(path_fil_json) as f:
            data = json.load(f)
            assert data["start"] == b.min().strftime("%Y-%m-%d %H:%M:%S")
            assert data["end"] == b.max().strftime("%Y-%m-%d %H:%M:%S")
            assert data["hash_node"] == d.hash_tree()
            assert data["resolution"] == Time_resolution.DAY.value
            assert data["type"] == "full"

        # Validation for the pickle files
        all_data_count = 0
        path_fil_pickle = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".pickle")
        ][0]
        with open(path_fil_pickle, "rb") as f:
            data = pickle.load(f)
            assert isinstance(data, Segments)
            assert len(data) < len(b)
            all_data_count += len(data)
        path_fil_pickle = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".pickle")
        ][1]
        with open(path_fil_pickle, "rb") as f:
            data = pickle.load(f)
            assert isinstance(data, Segments)
            assert len(data) < len(b)
            all_data_count += len(data)
        assert all_data_count == len(b)


def test_node_cache_save_tissomething():
    """We first get data with an offseted week and then with 1 month"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        # Step 0: Data gen
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=12, minute=30, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(days=0),
                a - datetime.timedelta(days=1),
                a - datetime.timedelta(days=2),
                a - datetime.timedelta(days=3),
                a - datetime.timedelta(days=4),
                a - datetime.timedelta(days=5),
                a - datetime.timedelta(days=5.1),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)

        # Step 1: First data gathering
        t = Time_interval.last_week() - datetime.timedelta(days=3)
        o = d.run(t)  # ✨

        # Data validation
        assert o is not None
        assert len(o) == 4

        # Cache folder validation
        path_dir_specific_cache = os.path.join(
            path_dir_caches, os.listdir(path_dir_caches)[0]
        )
        assert len(os.listdir(path_dir_specific_cache)) == 4

        # cache.json validation
        path_fil_json = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".json")
        ][0]
        with open(path_fil_json) as f:
            data = json.load(f)
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
            data_creation_dates = [
                int(v["data_count"]) for k, v in data["data"].items()
            ]
            assert sum(data_creation_dates) == len(o)

        # Step 2: New data gathering
        time.sleep(2.1)  # To create different saving times ✌🏻
        t = Time_interval.last_month()
        o = d.run(t)  # ✨

        # Data validation
        assert o is not None
        assert len(o) == 7

        # Cache folder validation
        path_dir_specific_cache = os.path.join(
            path_dir_caches, os.listdir(path_dir_caches)[0]
        )
        assert len(os.listdir(path_dir_specific_cache)) == 7

        # cache.json validation
        path_fil_json = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".json")
        ][0]
        with open(path_fil_json) as f:
            data = json.load(f)
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
            data_creation_dates = [
                int(v["data_count"]) for k, v in data["data"].items()
            ]
            assert sum(data_creation_dates) == len(o)


def test_node_cache_load_tisnone():  # Difficult!
    with tempfile.TemporaryDirectory() as path_dir_caches:
        # First run
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=13, minute=0, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(hours=0.0),
                a - datetime.timedelta(hours=0.5),
                a - datetime.timedelta(hours=1.3),
                a - datetime.timedelta(hours=950.0),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        _ = d.run(t=None)

        # The part that we're actually interested in
        o = d.run(t=None)  # ✨

        assert o is not None
        assert isinstance(o, Segments)
        assert len(o) == len(b)
        assert o.min() == b.min()
        assert o.max() == b.max()


def test_node_cache_load_tissomething():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        # First run
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=13, minute=0, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(hours=0.0),
                a - datetime.timedelta(hours=0.5),
                a - datetime.timedelta(hours=1.3),
                a - datetime.timedelta(hours=950.0),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        _ = d.run(t=None)

        # The part that we're actually interested in
        a = Time_interval.today()
        a.start -= datetime.timedelta(days=2)
        a.end += datetime.timedelta(days=2)
        o = d.run(t=a)  # ✨

        assert o is not None
        assert isinstance(o, Segments)
        assert len(o) == 3
        assert len(o[a]) == 3


def test_node_cache_dataisextended():
    with tempfile.TemporaryDirectory() as path_dir_caches:
        # Setup
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=13, minute=0, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(days=0),
                a - datetime.timedelta(days=1),
                a - datetime.timedelta(days=2),
                a - datetime.timedelta(days=3),
                a - datetime.timedelta(days=4),
                a - datetime.timedelta(days=5),
                a - datetime.timedelta(days=6),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=Time_interval.last_n_days(2))

        # We get `start`
        path_dir_specific_cache = os.path.join(
            path_dir_caches, os.listdir(path_dir_caches)[0]
        )
        with open(path_dir_specific_cache + "/cache.json") as f:
            data = json.load(f)
        date_start = parse(data["start"])

        assert o is not None
        assert data["type"] == "slice"

        # We get new `start`
        o = d.run(t=Time_interval.last_n_days(4))
        with open(path_dir_specific_cache + "/cache.json") as f:
            data = json.load(f)
        new_date_start = parse(data["start"])

        assert o is not None
        assert data["type"] == "slice"
        assert new_date_start < date_start

        # We get all
        date_start = new_date_start
        o = d.run()
        with open(path_dir_specific_cache + "/cache.json") as f:
            data = json.load(f)
        new_date_start = parse(data["start"])

        assert o is not None
        assert len(o) == len(b)
        assert data["type"] == "full"
        assert new_date_start < date_start

        # We get less
        o = d.run(t=Time_interval.last_n_days(2))
        date_start = new_date_start
        with open(path_dir_specific_cache + "/cache.json") as f:
            data = json.load(f)
        new_date_start = parse(data["start"])

        assert o is not None
        assert len(o) == 3
        assert data["type"] == "full"
        assert new_date_start == date_start


def test_node_cache_nodata():
    """Just checking it doesn't crash"""

    with tempfile.TemporaryDirectory() as path_dir_caches:
        # Setup
        a = Time_interval.today()
        a.start = a.start.replace(hour=12, minute=0, second=0, microsecond=0)
        a.end = a.end.replace(hour=13, minute=0, second=0, microsecond=0)
        a = a.to_seg()
        b = Segments(
            [
                a - datetime.timedelta(days=100),
                a - datetime.timedelta(days=101),
                a - datetime.timedelta(days=102),
                a - datetime.timedelta(days=103),
                a - datetime.timedelta(days=104),
                a - datetime.timedelta(days=105),
            ],
        )
        c = Node_segments_generate(b)
        d = Node_cache(c, path_dir_caches=path_dir_caches)
        o = d.run(t=Time_interval.last_n_days(2))

        assert isinstance(o, Segments)
        assert len(o) == 0
