from __future__ import annotations

import datetime
import json
import os
import pickle
import tempfile

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_cache import Node_cache
from lifetracking.graph.Node_segments import Node_segments_generate
from lifetracking.graph.Time_interval import Time_interval, Time_resolution


def test_node_cache_save_tisnone():
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
                a - datetime.timedelta(hours=150.0),
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
        path_fil_pickle = [
            os.path.join(path_dir_specific_cache, x)
            for x in os.listdir(path_dir_specific_cache)
            if x.endswith(".pickle")
        ][0]
        with open(path_fil_pickle, "rb") as f:
            data = pickle.load(f)
            assert isinstance(data, Segments)
            assert len(data) < len(b)
