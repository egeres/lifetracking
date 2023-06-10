import datetime
import os
import tempfile
import time

import pandas as pd

from lifetracking.utils import (
    cache_singleargument,
    export_pddataframe_to_lc_single,
    hash_method,
)


def test_hash_method():
    a = lambda x: x  # noqa: E731
    b = lambda x: x  # noqa: E731
    assert hash_method(a) == hash_method(b)

    c = lambda x: x + 1  # noqa: E731
    assert hash_method(a) != hash_method(c)

    def deffed_a(x):
        return x  # pragma: no cover

    def deffed_b(x):
        return x  # pragma: no cover

    assert hash_method(deffed_a) == hash_method(deffed_b)

    def deffed_c(x):
        return x + 1  # pragma: no cover

    assert hash_method(deffed_a) != hash_method(deffed_c)

    a = lambda x: x["app"] in [  # noqa: E731
        "CivilizationVI.exe",
        "Deathloop.exe",  # A comment
        "factorio.exe",  # Stay away from me
        # Another comment
        "Baba Is You.exe",
        "Cities.exe",  # More comments
    ]
    b = lambda x: x["app"] in [  # noqa: E731
        "CivilizationVI.exe",
        "Deathloop.exe",
        "factorio.exe",
        "Baba Is You.exe",
        "Cities.exe",
    ]
    assert hash_method(a) == hash_method(b)


def test_cache_singleargument():
    with tempfile.TemporaryDirectory() as path_dir:

        def non_cached_method(x: str):
            time.sleep(0.5)
            return x

        t0 = time.time()
        non_cached_method("a")
        t0 = time.time() - t0
        t1 = time.time()
        non_cached_method("a")
        t1 = time.time() - t1

        assert 0.4 < t0 < 0.6
        assert 0.4 < t1 < 0.6

        @cache_singleargument(os.path.join(path_dir, "aha"))
        def cached_method(x: str):
            time.sleep(0.5)
            return x

        t2 = time.time()
        cached_method("a")
        t2 = time.time() - t2
        t3 = time.time()
        cached_method("a")
        t3 = time.time() - t3

        assert 0.4 < t2 < 0.6
        assert 0.0 < t3 < 0.1


def test_export_pddataframe_to_lc_single():
    # Data setup
    d = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    df = pd.DataFrame(
        [
            # A
            {"time": d + datetime.timedelta(minutes=1), "label": "A"},
            {"time": d + datetime.timedelta(minutes=2), "label": "A"},
            {"time": d + datetime.timedelta(minutes=3), "label": "A"},
        ]
    )

    with tempfile.TemporaryDirectory() as path_dir:
        export_pddataframe_to_lc_single(
            df,
            lambda x: x["time"],
            os.path.join(path_dir, "out.json"),
            color="#F00",
            opacity=0.5,
        )
