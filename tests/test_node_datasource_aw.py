import hashlib
import os
import socket

import pandas as pd
import pytest

from lifetracking.graph.Time_interval import Time_interval
from lifetracking.Node_activitywatch import Parse_activitywatch


def get_computer_name_hash():
    return hashlib.sha256(socket.gethostname().encode()).hexdigest()


hash_pc = "cdec2e6b8ba4e089181714878880ca0a27cf5f8377ab231bb3baf1c73238b9da"
reason = "Test is skipped due because it's very specific and dependent of \
        my machine (if anyone has an idea of how to replicate an anki session \
            in a computer make a PR!!)"


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or get_computer_name_hash() != hash_pc, reason=reason
)
def test_node_aw_0():
    print("GH actions =", os.environ.get("GITHUB_ACTIONS"))
    print("hash       =", get_computer_name_hash())

    # We get the data
    a = Parse_activitywatch("aw-watcher-window")
    t = Time_interval.last_month()
    o = a.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert isinstance(o.index, pd.DatetimeIndex)
    assert t.duration_days > (max(o.index) - min(o.index)).total_seconds() / 86400

    prev_shape = o.shape[0]
    b = a.filter(lambda x: x["app"] in ["firefox.exe"])
    o = b.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert o.shape[0] < prev_shape


def test_node_aw_1():
    a = Parse_activitywatch(
        "thisdoesntexist",
        url_base="http://www.doesnotexist.com:99",
    )
    t = Time_interval.last_month()
    o = a.run(t)
    assert o is None
