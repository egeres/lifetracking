import datetime
import hashlib
import os
import socket
from operator import itemgetter

import pytest
from dateutil import parser

from lifetracking.graph.Time_interval import Time_interval
from lifetracking.Node_activitywatch import Parse_activitywatch
from lifetracking.Node_anki import Parse_anki_creation, Parse_anki_study


def get_computer_name_hash():
    return hashlib.sha256(socket.gethostname().encode()).hexdigest()


hash_pc = "cdec2e6b8ba4e089181714878880ca0a27cf5f8377ab231bb3baf1c73238b9da"
reason = "Test is skipped due because it's very specific and dependent of \
        my machine (if anyone has an idea of how to replicate an anki session \
            in a computer make a PR!!)"


@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS") == "true" or get_computer_name_hash() != hash_pc,
    reason=reason,
)
def test_node_aw_0():
    print("GH actions =", os.environ.get("GITHUB_ACTIONS"))
    print("hash       =", get_computer_name_hash())

    # We dynamically get the most recent bucket
    buckets = Parse_activitywatch._get_buckets()
    buckets_list = [v for k, v in buckets.items() if "aw-watcher-window" in k]
    for item in buckets_list:
        if "last_updated" in item:
            item["last_updated"] = datetime.datetime.fromisoformat(
                item["last_updated"].replace("Z", "+00:00")
            )
    buckets_list.sort(key=itemgetter("last_updated"), reverse=True)
    most_recent_aw_watcher_window = buckets_list[0]

    # We get the data
    t = Time_interval.last_month()
    a = Parse_activitywatch(most_recent_aw_watcher_window["id"])
    o = a.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert (
        t.duration_days
        > (
            parser.parse(max(o.timestamp)) - parser.parse(min(o.timestamp))
        ).total_seconds()
        / 86400
    )

    prev_shape = o.shape[0]
    b = a.filter(lambda x: x["app"] in ["firefox.exe"])
    o = b.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert o.shape[0] < prev_shape
