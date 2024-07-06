import hashlib
import os
import socket

import pytest

from lifetracking.graph.Time_interval import Time_interval
from lifetracking.Node_anki import Parse_anki_creation, Parse_anki_study


def get_computer_name_hash():
    return hashlib.sha256(socket.gethostname().encode()).hexdigest()


hash_pc = "cdec2e6b8ba4e089181714878880ca0a27cf5f8377ab231bb3baf1c73238b9da"
reason = "Test is skipped due because it's very specific and dependent of \
        my machine (if anyone has an idea of how to replicate an anki session \
            in a computer make a PR!!)"


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or get_computer_name_hash() != hash_pc, reason=reason
)
def test_node_anki_study():
    t = Time_interval.last_year()
    a = Parse_anki_study()
    o = a.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert t.duration_days > (max(o.index) - min(o.index)).total_seconds() / 86400


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or get_computer_name_hash() != hash_pc, reason=reason
)
def test_node_anki_create():
    t = Time_interval.last_year()
    a = Parse_anki_creation()
    o = a.run(t)
    assert o is not None
    assert o.shape[0] > 0
    assert t.duration_days > (max(o.index) - min(o.index)).total_seconds() / 86400


def test_node_anki_dir_does_not_exist():

    with pytest.raises(AssertionError):
        Parse_anki_creation("/this_anki_folder_does_not_exist")
