import hashlib
import os
import socket

import pytest

from lifetracking.Node_weather import Node_weather


def get_computer_name_hash():
    return hashlib.sha256(socket.gethostname().encode()).hexdigest()


hash_pc = "cdec2e6b8ba4e089181714878880ca0a27cf5f8377ab231bb3baf1c73238b9da"
reason = "Test is skipped due because it's very specific and dependent of \
        my machine (if anyone has an idea of how to replicate an anki session \
            in a computer make a PR!!)"


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or get_computer_name_hash() != hash_pc, reason=reason
)
def test_node_weather_0():
    a = Node_weather((41.39, 2.15))
    o = a.run()
    assert o is not None
    assert len(o) > 0
