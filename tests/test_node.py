from __future__ import annotations

import copy
import datetime
import json
import os
import tempfile

from hypothesis import given, settings
from hypothesis import strategies as st

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_segments import Node_segments_generate, Node_segments_merge
from lifetracking.graph.Time_interval import Time_interval


def test_print_stats():
    a = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    b = Segments(
        [
            Seg(
                a + datetime.timedelta(minutes=0),
                a + datetime.timedelta(minutes=1),
                {"my_key": 0},
            ),
            Seg(
                a + datetime.timedelta(minutes=3),
                a + datetime.timedelta(minutes=4),
                {"my_key": 1},
            ),
            Seg(
                a + datetime.timedelta(minutes=6),
                a + datetime.timedelta(minutes=7),
                {"my_key": 0},
            ),
            Seg(
                a + datetime.timedelta(minutes=9),
                a + datetime.timedelta(minutes=10),
                {"my_key": 0},
            ),
        ]
    )
    c = Node_segments_generate(b)

    c.run()
    c.print_stats()
