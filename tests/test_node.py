from __future__ import annotations

import datetime

from lifetracking.datatypes.Seg import Seg
from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node_int import Node_int_generate, Node_int_singleincrement
from lifetracking.graph.Node_segments import Node_segments_generate


def test_node_basics_0():
    a = Node_int_generate(1)
    assert str(a) == "Node_int_generate"
    a.name = "a"
    assert str(a) == "Node_int_generate(a)"


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


def test_graph_count_nodes_0():
    a = Node_int_generate(1)
    assert len(a._get_children_all()) + 1 == 1


def test_graph_count_nodes_1():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + Node_int_singleincrement(b)
    assert len(c._get_children_all()) + 1 == 4


def test_graph_count_nodes_2():
    a = Node_int_generate(0)
    b = Node_int_singleincrement(a)
    c = Node_int_singleincrement(b)
    d = Node_int_singleincrement(c)
    e = Node_int_singleincrement(d)
    f = e + e
    o = f.run(prefect=True)
    assert o == 8
    assert len(f._get_children_all()) + 1 == 6


def test_graph_count_nodes_3():
    a = Node_int_generate(1)
    b = a + a
    o = b.run(prefect=True)
    assert o == 2
