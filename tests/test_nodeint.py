from __future__ import annotations

from lifetracking.graph.Node import Node, run_multiple, run_multiple_parallel
from lifetracking.graph.Node_int import (
    Node_int,
    Node_int_addition,
    Node_int_generate,
    Node_int_singleincrement,
)


def test_run_single_simple():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + Node_int_singleincrement(b)
    o = c.run()
    assert o == 4


def test_run_single_prefect():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + Node_int_singleincrement(b)
    o = c.run(prefect=True)
    assert o == 4


def _generate_multi_graph() -> list[Node]:
    artificial_delay = 0.0
    a = Node_int_generate(1, artificial_delay)
    b = Node_int_generate(2, artificial_delay)
    c = Node_int_addition(a, b, artificial_delay)
    d = Node_int_singleincrement(a, artificial_delay)
    return [c, d]


def test_run_multiple_simple():
    graph = _generate_multi_graph()
    o = run_multiple(graph)
    assert o == [3, 2]


def test_run_multiple_prefect():
    graph = _generate_multi_graph()
    o = run_multiple(graph, prefect=True)
    assert o == [3, 2]


def test_run_multiple_parallel_simple():
    graph = _generate_multi_graph()
    o = run_multiple_parallel(graph)
    assert o == [3, 2]


def test_run_multiple_parallel_prefect():
    graph = _generate_multi_graph()
    o = run_multiple_parallel(graph, prefect=True)
    assert o == [3, 2]
