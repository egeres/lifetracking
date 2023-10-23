from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from lifetracking.graph.Node import (
    Node,
    Node_generate_None,
    run_multiple,
    run_multiple_parallel,
)
from lifetracking.graph.Node_int import (
    Node_int_addition,
    Node_int_generate,
    Node_int_singleincrement,
)


def test_run_single_basic():
    a = Node_int_generate(1)
    b = Node_int_generate(1)
    c = a + b
    assert c.run() == 2


def test_run_single_none():
    a = Node_int_generate(1)
    b = Node_generate_None()
    c = a + b  # type: ignore
    assert c.run() is None


@given(st.integers(), st.integers())
def test_run_single_simple(a_val, b_val):
    a = Node_int_generate(a_val)
    b = Node_int_generate(b_val)
    c = a + Node_int_singleincrement(b)
    o = c.run()
    assert o == a_val + b_val + 1

    # We check the "childrenity" of the nodes
    assert len(a.children) == 0
    assert len(b.children) == 0
    assert len(Node_int_singleincrement(b).children) == 1
    assert len(c.children) == 2


def test_run_single_hash():
    a = Node_int_generate(1)
    b = Node_int_generate(1)
    c = a + b

    h_i = a._hashstr()  # Individual hash
    h_t = c.hash_tree()  # Hash tree
    assert h_i != h_t


def test_run_single_simple_test_iadd():
    a = Node_int_generate(1)
    b = Node_int_generate(1)
    a += Node_int_singleincrement(b)
    o = a.run()
    assert o == 3


# Currently I'm not interested in testing the prefect integration with hypothesis
# because it's too slow
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
