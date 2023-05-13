from lifetracking.graph.Node import run_multiple, run_multiple_parallel
from lifetracking.graph.Node_int import (
    Node_int_addition,
    Node_int_generate,
    Node_int_singleincrement,
)


def test_run_single_simple():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + Node_int_singleincrement(b)
    o = c.run(t=0.01)
    assert o == 4


def test_run_single_prefect():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + Node_int_singleincrement(b)
    o = c.run(t=0.01, prefect=True)
    assert o == 4


def test_run_multiple_simple():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(a)
    o = run_multiple([c, d], t=0.01)
    assert o == [3, 2]


def test_run_multiple_prefect():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(a)
    o = run_multiple([c, d], t=0.01, prefect=True)
    assert o == [3, 2]


def test_run_multiple_parallel_simple():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(a)
    o = run_multiple_parallel([c, d], t=0.01)
    assert o == [3, 2]


def test_run_multiple_parallel_prefect():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(a)
    o = run_multiple_parallel([c, d], t=0.01, prefect=True)
    assert o == [3, 2]
