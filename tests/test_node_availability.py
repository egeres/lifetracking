from __future__ import annotations

from lifetracking.graph.Node_int import (
    Node_int_generate,
    Node_int_generate_unavailable,
    Node_int_singleincrement,
)


def test_available():
    a = Node_int_generate(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(c)
    assert d.available is True


def test_unavailable():
    a = Node_int_generate_unavailable(1)
    b = Node_int_generate(2)
    c = a + b
    d = Node_int_singleincrement(c)
    assert d.available is False
