import pytest

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_int import Node_int, Node_int_generate


def recursive_subclasses(cls):
    for sub_cls in cls.__subclasses__():
        yield sub_cls
        yield from recursive_subclasses(sub_cls)


@pytest.mark.skip(reason="I need a way to check how to do inits with params")
def test_nodes_have_unique_hashes():
    hashes = set()
    for sub_cls in recursive_subclasses(Node):
        # We skip the direct nodes
        if Node in sub_cls.__bases__:
            continue
        h = sub_cls()._hashstr()  # type: ignore
        assert h not in hashes, f"Hash {h} for {sub_cls} is not unique"
        hashes.add(h)
