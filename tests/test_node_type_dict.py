import pytest

from lifetracking.graph.Node_dicts import Reader_dicts


def test_node_dicts_creation():
    with pytest.raises(AssertionError):
        Reader_dicts("Some path which I just made up :)")
