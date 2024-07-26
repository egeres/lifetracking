import pytest

from lifetracking.graph.Node_geopandas import Reader_geodata


def test_node_geopandas_creation():
    with pytest.raises(AssertionError):
        Reader_geodata("Some path")
