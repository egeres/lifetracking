import pytest

from lifetracking.graph.Node_geopandas import (
    Reader_geojson,
)


def test_node_geopandas_creation():
    with pytest.raises(AssertionError):
        Reader_geojson("Some path")
