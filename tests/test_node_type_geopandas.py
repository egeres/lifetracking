import pytest

from lifetracking.graph.Node_geopandas import (
    Label_geopandas,
    Node_geopandas,
    Node_geopandas_operation,
    Reader_geojson,
)
from lifetracking.graph.Time_interval import Time_interval


def test_node_geopandas_creation():
    with pytest.raises(AssertionError):
        Reader_geojson("Some path")
