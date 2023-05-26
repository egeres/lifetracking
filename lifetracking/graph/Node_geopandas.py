from __future__ import annotations

import datetime
import hashlib
import os
from typing import Any

import geopandas as gpd
import pandas as pd
from fiona.errors import DriverError
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync
from shapely.geometry import Point, Polygon

from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval


class Node_geopandas(Node[gpd.GeoDataFrame]):
    def __init__(self) -> None:
        super().__init__()


class Reader_geojson(Node_geopandas):
    def __init__(self, path_dir: str) -> None:
        if not os.path.isdir(path_dir):
            raise ValueError(f"{path_dir} is not a directory")
        super().__init__()
        self.path_dir = path_dir

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return (
            os.path.isdir(self.path_dir)
            and len([i for i in os.listdir(self.path_dir) if i.endswith(".geojson")])
            > 0
        )

    def _operation(self, t: Time_interval | None = None) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, Time_interval)
        to_return: list = []
        for filename in os.listdir(self.path_dir):
            if filename.endswith(".geojson"):
                filename_date = datetime.datetime.strptime(
                    filename.split("_")[-1], "%Y%m%d.geojson"
                )
                if t is not None and not (t.start <= filename_date <= t.end):
                    continue
                try:
                    to_return.append(
                        gpd.read_file(
                            os.path.join(
                                self.path_dir,
                                filename,
                            )
                        )
                    )
                except DriverError:
                    print(f"Error reading {filename}")
        return pd.concat(to_return, axis=0)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> gpd.GeoDataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[gpd.GeoDataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


class Label_geopandas(Node_geopandas):
    """Adds an extra column according to your labels"""

    def __init__(
        self,
        n0: Node_geopandas,
        coords_points: list[tuple[Any, str]],
        data_polygons,
    ) -> None:
        super().__init__()
        self.n0 = n0

        self.coords_points = gpd.GeoDataFrame(
            coords_points,
            columns=["geometry", "label"],
            geometry=[
                Point(map(lambda x: float(x.strip()), x.split(",")[::-1]))
                for x, _ in coords_points
            ],
        )
        self.coords_points.crs = "EPSG:4326"

        self.data_polygons = gpd.GeoDataFrame(
            data_polygons,
            columns=["geometry", "label"],
            geometry=[
                Polygon([pt[::-1] for pt in coords]) for coords, _ in data_polygons
            ],
        )
        self.data_polygons.crs = "EPSG:4326"

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return (
            os.path.isdir(self.path_dir)
            and len([i for i in os.listdir(self.path_dir) if i.endswith(".geojson")])
            > 0
        )

    def _operation(self, n0, t: Time_interval | None = None) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, Time_interval)

        # Ensure the input GeoDataFrame is using the correct CRS
        n0 = n0.to_crs("EPSG:4326")

        def get_label(point):
            # First, check if the point is within any of the provided points
            for _, pt_row in self.coords_points.iterrows():
                if (
                    point.geometry.distance(pt_row["geometry"]) <= 20 / 10**5
                ):  # Rough conversion of meters to degrees
                    return pt_row["label"]

            # If not, check if the point is within any of the provided polygons
            for _, poly_row in self.data_polygons.iterrows():
                if point.geometry.buffer(20 / 10**5).intersects(poly_row["geometry"]):
                    return poly_row["label"]

            return None

        n0["label"] = n0.apply(get_label, axis=1)

        return n0

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> int | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(
            n0_out,
            t,
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[int, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )
