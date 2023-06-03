from __future__ import annotations

import datetime
import dis
import hashlib
import os
from typing import Any, Callable

import geopandas as gpd
import pandas as pd
from fiona.errors import DriverError
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync
from shapely.geometry import Point, Polygon

from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import hash_method


class Node_geopandas(Node[gpd.GeoDataFrame]):
    def __init__(self) -> None:
        super().__init__()

    def operation(
        self,
        f: Callable[
            [gpd.GeoDataFrame | PrefectFuture[gpd.GeoDataFrame, Sync]], gpd.GeoDataFrame
        ],
    ) -> Node_geopandas:
        return Node_geopandas_operation(self, f)


# TODO: Ok, so... does this count as duplicated code? Maybe I should do some
# kind of template for operation nodes which then is implemented for
# sub-classes...?
class Node_geopandas_operation(Node_geopandas):
    def __init__(
        self,
        n0: Node_geopandas,
        fn_operation: Callable[
            [gpd.GeoDataFrame | PrefectFuture[gpd.GeoDataFrame, Sync]], gpd.GeoDataFrame
        ],
    ) -> None:
        assert isinstance(n0, Node_geopandas)
        assert callable(fn_operation), "operation_main must be callable"
        super().__init__()
        self.n0 = n0
        self.fn_operation = fn_operation

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + hash_method(self.fn_operation)).encode()
        ).hexdigest()

    def _operation(
        self,
        n0: gpd.GeoDataFrame | PrefectFuture[gpd.GeoDataFrame, Sync],
        t: Time_interval | None = None,
    ) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, Time_interval)
        return self.fn_operation(n0)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> gpd.GeoDataFrame | None:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(n0_out, t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[gpd.GeoDataFrame, Sync] | None:
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        if n0_out is None:
            return None
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out, t
        )


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
        coords_polygs: list[tuple[list[tuple[float, float]], str]],
    ) -> None:
        assert isinstance(n0, Node_geopandas)
        super().__init__()
        self.n0 = n0

        self.hash_coords_points = hashlib.md5((str(coords_points)).encode()).hexdigest()
        self.hash_coords_polygs = hashlib.md5((str(coords_polygs)).encode()).hexdigest()

        self.coords_points = gpd.GeoDataFrame(
            coords_points,
            columns=["geometry", "label"],
            geometry=[
                Point(map(lambda x: float(x.strip()), x.split(",")[::-1]))
                for x, _ in coords_points
            ],
        )
        self.coords_points.crs = "EPSG:4326"

        self.coords_polygs = gpd.GeoDataFrame(
            coords_polygs,
            columns=["geometry", "label"],
            geometry=[
                Polygon([pt[::-1] for pt in coords]) for coords, _ in coords_polygs
            ],
        )
        self.coords_polygs.crs = "EPSG:4326"

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr()
                + str(self.hash_coords_points)
                + str(self.hash_coords_polygs)
            ).encode()
        ).hexdigest()

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
            for _, poly_row in self.coords_polygs.iterrows():
                if point.geometry.buffer(20 / 10**5).intersects(poly_row["geometry"]):
                    return poly_row["label"]

            return None

        n0["label"] = n0.apply(get_label, axis=1)

        return n0

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> gpd.GeoDataFrame | None:
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
    ) -> PrefectFuture[gpd.GeoDataFrame, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )
