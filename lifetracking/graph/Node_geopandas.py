from __future__ import annotations

import datetime
import hashlib
import os
import warnings
from typing import Any, Callable

import geopandas as gpd
import pandas as pd
from fiona.errors import DriverError
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync
from rich import print
from shapely.geometry import Point, Polygon

from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import export_pddataframe_to_lc_single, hash_method


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

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        color: str | Callable[[pd.Series], str] | None = None,
        opacity: float | Callable[[pd.Series], float] = 1.0,
    ):
        o = self.run(t)
        assert o is not None
        export_pddataframe_to_lc_single(
            o,
            path_filename=path_filename,
            color=color,
            opacity=opacity,
        )


# TODO: Ok, so... does this count as duplicated code? Maybe I should do some
# kind of template for operation nodes which then is implemented for
# sub-classes...?
class Node_geopandas_operation(Node_1child, Node_geopandas):
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

    @property
    def child(self) -> Node:
        return self.n0

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


class Reader_geojson(Node_0child, Node_geopandas):
    def __init__(
        self,
        path_dir: str,
        column_date_index: str | None = None,
    ) -> None:
        assert isinstance(path_dir, str)
        assert column_date_index is None or isinstance(column_date_index, str)
        if not os.path.isdir(path_dir):
            # raise ValueError(f"{path_dir} is not a directory")
            warnings.warn(f"{path_dir} is not a directory", stacklevel=2)
        super().__init__()
        self.path_dir = path_dir
        self.column_date_index = column_date_index

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return (
            os.path.isdir(self.path_dir)
            # TODO_2: Optimize the "len(...)" to see if there is at least one file with
            # the extension we want by just adding an iterator that fucking stops when
            # it encounters the corresponding file?
            and len([i for i in os.listdir(self.path_dir) if i.endswith(".geojson")])
            > 0
        )

    def _operation(self, t: Time_interval | None = None) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, Time_interval)
        to_return: list = []
        for filename in os.listdir(self.path_dir):
            if filename.endswith(".geojson"):
                filename_date = datetime.datetime.strptime(
                    filename.split("_")[-1],
                    "%Y%m%d.geojson",
                )
                if isinstance(t, Time_interval) and t.start.tzinfo is not None:
                    filename_date = filename_date.replace(tzinfo=t.start.tzinfo)
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
                    print(f"[red]Error reading {filename}")
        df = pd.concat(to_return, axis=0)
        if self.column_date_index is not None:
            # Parse to datetime
            df[self.column_date_index] = pd.to_datetime(
                df[self.column_date_index],
                format="mixed",
            )
            # Set as index
            df = df.set_index(self.column_date_index)
        return df


class Label_geopandas(Node_1child, Node_geopandas):
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

    @property
    def child(self) -> Node:
        return self.n0

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
                    point.geometry.distance(pt_row["geometry"]) <= 30 / 10**5
                ):  # Rough conversion of meters to degrees
                    return pt_row["label"]

            # If not, check if the point is within any of the provided polygons
            for _, poly_row in self.coords_polygs.iterrows():
                if point.geometry.buffer(20 / 10**5).intersects(poly_row["geometry"]):
                    return poly_row["label"]

            return None

        n0["label"] = n0.apply(get_label, axis=1)

        return n0
