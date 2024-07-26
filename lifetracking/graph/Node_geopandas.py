from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from lifetracking.graph.Node import Node, Node_0child, Node_1child
from lifetracking.graph.quantity import Quantity
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import export_pddataframe_to_lc_single, hash_method

if TYPE_CHECKING:
    from prefect.futures import PrefectFuture
    from prefect.utilities.asyncutils import Sync


def count_lines(file_path: Path) -> int:
    with file_path.open("r") as file:
        return sum(1 for _ in file)


class Node_geopandas(Node[gpd.GeoDataFrame]):
    def __init__(self) -> None:
        super().__init__()

    def apply(
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
        t: Time_interval | Quantity | None = None,
    ) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, (Time_interval, Quantity))
        return self.fn_operation(n0)


class Reader_geodata(Node_0child, Node_geopandas):
    def __init__(
        self,
        path_dir: Path | str,
        column_date_index: str | None = None,
        file_format: str = ".csv",
    ) -> None:
        """For now this assumes that the files ends with a date like '_20240130'"""

        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert isinstance(path_dir, Path)
        assert column_date_index is None or isinstance(column_date_index, str)
        assert path_dir.exists()
        assert path_dir.is_dir()
        assert file_format in [".csv", ".geojson", ".gpx", ".kml"]
        super().__init__()
        self.path_dir = path_dir
        self.column_date_index = column_date_index
        self.format = file_format

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        return self.path_dir.is_dir() and any(self.path_dir.glob(f"*{self.format}"))

    def _operation(
        self, t: Time_interval | Quantity | None = None
    ) -> gpd.GeoDataFrame | None:
        assert t is None or isinstance(t, (Time_interval, Quantity))

        date_to_file = {
            datetime.strptime(i.name.split("_")[-1], f"%Y%m%d{self.format}"): i
            for i in self.path_dir.glob(f"*{self.format}")
        }

        if isinstance(t, Time_interval):
            date_to_file = {
                date: file
                for date, file in date_to_file.items()
                if t.start <= date <= t.end
            }

        to_return = []
        rows_so_far = 0
        for _, f in sorted(date_to_file.items(), reverse=True):

            if f.suffix == ".csv":
                total_lines_file = count_lines(f) - 1
                to_get_from_this_file = total_lines_file
                if isinstance(t, Quantity):
                    to_get_from_this_file = min(t.value - rows_so_far, total_lines_file)
                df = pd.read_csv(
                    self.path_dir / f,
                    skiprows=range(1, total_lines_file - to_get_from_this_file + 1),
                )
                if "lat" not in df.columns or "lon" not in df.columns:
                    msg = (
                        "The GeoDataFrame does not have 'lat' and 'lon' "
                        "columns to create a 'geometry' column"
                    )
                    raise ValueError(msg)
                df["geom"] = df.apply(lambda r: Point(r["lon"], r["lat"]), axis=1)
                df = gpd.GeoDataFrame(df, geometry="geom")
                df.set_crs(epsg=4326, inplace=True)
            else:
                df = gpd.read_file(self.path_dir / f)

            to_return.append(df)
            rows_so_far += len(df)

            if isinstance(t, Quantity) and rows_so_far >= t.value:
                break

        if len(to_return) == 0:
            return None
        df = to_return[0] if len(to_return) == 1 else pd.concat(to_return, axis=0)

        if self.column_date_index is not None:
            df[self.column_date_index] = pd.to_datetime(
                df[self.column_date_index], format="mixed"
            )
            df = df.set_index(self.column_date_index)

        assert isinstance(df, gpd.GeoDataFrame) or df is None
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
                # Point(map(lambda x: float(x.strip()), x.split(",")[::-1]))
                Point(float(x.strip()) for x in x.split(",")[::-1])
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

    def _operation(
        self, n0: gpd.GeoDataFrame, t: Time_interval | Quantity | None = None
    ) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, (Time_interval, Quantity))
        assert isinstance(n0, gpd.GeoDataFrame)

        # Ensure the input GeoDataFrame is using the correct CRS
        # n0 = n0.to_crs("EPSG:4326")

        n0 = n0.set_crs("EPSG:4326") if n0.crs is None else n0.to_crs("EPSG:4326")  # type: ignore

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


class Label_geopandas_singlepoint(Node_1child, Node_geopandas):
    """Adds a column titled 'label' with either "in" or "out" depending on the distance
    to a single point"""

    def __init__(
        self,
        n0: Node_geopandas,
        coords: tuple[float, float],
        distance_meters: float = 50,
    ) -> None:
        assert isinstance(n0, Node_geopandas)
        assert isinstance(coords, tuple)
        assert len(coords) == 2
        assert isinstance(distance_meters, (int, float))

        super().__init__()
        self.n0 = n0
        self.coords = coords
        self.distance_meters = float(distance_meters)

    @property
    def child(self) -> Node:
        return self.n0

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + str(self.coords)).encode()).hexdigest()

    def _operation(
        self,
        n0: gpd.GeoDataFrame,
        t: Time_interval | Quantity | None = None,
    ) -> gpd.GeoDataFrame:
        assert t is None or isinstance(t, (Time_interval, Quantity))
        assert isinstance(n0, gpd.GeoDataFrame)

        n0["label"] = "out"
        n0 = n0.to_crs(epsg=32631)
        p = (
            gpd.GeoSeries([Point(*self.coords)], crs="EPSG:4326")
            .to_crs(epsg=32631)
            .iloc[0]
        )
        n0["distance"] = n0.geometry.apply(lambda point: point.distance(p))
        n0.loc[n0["distance"] <= self.distance_meters, "label"] = "in"
        return n0
