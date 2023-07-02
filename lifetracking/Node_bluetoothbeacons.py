from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Segment import Seg, Segments
from lifetracking.graph.Node import Node, Node_1child
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.graph.Time_interval import Time_interval


class Parse_BLE_info(Node_1child, Node_segments):
    class Config:
        def __init__(self, config) -> None:
            if isinstance(config, str):
                self._config = self._load_config(config)
            else:
                self._config = config

        def _load_config(self, path_file: str) -> dict[str, Any]:
            with open(path_file) as f:
                return json.load(f)

        @property
        def config(self) -> dict[str, tuple[str, float]]:
            return self._config

    def __init__(self, n0: Node_pandas, config: dict[str, Any] | str) -> None:
        assert isinstance(n0, Node_pandas)
        super().__init__()
        self.n0 = n0
        self.config = self.Config(config)

    def _hashstr(self) -> str:
        return hashlib.md5(
            (
                super()._hashstr() + str(json.dumps(self.config.config, sort_keys=True))
            ).encode()
        ).hexdigest()

    def _operation_skip_certain_columns(
        self, column_name: str, config: Config, df: pd.DataFrame
    ) -> bool:
        if column_name not in config.config:
            return True
        if column_name == "timestamp":
            return True
        if pd.isna(df[column_name]).all():
            return True
        return False

    def _operation(
        self,
        n0: pd.DataFrame | PrefectFuture[pd.DataFrame, Sync],
        config: Config | None = None,
        t: Time_interval | None = None,
    ) -> Segments:
        assert n0 is not None
        assert config is not None
        assert t is None or isinstance(t, Time_interval)

        df: pd.DataFrame = n0  # type: ignore
        df.replace(9999.0, np.nan, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")

        to_return = []
        for column_name in list(df.columns):
            if self._operation_skip_certain_columns(column_name, config, df):
                continue

            # Pre-data
            name, min_distance = config.config[column_name]
            time_to_wait_before_next = datetime.timedelta(minutes=3.0)

            # Processing itself
            segments: list[Seg] = []
            in_segment = False
            start_time: datetime.datetime | None = None
            for _, row in df.iterrows():
                timestamp = row["timestamp"]
                value = row[column_name]

                if not pd.isna(value) and value < min_distance:
                    if not in_segment:
                        in_segment = True
                        start_time = timestamp
                else:
                    if in_segment:
                        assert start_time is not None
                        end_time = timestamp
                        if (
                            segments
                            and (start_time - segments[-1].end)
                            <= time_to_wait_before_next
                        ):
                            segments[-1].end = end_time
                        else:
                            segments.append(Seg(start_time, end_time, {"name": name}))
                        in_segment = False

            to_return.extend(segments)

        return Segments(to_return)
