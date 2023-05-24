from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd
import requests
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_activitywatch(Node_pandas):
    def __init__(self, bucket_name: str) -> None:
        super().__init__()
        self.bucket_name = bucket_name

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + self.bucket_name).encode()).hexdigest()

    def _available(self) -> bool:
        """Checks if the server is alive and has the bucket"""
        try:
            r = requests.get("http://localhost:5600/api/0/buckets")
            r.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return False
        except requests.exceptions.HTTPError:
            return False

        # Does the bucket exist?
        buckets = self._get_buckets()
        if self.bucket_name not in buckets:
            return False

        # I guess so :[
        return True

    @staticmethod
    def _get_buckets(url_base: str = "http://localhost:5600") -> list[dict]:
        """Extracts a particular type of bucket"""

        out = requests.get(f"{url_base}/api/0/buckets")
        if out.status_code != 200:
            raise Exception("The connection had a problem!")
        return out.json()

    def _get_data(self, bucket: dict, t: Time_interval | None = None) -> list:
        params = {}
        if t is None:
            params["start"] = bucket["created"]
            params["end"] = bucket["last_updated"]
        else:
            params["start"] = t.start.isoformat()
            params["end"] = t.end.isoformat()
        url_base: str = "http://localhost:5600"
        out = requests.get(
            f"{url_base}/api/0/buckets/{bucket['id']}/events",
            params=params,
        )
        if out.status_code != 200:
            raise Exception("The connection had a problem!")
        return out.json()

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame:
        assert t is None or isinstance(t, Time_interval)

        # We get out bucket
        buckets = self._get_buckets()
        if self.bucket_name not in buckets:
            raise Exception("The bucket name is not available!")
        bucket = buckets[self.bucket_name]

        # Data request
        out = self._get_data(bucket, t)

        # Formatting
        out = [
            {
                "id": x["id"],
                "timestamp": x["timestamp"],
                "duration": x["duration"],
            }
            | x["data"]  # 🙄 Ugh, dumb or genius?
            for x in out
        ]
        return pd.DataFrame(out)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> pd.DataFrame | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[pd.DataFrame, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)
