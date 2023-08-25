from __future__ import annotations

import datetime
import hashlib
from operator import itemgetter

import pandas as pd
import requests

from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_pandas import Node_pandas
from lifetracking.graph.Time_interval import Time_interval


class Parse_activitywatch(Node_pandas, Node_0child):
    def __init__(
        self, bucket_name: str, url_base: str = "http://localhost:5600"
    ) -> None:
        super().__init__()
        assert isinstance(bucket_name, str)
        assert isinstance(url_base, str)
        assert url_base.startswith("http://")
        assert url_base.split(":")[-1].isnumeric()

        self.url_base: str = url_base
        self.bucket_id = self._get_latest_bucket_that_starts_with_name(bucket_name)
        if isinstance(self.bucket_id, dict):
            self.bucket_id = self.bucket_id["id"]

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.bucket_id)).encode()
        ).hexdigest()

    def _available(self) -> bool:
        """Checks if the server is alive and has the bucket"""

        # The bucket wasn't found
        if self.bucket_id is None:
            return False

        # Try to connect
        try:
            r = requests.get(f"{self.url_base}/api/0/buckets")
            r.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return False
        except requests.exceptions.HTTPError:
            return False

        # Does the bucket exist?
        buckets = self._get_buckets(self.url_base)
        if buckets is None:
            return False
        if self.bucket_id not in buckets:
            return False

        # I guess so :[
        return True

    def _get_latest_bucket_that_starts_with_name(self, name) -> dict | None:
        buckets = self._get_buckets(self.url_base)
        if buckets is None:
            return None
        buckets_list = [v for k, v in buckets.items() if k.startswith(name)]
        for item in buckets_list:
            if "last_updated" in item:
                item["last_updated"] = datetime.datetime.fromisoformat(
                    item["last_updated"].replace("Z", "+00:00")
                )
        buckets_list.sort(key=itemgetter("last_updated"), reverse=True)
        if len(buckets_list) == 0:
            return None
        return buckets_list[0]

    @staticmethod
    def _get_buckets(url_base: str) -> dict | None:
        """Extracts a particular type of bucket"""

        try:
            out = requests.get(f"{url_base}/api/0/buckets")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return None

        if out.status_code != 200:
            raise Exception("The connection had a problem!")
        return out.json()

    def _get_data(self, bucket: dict, t: Time_interval | None = None) -> list:
        params = {}
        if t is None:
            params.update({"start": bucket["created"], "end": bucket["last_updated"]})
        else:
            params.update({"start": t.start.isoformat(), "end": t.end.isoformat()})

        out = requests.get(
            f"{self.url_base}/api/0/buckets/{bucket['id']}/events",
            params=params,
        )
        if out.status_code != 200:
            raise Exception("The connection had a problem!")
        return out.json()

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        assert t is None or isinstance(t, Time_interval)

        if self.bucket_id is None:
            return None

        # We get out bucket
        buckets = self._get_buckets(self.url_base)
        if buckets is None:
            return None
        if self.bucket_id not in buckets:
            raise Exception("The bucket name is not available!")
        bucket = buckets[self.bucket_id]

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
        df = pd.DataFrame(out)
        if len(df) == 0:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df.set_index("timestamp", inplace=True)
        return df
