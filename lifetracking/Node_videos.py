from __future__ import annotations

import datetime
import hashlib
import os
from typing import Any

import ffmpeg
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Segment import Seg, Segments
from lifetracking.graph.Node import Node
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.graph.Time_interval import Time_interval


class Reader_videos(Node_segments):
    def __init__(self, path_dir: str) -> None:
        super().__init__()
        if not os.path.isdir(path_dir):
            raise ValueError(f"{path_dir} is not a directory")
        self.path_dir = path_dir

    def _get_children(self) -> list[Node]:
        return []

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + self.path_dir).encode()).hexdigest()

    # TODO add cache_hash to this
    @staticmethod
    def _get_video_length_in_s(filename: str) -> float | None:
        """Warning, has cache decorator"""
        vid = ffmpeg.probe(filename)
        if (
            len(vid["streams"]) == 2
            and vid["streams"][0].get("tags", {}).get("DURATION") is not None
        ):
            dur_hours = int(
                vid["streams"][0]["tags"]["DURATION"].split(".")[0].split(":")[0]
            )
            dur_mins = int(
                vid["streams"][0]["tags"]["DURATION"].split(".")[0].split(":")[1]
            )
            dur_secs = int(
                vid["streams"][0]["tags"]["DURATION"].split(".")[0].split(":")[2]
            )
            time_in_s = dur_secs + 60 * dur_mins + 3600 * dur_hours
            return time_in_s
        else:
            print("Woops, error, invalid video streams!")

    def _operation(self, t: Time_interval | None = None) -> Segments:
        to_return = []
        for i in os.listdir(self.path_dir):
            # File processing
            filename = os.path.join(self.path_dir, i)
            if not os.path.isfile(filename):
                continue
            if "desktop.ini" in filename:
                continue
            if not (
                filename.endswith(".mp4")
                or filename.endswith(".mkv")
                or filename.endswith(".avi")
                or filename.endswith(".mov")
                or filename.endswith(".webm")
            ):
                continue

            # Date filtering
            date_creation = datetime.datetime.fromtimestamp(os.stat(filename).st_ctime)
            if t is not None:
                if date_creation not in t:
                    continue

            # Info extraction
            duration_in_s = self._get_video_length_in_s(filename)
            if duration_in_s is None:
                continue  # TODO This should be registered as faulty data
            to_return.append(
                Seg(
                    start=date_creation,
                    end=date_creation + datetime.timedelta(seconds=duration_in_s),
                    value={"duration_in_s": duration_in_s, "filename": filename},
                )
            )
        return Segments(to_return)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)
