from __future__ import annotations

import datetime
import hashlib
import os

from pydub.utils import mediainfo
from rich import print

from lifetracking.datatypes.Segment import Seg, Segments
from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import cache_singleargument


class Reader_audios(Node_segments, Node_0child):
    def __init__(self, path_dir: str) -> None:
        super().__init__()
        self.path_dir = path_dir

    def _available(self) -> bool:
        return (
            os.path.exists(self.path_dir)
            and len(self._get_plausible_files(self.path_dir)) > 0
        )

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + self.path_dir).encode()).hexdigest()

    @staticmethod
    @cache_singleargument("cache_audios_length")
    def _get_audio_length_in_s(filename: str) -> float | None:
        try:
            return float(mediainfo(filename)["duration"])
        except Exception as e:
            print(f"[red]Error while reading {filename}: {e}")
            return None

    def _get_plausible_files(self, path_dir: str) -> list[str]:
        to_return = []
        for i in os.listdir(path_dir):
            # File processing
            filename = os.path.join(path_dir, i)
            if not os.path.isfile(filename):
                continue
            if not (
                filename.endswith(".mp3")
                or filename.endswith(".wav")
                or filename.endswith(".flac")
                or filename.endswith(".ogg")
                or filename.endswith(".m4a")
                or filename.endswith(".opus")
                or filename.endswith(".wma")
                or filename.endswith(".amr")
            ):
                continue
            to_return.append(filename)
        return to_return

    def _operation(self, t: Time_interval | None = None) -> Segments:
        to_return = []
        for filename in self._get_plausible_files(self.path_dir):
            # Date filtering
            date_creation = datetime.datetime.fromtimestamp(os.stat(filename).st_ctime)
            if t is not None:
                if date_creation not in t:
                    continue

            # Info extraction
            duration_in_s = self._get_audio_length_in_s(filename)
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
