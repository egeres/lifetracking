from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

from pydub.utils import mediainfo
from rich import print

from lifetracking.datatypes.Segments import Seg, Segments
from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import cache_singleargument


class Reader_audios(Node_segments, Node_0child):
    def __init__(self, path_dir: Path | str) -> None:
        super().__init__()
        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert path_dir.exists()
        assert path_dir.is_dir()
        self.path_dir = path_dir

    def _available(self) -> bool:
        return (
            self.path_dir.exists() and len(self._get_plausible_files(self.path_dir)) > 0
        )

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + self.path_dir).encode()).hexdigest()

    @staticmethod
    @cache_singleargument("cache_audios_length")
    def _get_audio_length_in_s(filename: Path) -> float | None:
        try:
            return float(mediainfo(str(filename))["duration"])
        except Exception as e:
            print(f"[red]Error while reading {filename}: {e}")
            return None

    def _get_plausible_files(self, path_dir: Path) -> list[Path]:
        to_return = []
        for i in path_dir.iterdir():
            if not i.is_file():
                continue
            if i.suffix not in (
                ".mp3",
                ".wav",
                ".flac",
                ".ogg",
                ".m4a",
                ".opus",
                ".wma",
                ".amr",
            ):
                continue
            to_return.append(i)
        return to_return

    def _operation(self, t: Time_interval | None = None) -> Segments | None:
        to_return = []
        for filename in self._get_plausible_files(self.path_dir):
            # Date filtering
            date_creation = datetime.datetime.fromtimestamp(filename.stat().st_ctime)
            if t is not None and date_creation not in t:
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
