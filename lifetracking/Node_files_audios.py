from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import ffmpeg
from pydub.utils import mediainfo
from rich import print

from lifetracking.datatypes.Segments import Seg, Segments
from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.utils import cache_singleargument

if TYPE_CHECKING:
    from typing import Callable

    from lifetracking.graph.Time_interval import Time_interval


class Reader_audios(Node_segments, Node_0child):
    def __init__(
        self,
        path_dir: Path | str,
        fn: Callable[[Path, timedelta | None], datetime] | None = None,
    ) -> None:
        """
        The fn is a function that takes a Path and a timedelta and returns a datetime.
        it's like, a function you would use to parse the data from the file to a date,
        if you don't provide it, the creation date of the file will be used.

        This is mostly useful if you have files with no metadata, like .wav files, but
        that have a creation date that you want to use as the date of the data. e.g.:
        Recording 20241011161010.m4a
        For which you could define something like:

        lambda p, t: (
            datetime.strptime(p.stem.split(" ")[1], "%Y%m%d%H%M%S") -
            (t or timedelta())
        )
        """

        super().__init__()
        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert path_dir.exists()
        assert path_dir.is_dir()
        assert fn is None or callable(fn)
        self.path_dir = path_dir
        self.fn = fn

    def _available(self) -> bool:
        return (
            self.path_dir.exists() and len(self._get_plausible_files(self.path_dir)) > 0
        )

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + str(self.path_dir)).encode()
        ).hexdigest()

    # TODO_2: This cache_single_argument should use the hash of the file
    @staticmethod
    @cache_singleargument("cache_audio_length")
    def _get_audio_length(filename: Path) -> timedelta | None:
        try:
            return timedelta(seconds=float(mediainfo(str(filename))["duration"]))
        except Exception as e:
            print(f"[red]Error while reading {filename}: {e}")
            return None

    def _has_audio_only(self, file: Path) -> bool:
        """Check if a .webm file contains only audio streams using ffmpeg-python."""
        try:
            probe = ffmpeg.probe(str(file))
            # Check if there are any video streams
            video_streams = [
                stream for stream in probe["streams"] if stream["codec_type"] == "video"
            ]
            # If no video streams are found, return True (audio only)
            return len(video_streams) == 0
        except Exception as e:
            print(f"Error checking file {file}: {e}")
            return False

    def _get_plausible_files(self, path_dir: Path) -> list[Path]:
        to_return = []
        for i in path_dir.iterdir():
            if not i.is_file():
                continue
            if i.suffix in (
                ".mp3",
                ".wav",
                ".flac",
                ".ogg",
                ".m4a",
                ".opus",
                ".wma",
                ".amr",
            ) or (i.suffix == ".webm" and self._has_audio_only(i)):
                to_return.append(i)
        return to_return

    def _operation(self, t: Time_interval | None = None) -> Segments | None:
        to_return = []
        for filename in self._get_plausible_files(self.path_dir):

            # The code looks messy because an optimization made to duration, we might
            # only need to calculate it if the file passes the date filter, but also, if
            # the user provides an fn, we then need to calculate it for this fn thing

            duration = None

            # Date filtering
            if self.fn is None:
                date_creation = datetime.fromtimestamp(filename.stat().st_ctime)
            else:
                duration = self._get_audio_length(filename)
                if duration is None:
                    continue  # TODO This should be registered as faulty data
                date_creation = self.fn(filename, duration)
            assert date_creation is not None
            if t is not None and date_creation not in t:
                continue

            # Seg creation
            if duration is None:
                duration = self._get_audio_length(filename)
            if duration is None:
                continue  # TODO This should be registered as faulty data
            to_return.append(
                Seg(
                    date_creation,
                    date_creation + duration,
                    {
                        "duration_in_s": duration.total_seconds(),
                        "filename": filename,
                    },
                )
            )
        return Segments(to_return)
