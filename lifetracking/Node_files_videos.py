from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import ffmpeg

from lifetracking.datatypes.Segments import Seg, Segments
from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_segments import Node_segments
from lifetracking.graph.Time_interval import Time_interval
from lifetracking.utils import cache_singleargument


class Reader_videos(Node_segments, Node_0child):
    def __init__(
        self,
        path_dir: Path | str,
        # TODO: Add filename_date_pattern or option for os.stat(filename).st_ctime
        # filename_date_pattern=None,
        dir_tmp: None | Path = None,
    ) -> None:
        super().__init__()
        if isinstance(path_dir, str):
            path_dir = Path(path_dir)
        assert isinstance(path_dir, Path)
        assert dir_tmp is None or isinstance(dir_tmp, Path)
        if not path_dir.exists():
            msg = f"{path_dir} doesn't exist"
            raise ValueError(msg)
        if not path_dir.is_dir():
            msg = f"{path_dir} is not a directory"
            raise ValueError(msg)

        self.path_dir = path_dir
        self._get_video_length = cache_singleargument(
            "cache_videos_length",
            dir_tmp,
        )(self._get_video_length)

    def _hashstr(self) -> str:
        return hashlib.md5(
            (super()._hashstr() + self.path_dir.name).encode()
        ).hexdigest()

    @staticmethod
    def _get_video_length(filename: str) -> timedelta | None:
        """Warning, has cache decorator"""
        vid = ffmpeg.probe(filename)
        if not (
            len(vid["streams"]) == 2
            and vid["streams"][0].get("tags", {}).get("DURATION") is not None
        ):
            print(f"Woops! error, invalid video streams with {filename}")
            return None

        dur = vid["streams"][0]["tags"]["DURATION"]
        dur_hour = int(dur.split(".")[0].split(":")[0])
        dur_mins = int(dur.split(".")[0].split(":")[1])
        dur_secs = int(dur.split(".")[0].split(":")[2])
        return timedelta(seconds=dur_secs, minutes=dur_mins, hours=dur_hour)

    def _operation(self, t: Time_interval | None = None) -> Segments | None:
        assert isinstance(t, Time_interval) or t is None

        to_return = []

        for filename in (
            file
            for file in self.path_dir.iterdir()
            if file.suffix in {".mp4", ".mkv", ".avi", ".mov", ".webm"}
        ):

            # I'm confused about which is the "right one", ctime makes more sense?
            # date_creation = datetime.fromtimestamp(filename.stat().st_mtime)
            date_creation = datetime.fromtimestamp(filename.stat().st_ctime)
            if t is not None and date_creation not in t:
                continue

            # Info extraction
            try:
                duration = self._get_video_length(str(filename))
            except Exception as e:
                print(f"Error with {filename}: {e}")
                continue
            if duration is None:
                print(f"Error with {filename}: duration is None")
                continue  # TODO This should be registered as faulty data
            to_return.append(
                Seg(
                    date_creation,
                    date_creation + duration,
                    value={"duration_in_s": duration, "filename": filename},
                )
            )
        return Segments(to_return)
