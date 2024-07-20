"""
Meant to be used for single data points, like weight, height, etc.
"""

from __future__ import annotations

import csv
import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path


def get_system_details(way_this_info_was_added: str) -> dict[str, str]:
    """Gets the details of the system"""

    assert isinstance(way_this_info_was_added, str)

    return {
        "os.login()": os.getlogin(),
        "platform.system()": platform.system(),
        "machine_name": str(os.getenv("COMPUTERNAME", os.getenv("HOSTNAME"))),
        "way_this_info_was_added": way_this_info_was_added,
        "name_of_the_script": sys.argv[0],
        "datetime_of_annotation": datetime.now(timezone.utc).isoformat(),
    }


def write_single_on_csv(
    path_to_file: Path,
    now: datetime | None = None,
    way_this_info_was_added: str = "manual",
    **kwargs,
):
    assert isinstance(path_to_file, Path)
    assert isinstance(now, datetime) or now is None
    assert path_to_file.suffix == ".csv"
    assert isinstance(way_this_info_was_added, str)

    now = datetime.now(timezone.utc) if now is None else now
    d = get_system_details(way_this_info_was_added)

    mode = "w" if not path_to_file.exists() else "a"
    with path_to_file.open(mode, encoding="utf-8", newline="") as f:
        # TODO_2: Add check if columns change from the header to what it's being saved

        w = csv.writer(f)
        if mode == "w":
            w.writerow(["datetime", *d.keys(), *kwargs.keys()])
        w.writerow([now.isoformat(), *d.values(), *[str(x) for x in kwargs.values()]])


def write_single_on_dated_json(
    path_to_dir: Path,
    now: datetime | None = None,
    way_this_info_was_added: str = "manual",
    **kwargs,
):
    assert isinstance(path_to_dir, Path)
    assert isinstance(now, datetime) or now is None
    assert path_to_dir.is_dir()
    assert isinstance(way_this_info_was_added, str)

    now = datetime.now(timezone.utc) if now is None else now

    fil = path_to_dir / f"{now.strftime('%Y-%m-%d')}.json"
    df: list[dict] = json.load(fil.open("r", encoding="utf-8")) if fil.exists() else []
    df.append(
        {"datetime": now.isoformat()}
        | get_system_details(way_this_info_was_added)
        | kwargs
    )
    json.dump(df, fil.open("w", encoding="utf-8"), indent=4)
