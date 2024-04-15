"""
Meant to be used for single data points, like weight, height, etc.
"""

from __future__ import annotations

import csv
import datetime
import json
import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path


def get_system_details(way_this_info_was_added: str) -> dict[str, str]:
    return {
        "os.login()": os.getlogin(),
        "platform.system()": platform.system(),
        "machine_name": str(os.getenv("COMPUTERNAME", os.getenv("HOSTNAME"))),
        "way_this_info_was_added": way_this_info_was_added,
        "name_of_the_script": sys.argv[0],
    }


def write_single_on_csv(
    path_to_file: Path,
    way_this_info_was_added: str = "manual",
    now: datetime.datetime | None = None,
    **kwargs,
):

    assert isinstance(path_to_file, Path)
    assert path_to_file.suffix == ".csv"
    assert isinstance(way_this_info_was_added, str)

    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)

    mode = "w" if not path_to_file.exists() else "a"
    d = get_system_details(way_this_info_was_added)

    with open(path_to_file, mode, newline="") as file:
        # TODO_2: Add check if columns change from the header to what it's being saved

        writer = csv.writer(file)
        if mode == "w":
            writer.writerow(["datetime", *d.keys(), *kwargs.keys()])
        writer.writerow([now, *d.values(), *[str(x) for x in kwargs.values()]])


def write_single_on_dated_json(
    path_to_dir: Path,
    way_this_info_was_added: str = "manual",
    now: datetime.datetime | None = None,
    **kwargs,
):

    assert isinstance(path_to_dir, Path)
    assert path_to_dir.is_dir()
    assert isinstance(way_this_info_was_added, str)

    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)

    df: list[dict] = []
    if os.path.exists(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json"):
        with open(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json", "r+") as f:
            df = json.load(f)
    df.append(
        {"datetime": now.isoformat()}
        | get_system_details(way_this_info_was_added)
        | kwargs
    )
    with open(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json", "w") as f:
        json.dump(df, f, indent=4)


# TODO_2: Add write_segment_on_csv
# TODO_2: Add write_segment_on_dated_json


def write_segment_on_csv(
    path_to_file: Path,
    start: datetime.datetime,
    end: datetime.datetime,
    way_this_info_was_added: str = "manual",
    **kwargs,
):
    assert isinstance(path_to_file, Path)
    assert path_to_file.suffix == ".csv"
    assert isinstance(way_this_info_was_added, str)
    assert start < end
    assert isinstance(start, datetime.datetime)
    assert isinstance(end, datetime.datetime)
    assert isinstance(way_this_info_was_added, str)

    mode = "w" if not path_to_file.exists() else "a"
    d = get_system_details(way_this_info_was_added)

    with open(path_to_file, mode, newline="") as file:
        # TODO_2: Add check if columns change from the header to what it's being saved

        writer = csv.writer(file)
        if mode == "w":
            writer.writerow(["start", "end", *d, *kwargs.keys()])
        writer.writerow([start, end, *d.values(), *[str(x) for x in kwargs.values()]])
