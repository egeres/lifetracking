# TODO_2: Add write_segment_on_csv
# TODO_2: Add write_segment_on_dated_json


import csv
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path


def get_system_details(way_this_info_was_added: str) -> dict[str, str]:
    """Gets the details of the system"""
    # TODO_1: Make this code common pls

    assert isinstance(way_this_info_was_added, str)

    return {
        "os.login()": os.getlogin(),
        "platform.system()": platform.system(),
        "machine_name": str(os.getenv("COMPUTERNAME", os.getenv("HOSTNAME"))),
        "way_this_info_was_added": way_this_info_was_added,
        "name_of_the_script": sys.argv[0],
        "datetime_of_annotation": datetime.now(timezone.utc).isoformat(),
    }


def write_segment_on_csv(
    path_to_file: Path,
    start: datetime,
    end: datetime,
    way_this_info_was_added: str = "manual",
    **kwargs,
):
    assert isinstance(path_to_file, Path)
    assert path_to_file.suffix == ".csv"
    assert isinstance(way_this_info_was_added, str)
    assert start < end
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert isinstance(way_this_info_was_added, str)

    mode = "w" if not path_to_file.exists() else "a"
    d = get_system_details(way_this_info_was_added)

    with path_to_file.open(mode, newline="") as file:
        # TODO_2: Add check if columns change from the header to what it's being saved

        # TODO_2: I'm not 100% sure we need csv
        writer = csv.writer(file)
        if mode == "w":
            writer.writerow(["start", "end", *d, *kwargs.keys()])
        writer.writerow([start, end, *d.values(), *[str(x) for x in kwargs.values()]])
