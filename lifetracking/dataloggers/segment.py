# TODO_2: Add write_segment_on_csv
# TODO_2: Add write_segment_on_dated_json


import csv
from datetime import datetime
from pathlib import Path

from lifetracking.dataloggers.systemdetails import get_system_details


def write_segment_on_csv(
    path_to_file: Path,
    start: datetime,
    end: datetime,
    way_this_info_was_added: str = "manual",
    **kwargs,
) -> None:
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
