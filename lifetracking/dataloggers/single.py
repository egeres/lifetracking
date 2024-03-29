"""
Meant to be used for single data points, like weight, height, etc.
"""

import csv
import datetime
import json
import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path


def write_single_on_csv(
    path_to_file: Path, way_this_info_was_added: str = "manual", **kwargs
):

    assert isinstance(path_to_file, Path)
    assert path_to_file.suffix == ".csv"
    assert isinstance(way_this_info_was_added, str)

    mode = "w" if not path_to_file.exists() else "a"

    with open(path_to_file, mode, newline="") as file:
        writer = csv.writer(file)
        if mode == "w":
            writer.writerow(
                [
                    "datetime",
                    "os.login()",
                    "platform.system()",
                    "machine_name",
                    "way_this_info_was_added",
                    "name_of_the_script",
                    # Extra data...
                    *kwargs.keys(),
                ]
            )

        # TODO_2: Add check in case columns change from the header to what it's being
        # saved

        writer.writerow(
            [
                datetime.datetime.now(datetime.timezone.utc),
                os.getlogin(),
                platform.system(),
                os.getenv("COMPUTERNAME", os.getenv("HOSTNAME")),
                way_this_info_was_added,
                sys.argv[0],
                # Extra data...
                *[str(x) for x in kwargs.values()],
            ]
        )


def write_single_on_dated_json(
    path_to_dir: Path, way_this_info_was_added: str = "manual", **kwargs
):

    assert isinstance(path_to_dir, Path)
    assert path_to_dir.is_dir()
    assert isinstance(way_this_info_was_added, str)

    now = datetime.datetime.now(datetime.timezone.utc)

    df: list[dict] = []
    if os.path.exists(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json"):
        with open(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json", "r+") as f:
            df = json.load(f)
    df.append(
        {
            "datetime": now.isoformat(),
            "os.login()": os.getlogin(),
            "platform.system()": platform.system(),
            "machine_name": os.getenv("COMPUTERNAME", os.getenv("HOSTNAME")),
            "way_this_info_was_added": way_this_info_was_added,
            "name_of_the_script": sys.argv[0],
        }
        | kwargs
    )
    with open(path_to_dir / f"{now.strftime('%Y-%m-%d')}.json", "w") as f:
        json.dump(df, f, indent=4)


# TODO_2: Add write_segment_on_csv
# TODO_2: Add write_segment_on_dated_json

if __name__ == "__main__":

    from pydantic import BaseModel

    class Person_0(BaseModel):
        name: str
        age: int = 99

    @dataclass
    class Person_1:
        name: str
        age: int

    now = datetime.datetime.now(datetime.timezone.utc)
    file = Path(f"C:/Shared/deletemee_{now.strftime('%Y-%m-%d')}.csv")

    write_single_on_csv(file, "manual", weight=70, height=1.70)
