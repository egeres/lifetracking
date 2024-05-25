from __future__ import annotations

import dis
import hashlib
import inspect
import json
import os
import pickle
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import plotly.graph_objects as go
from pandas.core.resample import DatetimeIndexResampler

from lifetracking.graph.Time_interval import Time_interval
from lifetracking.plots.graphs import (
    graph_annotate_annotations,
    graph_annotate_title,
    graph_annotate_today,
    graph_udate_layout,
)


def _lc_export_prepare_dir(path_filename: str) -> None:
    """Takes a path like ..."""

    # Intermediate folders are created
    if not os.path.exists(os.path.split(path_filename)[0]):
        os.makedirs(os.path.split(path_filename)[0])

    # If file does not exist, it is created
    if not os.path.exists(path_filename):
        with open(path_filename, "w") as f:
            json.dump({}, f)

    # If file is empty, it is filled with an empty dict
    with open(path_filename) as f:
        if f.read().strip() == "":
            with open(path_filename, "w") as f:
                json.dump({}, f)

    # If config.json does not exist, it is created
    path_fil_config = os.path.join(os.path.split(path_filename)[0], "config.json")
    if not os.path.exists(path_fil_config):
        with open(path_fil_config, "w") as f:
            json.dump({}, f)

    # If config.json is empty, it is filled with an empty dict
    with open(path_fil_config) as f:
        if f.read().strip() == "":
            with open(path_fil_config, "w") as f:
                json.dump({}, f)

    # If the data key does not exist, it is created
    with open(path_fil_config) as f:
        data = json.load(f)
        if "data" not in data:
            data["data"] = {}
    with open(path_fil_config, "w") as f:
        json.dump(data, f, indent=4)


def _lc_export_configchanges(
    path_filename: str,
    color: str | Any,
    opacity: float | Any,
) -> None:
    """Takes a path like .../data/meditation.json"""

    if isinstance(color, str) or isinstance(opacity, float):
        # Data parsing
        path_fil_config = os.path.join(os.path.split(path_filename)[0], "config.json")
        with open(path_fil_config) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            key_name = os.path.split(path_filename)[1].split(".")[0]
            if key_name not in data["data"]:
                data["data"][key_name] = {}

        # We write color and opacity
        if isinstance(color, str):
            data["data"][key_name]["color"] = f"{color}"
        if isinstance(opacity, float) and opacity != 1.0:
            data["data"][key_name]["opacity"] = opacity

        with open(path_fil_config, "w") as f:
            json.dump(data, f, indent=4)


def export_pddataframe_to_lc_single(
    df: pd.DataFrame,
    path_filename: str | Path,
    time_offset: timedelta | None = None,
    color: str | Callable[[pd.Series], str] | None = None,
    opacity: float | Callable[[pd.Series], float] = 1.0,
):
    """Long calendar is a custom application of mine that I use to visualize my
    data. No tooltip support is intentional!"""

    # Assertions
    assert isinstance(path_filename, (str, Path))
    # TODO_2: Remove this and only use pathlib
    if isinstance(path_filename, Path):
        path_filename = str(path_filename)
    if not path_filename.endswith(".json"):
        msg = "path_filename must end with .json"
        raise ValueError(msg)
    assert os.path.split(path_filename)[-1] != "config.json"
    if time_offset is None:
        time_offset = timedelta()
    assert isinstance(time_offset, timedelta)
    assert isinstance(df, pd.DataFrame)

    # Assertion of color, opacity and tooltip
    assert color is None or isinstance(color, str) or callable(color)
    assert (
        color is None
        or isinstance(color, str)
        or len(inspect.signature(color).parameters) == 1
    )
    assert isinstance(opacity, float) or callable(opacity)
    assert (
        opacity is None
        or isinstance(opacity, float)
        or len(inspect.signature(opacity).parameters) == 1
    )

    # Dir setup
    _lc_export_prepare_dir(path_filename)

    # Changes at config.json
    _lc_export_configchanges(path_filename, color, opacity)

    # Export itself
    to_export = []
    base_dict = {}
    for n, i in df.iterrows():
        if isinstance(color, Callable):
            base_dict["color"] = color(i)
        if isinstance(opacity, Callable):
            base_dict["opacity"] = opacity(i)

        if isinstance(n, pd.Timestamp):
            to_export.append({"start": n + time_offset} | base_dict)
        else:
            msg = "Index must be a pd.Timestamp"
            raise TypeError(msg)
    with open(path_filename, "w") as f:
        json.dump(to_export, f, indent=4, default=str)


# TODO_1: Rename to something like "Calculate_hash_from_method" or more verbose
# DOCS: Docstring on this bs
def hash_method(method: Callable) -> str:
    z = ""
    for i in dis.get_instructions(method):
        # Skipping RESUME instruction
        # https://github.com/python/cpython/issues/91201
        if i.opcode == 151:
            continue
        z += str(i.arg)
        z += i.argrepr
        z += str(i.is_jump_target)
        # z += str(i.offset) # Skipping this for now...
        z += str(i.opcode)
        z += ";"
    return hashlib.md5(z.encode()).hexdigest()


def hash_string(string: str) -> str:
    return hashlib.md5(string.encode()).hexdigest()


def cache_singleargument(dirname: str, rootdir: Path | None = None) -> Callable:

    if rootdir is None:
        rootdir = Path(tempfile.gettempdir())
    assert isinstance(rootdir, Path)
    assert rootdir.is_dir()
    assert isinstance(dirname, str)

    d = rootdir / dirname
    d.mkdir(exist_ok=True, parents=True)

    def decorator(method: Callable) -> Callable:
        def wrapper(arg: str) -> str:
            arg_hash = hash_string(arg)
            if arg_hash in {x.name[:-7] for x in d.glob("*.pickle")}:
                with open(d / f"{arg_hash}.pickle", "rb") as f:
                    return pickle.load(f)
            to_return = method(arg)
            if to_return is not None:  # Huh, should I?
                with open(d / f"{arg_hash}.pickle", "wb") as f:
                    pickle.dump(to_return, f)
            return to_return

        return wrapper

    return decorator


def plot_empty(
    t: Time_interval | None = None,
    title: str | None = None,
    annotations: list | None = None,
) -> go.Figure:
    fig = go.Figure()

    graph_udate_layout(fig, t)
    graph_annotate_title(fig, title)
    fig_min, fig_max = 0, 1
    if t is not None:
        graph_annotate_today(fig, t, (fig_min, fig_max))
        graph_annotate_annotations(fig, t, annotations, (fig_min, fig_max))
    return fig


def operator_resample_stringified(
    df: DatetimeIndexResampler, operator: str
) -> pd.DataFrame:
    """Takes a "max", "sum", etc... and applies it"""

    assert isinstance(df, DatetimeIndexResampler)
    assert isinstance(operator, str)
    assert operator in ["avg", "sum", "max", "min"]

    if operator == "avg":
        return df.mean()
    if operator == "sum":
        return df.sum()
    if operator == "max":
        return df.max()
    if operator == "min":
        return df.min()

    msg = "mode must be one of avg, sum, max, min (for now!!)"
    raise ValueError(msg)
