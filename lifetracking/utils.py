from __future__ import annotations

import datetime
import dis
import hashlib
import inspect
import json
import os
import pickle
import tempfile
from typing import Callable

import pandas as pd
import plotly.graph_objects as go
from dateutil.parser import parse

from lifetracking.graph.Time_interval import Time_interval


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


def export_pddataframe_to_lc_single(
    df: pd.DataFrame,
    fn: Callable[[pd.Series], str],  # Specifies a way to get the "start"
    path_filename: str,
    # hour_offset: float = 0.0,
    color: str | Callable[[pd.Series], str] | None = None,
    opacity: float | Callable[[pd.Series], float] = 1.0,
    # No tooltip here!
):
    """Long calendar is a custom application of mine that I use to visualize
    my data."""

    # Assertions
    assert isinstance(path_filename, str)
    if not path_filename.endswith(".json"):
        raise ValueError("path_filename must end with .json")
    assert os.path.split(path_filename)[-1] != "config.json"

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

    # Other assertions
    assert isinstance(df, pd.DataFrame)
    assert callable(fn), "fn must be callable"

    # Dir setup
    _lc_export_prepare_dir(path_filename)

    # Changes at config.json
    if isinstance(color, str) or isinstance(color, float):
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

    # Export itself
    to_export = []
    base_dict = {}
    for _, i in df.iterrows():
        if isinstance(color, Callable):
            base_dict["color"] = color(i)
        if isinstance(opacity, Callable):
            base_dict["opacity"] = opacity(i)
        to_export.append({"start": fn(i)} | base_dict)
    with open(path_filename, "w") as f:
        json.dump(to_export, f, indent=4, default=str)


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


def cache_singleargument(dirname: str) -> Callable:
    # Create folder if not exists
    path_dir = os.path.join(tempfile.gettempdir(), dirname)
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)

    def decorator(method: Callable) -> Callable:
        def wrapper(arg: str) -> str:
            caches_existing = [
                x.split(".pickle")[0]
                for x in os.listdir(path_dir)
                if x.endswith(".pickle")
            ]
            hash_arg = hash_string(arg)

            if hash_arg in caches_existing:
                with open(os.path.join(path_dir, f"{hash_arg}.pickle"), "rb") as f:
                    return pickle.load(f)
            else:
                to_return = method(arg)
                if to_return is not None:  # Huh, should I?
                    with open(os.path.join(path_dir, f"{hash_arg}.pickle"), "wb") as f:
                        pickle.dump(to_return, f)
                return to_return

        return wrapper

    return decorator


def graph_udate_layout(fig, t: Time_interval | None):
    c = fig.data[0].x
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=0, r=0, t=0, b=0),  # Is this a good idea tho?
        # To show it in "days ago"
        xaxis=dict(
            tickmode="array",
            # tickvals = list(range(len(c))),
            # ticktext = list(range(-len(c), 0))
            tickvals=list(range(0, len(c), 30)),  # Tick every 30
            ticktext=list(range(-len(c), 0, 30)),  # Tick every 30
            # TODO: Actually, this should vary depending on the time scale
        ),
    )


def graph_annotate_today(
    fig,
    t: Time_interval,
    minmax: tuple[float, float] | None = None,
):
    fig_min, fig_max = (0, 1) if minmax is None else minmax

    today = datetime.datetime.now()
    days_diff = (today - t.start).days
    if days_diff >= 0 and days_diff <= int(t.duration_days):
        fig.add_shape(
            type="line",
            x0=days_diff,
            x1=days_diff,
            y0=fig_min,
            y1=fig_max,
            line={"color": "#a00"},
            xref="x",
            yref="y",
        )


def graph_annotate_annotations(
    fig,
    t: Time_interval,
    annotations: list | None,
    minmax: tuple[float, float] | None = None,
):
    assert isinstance(fig, go.Figure)
    if annotations is None:
        return

    for i in annotations:
        # Days diff
        date = i["date"]
        if isinstance(date, str):
            date = parse(date)
        assert isinstance(date, datetime.datetime)
        days_diff = (date - t.start).days

        if not (days_diff >= 0 and days_diff <= int(t.duration_days)):
            continue

        fig_min, fig_max = (0, 1) if minmax is None else minmax
        if fig_min > 0:
            fig_min = 0

        fig.add_shape(
            type="line",
            x0=days_diff,
            x1=days_diff,
            y0=fig_min,
            y1=fig_max,
            line={"color": "#aaa", "dash": "dash"},
            xref="x",
            yref="y",
        )
        if "title" in i:
            fig.add_annotation(
                x=days_diff,
                y=fig_min,
                text=i["title"],
                showarrow=False,
                xref="x",
                yref="y",
                yshift=10,
            )
