from __future__ import annotations

import dis
import hashlib
import json
import os
import tempfile
from typing import Callable

import pandas as pd


# TODO: Should probably be removed
def export_pddataframe_to_lc_single(
    df: pd.DataFrame,
    fn: Callable[[pd.Series], str],
    path_filename: str,
    color: str | None = None,
    opacity: float | None = None,
):
    assert callable(fn), "fn must be callable"
    assert isinstance(df, pd.DataFrame)
    assert isinstance(path_filename, str)
    assert path_filename.endswith(".json")
    assert color is None or isinstance(color, str)
    assert opacity is None or isinstance(opacity, float)

    # Base dict
    base_dict = {}
    if opacity is not None:
        base_dict["opacity"] = opacity
    if color is not None:
        base_dict["color"] = color

    # Export itself
    to_export = []
    for _, i in df.iterrows():
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
