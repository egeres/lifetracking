import json
from typing import Callable

import pandas as pd


def export_pddataframe_to_lc_single(
    df: pd.DataFrame,
    fn: Callable[[pd.Series], str],
    path_filename: str,
):
    assert callable(fn), "fn must be callable"
    assert isinstance(df, pd.DataFrame)
    assert isinstance(path_filename, str)
    assert path_filename.endswith(".json")
    to_export = []
    for _, i in df.iterrows():
        to_export.append({"start": fn(i)})
    with open(path_filename, "w") as f:
        json.dump(to_export, f, indent=4, default=str)
