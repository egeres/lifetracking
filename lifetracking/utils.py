import json


def export_pddataframe_to_lc_single(df, fn, path_filename: str):
    to_export = []
    for _, i in df.iterrows():
        to_export.append({"start": fn(i)})
    with open(path_filename, "w") as f:
        json.dump(to_export, f, indent=4, default=str)
