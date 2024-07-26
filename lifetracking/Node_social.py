from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from rich import print

from lifetracking.graph.Node import Node_0child
from lifetracking.graph.Node_pandas import Node_pandas

if TYPE_CHECKING:
    from lifetracking.graph.Time_interval import Time_interval


# TEST
# DOCS
# TODO_2: Warning if it finds a chat with "null" name in a resul.json file
# TODO_2 Some short of "id"-"name" mapping for the chats
class Social_telegram(Node_pandas, Node_0child):
    def __init__(
        self,
        names: list[str] | None = None,
        type_of_chat: str | None = "personal_chat",
        path_to_data: Path | None = None,
    ) -> None:
        super().__init__()
        assert type_of_chat is None or isinstance(type_of_chat, str)
        assert names is None or isinstance(names, list)
        assert path_to_data is None or isinstance(path_to_data, Path)

        self.type_of_chat = (
            [type_of_chat] if isinstance(type_of_chat, str) else type_of_chat
        )
        self.names = names if names is not None else []

        if path_to_data is None:
            path_to_data = Path.home() / "Downloads" / "Telegram Desktop"
        self.path_to_data = path_to_data

    def _hashstr(self) -> str:
        return super()._hashstr()

    def _get_chat_exports_dirs(self, path_dir_root: Path) -> list[Path]:
        assert path_dir_root.exists()

        return [
            i
            for i in path_dir_root.iterdir()
            if i.is_dir() and i.name.startswith("ChatExport")
        ]

    def _get_datajsons(self, path_dir_root: Path) -> list[Path]:
        assert path_dir_root.exists()

        return [
            j
            for i in self._get_chat_exports_dirs(path_dir_root)
            for j in i.iterdir()
            if j.suffix == ".json" and j.is_file()
        ]

    def get_most_recent_personal_chats(self) -> dict[str, dict[str, Any]]:
        to_return = {}

        for filename in self._get_datajsons(self.path_to_data):
            with filename.open(encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.decoder.JSONDecodeError:
                    continue

            if data["type"] not in self.type_of_chat:
                continue

            name = data["name"] if data["name"] is not None else filename.split(".")[0]
            last_update = data["messages"][-1]["date"]
            last_update = pd.to_datetime(last_update, format="%Y-%m-%dT%H:%M:%S")

            if data["id"] not in to_return:
                to_return[data["id"]] = {}
                to_return[data["id"]]["filename"] = filename
                to_return[data["id"]]["type"] = data["type"]
                to_return[data["id"]]["name"] = name
                to_return[data["id"]]["last_update"] = last_update
            else:
                if last_update <= to_return[data["id"]]["last_update"]:
                    continue
                to_return[data["id"]]["filename"] = filename
                to_return[data["id"]]["type"] = data["type"]
                to_return[data["id"]]["name"] = name
                to_return[data["id"]]["last_update"] = last_update

        gathered_names = [v["name"] for v in to_return.values()]
        for name in self.names:
            if name not in gathered_names:
                print(f"[yellow]WARNING[/yellow]: {name} not found in the data")

        return to_return

    def _operation(self, t: Time_interval | None = None) -> pd.DataFrame | None:
        chats = self.get_most_recent_personal_chats()

        # Filter by name
        if self.names is not None:
            chats = {k: v for k, v in chats.items() if v["name"] in self.names}

        # Filter by type (actually, this is redundant and might be removed in
        # the future...)
        if self.type_of_chat is not None:
            chats = {k: v for k, v in chats.items() if v["type"] in self.type_of_chat}

        # Load data
        to_return = []
        for k, v in chats.items():
            # Read messages
            with Path(v["filename"]).open(encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.decoder.JSONDecodeError:
                    continue

            # Df time
            df = pd.DataFrame(data["messages"])
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%dT%H:%M:%S")
            if t is not None:
                df = df[df["date"] >= t.start]
                df = df[df["date"] <= t.end]
            df["chat_id"] = k
            df["chat_name"] = v["name"]
            df.set_index("date", inplace=True)
            to_return.append(df)

        # Return
        if len(to_return) == 0:
            return pd.DataFrame()
        return pd.concat(to_return)
