from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from lifetracking.graph.Node import Node

from lifetracking.graph.quantity import Quantity


class DataWarning(ABC):
    def __init__(self) -> None:
        self.node: None | Node = None

    @abstractmethod
    def run(self) -> bool:
        """Is there a problem? Y/N"""
        ...  # pragma: no cover

    @abstractmethod
    def error_message(self) -> str:
        """What is the problem?"""
        ...  # pragma: no cover

    @abstractmethod
    def __eq__(self, value: object) -> bool: ...  # pragma: no cover

    @abstractmethod
    def __hash__(self) -> int: ...  # pragma: no cover

    def __repr__(self) -> str:
        return f"🔷 {self.__class__.__name__}"


class DataWarning_NotUpdated(DataWarning):

    def __init__(self, interval: timedelta | int | float):
        """Assumes ints and floats as days."""

        super().__init__()
        if isinstance(interval, int):
            interval = timedelta(days=interval)
        elif isinstance(interval, float):
            interval = timedelta(seconds=int(interval * 24 * 60 * 60))
        assert isinstance(interval, timedelta)
        self.interval = interval
        self.node: None | Node = None

    def run(self) -> bool:
        if self.node is None:
            msg = "Node not set for this warning"
            raise ValueError(msg)
        last_data = self.node.run(Quantity(1))

        if last_data is None:
            return True
        if isinstance(last_data, pd.DataFrame):
            last_date = last_data.index.max()
            return last_date < pd.Timestamp.now(tz=last_date.tzinfo) - self.interval

        raise NotImplementedError

    def error_message(self) -> str:
        return f"Data not updated at least in the last {self.interval}"

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, DataWarning_NotUpdated):
            return False
        return self.interval == value.interval

    def __hash__(self) -> int:
        return hash(self.interval)
