from __future__ import annotations

import copy
import hashlib
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from lifetracking_overhaul.core.timeinterval import TimeInterval

T = TypeVar("T")

TimeArg = TimeInterval | int | timedelta


class Node[T](ABC):
    """Abstract class for a node in the graph"""

    def __init__(self):
        self.last_datetime_run: datetime | None = None

    def __repr__(self) -> str:
        if getattr(self, "name", None) is not None:
            return f"{self.__class__.__name__}({self.name})"
        return self.__class__.__name__

    def run(self, t: TimeArg) -> T | None:
        """Entry point to run the graph"""

        # t-management
        if isinstance(t, int):
            t = TimeInterval.last_n_days(t)
        elif isinstance(t, timedelta):
            t = TimeInterval.last_n_days(t.days)
        t = copy.copy(t)  # TODO_2: Try to remove it and see what breaks in tests etc

        # Actual run
        to_return = self._run_sequential(t)

        # Post
        self.last_datetime_run = datetime.now(UTC)

        return to_return

    @abstractmethod
    def _hashstr(self) -> str:
        """Returns a custom hash of the node which should take into account
        configurations etc..."""
        return hashlib.md5(self.__class__.__name__.encode()).hexdigest()

    @abstractmethod
    def _run_sequential(
        self, t: TimeArg, context: dict[Node, Any] | None = None
    ) -> T | None:
        """Runs the graph sequentially"""
        ...  # pragma: no cover


# OTHER NODES ##########################################################################


class Node_0child(Node[T]):
    @abstractmethod
    def _operation(self, t: TimeArg) -> T | None:
        """Main operation of the node"""
        ...  # pragma: no cover

    def _get_children(self) -> list[Node]:
        return []

    def _run_sequential(
        self, t: TimeArg, context: dict[Node, Any] | None = None
    ) -> T | None:
        if not self._available():
            print(f"Node {self} is not available...")
            return None
        return self._operation(t)


class Node_1child(Node[T]):
    @abstractmethod
    def _operation(self, n0: Node, t: TimeArg) -> T | None:
        """Main operation of the node"""
        ...  # pragma: no cover

    def _get_children(self) -> list[Node]:
        return [self.child]

    def _run_sequential(
        self, t: TimeArg, context: dict[Node, Any] | None = None
    ) -> T | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.child, t, context)
        if n0_out is None:
            return None
        return self._operation(
            n0_out,
            t,
        )
