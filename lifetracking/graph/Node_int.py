"""Int nodes are mostly intended to be used for debugging and testing purposes.

They include an argument to simulate a delay, so that the execution of the graph can be
observed."""

from __future__ import annotations

import hashlib
import time
from typing import Any

from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval


class Node_int(Node[int]):
    def __init__(self) -> None:
        super().__init__()


class Node_int_generate(Node_int):
    def __init__(self, value: int, artificial_delay: float = 0) -> None:
        super().__init__()
        self.value = value
        self.artificial_delay = artificial_delay

    def _get_children(self) -> list[Node]:
        return []

    def _operation(self, t: Time_interval | None = None) -> int:
        time.sleep(self.artificial_delay)
        return self.value

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> int | None:
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[int, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + str(self.value)).encode()).hexdigest()

    def __add__(self, other: Node_int) -> Node_int_addition:
        return Node_int_addition(self, other)


class Node_int_singleincrement(Node_int):
    def __init__(self, n0: Node_int, artificial_delay: float = 0) -> None:
        super().__init__()
        self.n0 = n0
        self.artificial_delay = artificial_delay

    def _get_children(self) -> list[Node]:
        return [self.n0]

    def _hashstr(self):
        return super()._hashstr()

    def _operation(
        self, n0: int | PrefectFuture[int, Sync], t: Time_interval | None = None
    ) -> int:
        time.sleep(self.artificial_delay)
        return n0 + 1  # type: ignore

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> int | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        if n0_out is None:
            return None
        return self._operation(
            n0_out,
            t,
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[int, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )


class Node_int_addition(Node_int):
    def __init__(self, n0: Node_int, n1: Node_int, artificial_delay: float = 0) -> None:
        super().__init__()
        self.n0 = n0
        self.n1 = n1
        self.artificial_delay = artificial_delay

    def _get_children(self) -> list[Node]:
        return [self.n0, self.n1]

    def _operation(
        self,
        n0: int | PrefectFuture[int, Sync],
        n1: int | PrefectFuture[int, Sync],
        t: Time_interval | None = None,
    ) -> int:
        time.sleep(self.artificial_delay)
        return n0 + n1  # type: ignore

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> int | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        n1_out = self._get_value_from_context_or_run(self.n1, t, context)
        if n0_out is None or n1_out is None:
            return None
        return self._operation(
            n0_out,
            n1_out,
            t,
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[int, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        n1_out = self._get_value_from_context_or_makegraph(self.n1, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            n1_out,
            t,
        )

    def _hashstr(self):
        return super()._hashstr()
