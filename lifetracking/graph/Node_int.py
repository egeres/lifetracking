"""Int nodes are mostly intended to be used for debugging and testing purposes."""

from __future__ import annotations

import time
from abc import abstractmethod
from typing import Optional

from prefect import task as prefect_task
from prefect.futures import PrefectFuture

from lifetracking.graph.Node import Node


class Node_int(Node):
    @abstractmethod
    def _operation(self, t=None) -> int:
        ...


class Node_int_generate(Node_int):
    def __init__(self, value: int) -> None:
        self.value = value

    def _operation(self, t=None) -> int:
        time.sleep(t or 0)
        return self.value

    def _run_sequential(self, t=None, context=None) -> int | None:
        return self._operation(t)

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)

    def _get_children(self) -> set[Node]:
        return set()

    def __add__(self, other: Node_int) -> Node_int_addition:
        return Node_int_addition(self, other)


class Node_int_singleincrement(Node_int):
    def __init__(self, n0: Node_int) -> None:
        self.n0 = n0

    def _operation(self, n0, t=None) -> int:
        time.sleep(t or 0)
        return n0 + 1

    def _run_sequential(self, t=None, context=None) -> int | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        return self._operation(
            n0_out,
            t,
        )

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )

    def _get_children(self) -> set[Node]:
        return {self.n0}


class Node_int_addition(Node_int):
    def __init__(self, n0: Node_int, n1: Node_int) -> None:
        self.n0 = n0
        self.n1 = n1

    def _operation(self, n0: int, n1: int, t=None) -> int:
        time.sleep(t or 0)
        return n0 + n1

    def _run_sequential(self, t=None, context=None) -> int | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        n1_out = self._get_value_from_context_or_run(self.n1, t, context)
        return self._operation(
            n0_out,
            n1_out,
            t,
        )

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        n1_out = self._get_value_from_context_or_makegraph(self.n1, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            n1_out,
            t,
        )

    def _get_children(self) -> set[Node]:
        return {self.n0, self.n1}
