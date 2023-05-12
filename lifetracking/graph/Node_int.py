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
    def __init__(self, node: Node_int) -> None:
        self.node = node

    def _operation(self, node, t=None) -> int:
        time.sleep(t or 0)
        return node + 1

    def _run_sequential(self, t=None, context=None) -> int | None:
        # Depending on the context, the node can be already computed!
        node_output = None
        if context is not None and self.node in context:
            node_output = context[self.node]
        else:
            node_output = self.node._run_sequential(t, context)
        if context is not None:
            context[self.node] = node_output

        # The operation is performed
        return self._operation(
            node_output,
            t,
        )

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        node_output = None
        if context is not None and self.node in context:
            node_output = context[self.node]
        else:
            node_output = self.node._make_prefect_graph(t, context)
        if context is not None:
            context[self.node] = node_output

        # The operation is performed
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            node_output, t
        )

    def _get_children(self) -> set[Node]:
        return {self.node}


class Node_int_addition(Node_int):
    def __init__(self, n0: Node_int, n1: Node_int) -> None:
        self.n0 = n0
        self.n1 = n1

    def _operation(self, n0: int, n1: int, t=None) -> int:
        time.sleep(t or 0)
        return n0 + n1

    def _run_sequential(self, t=None, context=None) -> int | None:
        n0_out = self._get_value_from_context_or_run(self.n0, t, context)
        n1_out = self._get_value_from_context_or_run(self.n1, t, context)

        # The operation is performed
        return self._operation(
            n0_out,
            n1_out,
            t,
        )

    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        n0_out = self._get_value_from_context_or_makegraph(self.n0, t, context)
        n1_out = self._get_value_from_context_or_makegraph(self.n1, t, context)

        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            n1_out,
            t,
        )

    def _get_children(self) -> set[Node]:
        return {self.node1, self.node2}
