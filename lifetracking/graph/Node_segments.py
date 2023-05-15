from __future__ import annotations

from typing import Any

from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.utilities.asyncutils import Sync

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval


class Node_segments(Node[Segments]):
    def __init__(self) -> None:
        super().__init__()


class Node_segments_generate(Node_segments):
    def __init__(self, value: Segments) -> None:
        super().__init__()
        self.value = value

    def _get_children(self) -> list[Node]:
        return []

    def _operation(self, t: Time_interval | None = None) -> Segments:
        return self.value

    def _hash_node(self):
        return super()._hash_node() + hash(self.value)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[Segments, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> Segments | None:
        return self._operation(t)
