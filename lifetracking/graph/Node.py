from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from prefect import flow as prefect_flow
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.task_runners import ConcurrentTaskRunner
from rich import print


class Node(ABC):
    """Abstract class for a node in the graph"""

    @abstractmethod
    def _operation(self, t=None) -> Any | None:
        """The main operation of the node"""
        ...

    @abstractmethod
    def _run_sequential(self, t=None, context=None) -> Any | None:
        """Runs the graph sequentially"""
        ...

    @abstractmethod
    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture:
        """Parses the graph to prefect"""
        ...

    @abstractmethod
    def _get_children(self) -> set[Node]:
        """Returns a set with the children of the node"""
        ...

    def run(
        self,
        prefect: bool = False,
        context: dict[Node, Any] | None = None,
        t: Any = None,
    ) -> Any | None:
        """Entry point to run the graph"""

        assert context is None or isinstance(context, dict)
        assert isinstance(prefect, bool)

        # Actual run
        if prefect:
            return self._run_prefect_graph(t)
        else:
            return self._run_sequential(t, context)

    def _run_prefect_graph(self, t=None) -> Any | None:
        """Run the graph using prefect concurrently"""

        # A flow is created
        @prefect_flow(task_runner=ConcurrentTaskRunner(), name="run_prefect_graph")
        def flow():
            return self._make_prefect_graph(t).result()

        # Then is executed
        return flow()

    def _get_children_tree(self) -> set[Node]:
        """Returns a set with the children of the node"""
        children = self._get_children()
        for child in children:
            children = children | child._get_children_tree()
        return children
