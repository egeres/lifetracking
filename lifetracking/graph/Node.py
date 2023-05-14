from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Iterable, TypeVar

from prefect import flow as prefect_flow
from prefect.futures import PrefectFuture
from prefect.task_runners import ConcurrentTaskRunner
from prefect.utilities.asyncutils import Sync

T = TypeVar("T")


class Node(ABC, Generic[T]):
    """Abstract class for a node in the graph"""

    @abstractmethod
    def _get_children(self) -> set[Node]:
        """Returns a set with the children of the node"""
        ...

    @abstractmethod
    def _operation(self, t=None) -> T | None:
        """The main operation of the node"""
        ...

    @abstractmethod
    def _run_sequential(self, t=None, context=None) -> T | None:
        """Runs the graph sequentially"""
        ...

    @abstractmethod
    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        """Parses the graph to prefect"""
        ...

    def run(
        self,
        prefect: bool = False,
        t: Any = None,
        context: dict[Node, Any] | None = None,
    ) -> T | None:
        """Entry point to run the graph"""

        assert context is None or isinstance(context, dict)
        assert isinstance(prefect, bool)

        # Actual run
        if prefect:
            return self._run_prefect_graph(t)
        else:
            return self._run_sequential(t, context)

    def _run_prefect_graph(self, t=None) -> T | None:
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

    @staticmethod
    def _get_value_from_context_or_run(
        node: Node, t=None, context: dict[Node, Any] | None = None
    ) -> T | None:
        """This is intended to be used inside _run_sequential"""
        out = None
        if context is not None:
            out = context.get(node)
        if out is None:
            out = node._run_sequential(t, context)
        if context is not None and out is not None:
            context[node] = out
        return out

    @staticmethod
    def _get_value_from_context_or_makegraph(
        node: Node, t=None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[T, Sync]:
        """This is intended to be used inside _make_prefect_graph"""
        out = None
        if context is not None:
            out = context.get(node)
        if out is None:
            out = node._make_prefect_graph(t, context)
        if context is not None and out is not None:
            context[node] = out
        return out


def run_multiple(
    nodes_to_run: list[Node], t=None, prefect: bool = False
) -> Iterable[Any]:
    """Computes multiple nodes in sequence"""

    return [node.run(prefect, t) for node in nodes_to_run]


def run_multiple_parallel(
    nodes_to_run: list[Node], t=None, prefect: bool = False
) -> Iterable[Any | None]:
    """Runs multiple nodes in parallel"""

    if not prefect:
        # TODO Finish this
        # # Optimized version: Step 0, get all the common nodes IDs
        # nodes_children = [x._get_children() for x in nodes_to_run]
        # nodes_common = set.intersection(*nodes_children)

        # # Optimized version:
        # # Step 1, prune the common nodes that are children of other common nodes
        # nodes_common_pruned = nodes_common
        # to_remove = set()
        # for node in nodes_common:
        #     for node2 in nodes_common:
        #         if node2 in node._get_children():
        #             to_remove.add(node)
        # nodes_common_pruned = nodes_common - to_remove

        # nodes_common_pruned is not 100% needed, but it can be added to the context
        # to make it avoid storing nodes outputs that we know won't be
        # used further on? :[

        # Optimized version: Step 2, run the common nodes
        context: dict[Node, Any] = {}
        return [node._run_sequential(t, context) for node in nodes_to_run]

    else:
        # A flow is created
        @prefect_flow(task_runner=ConcurrentTaskRunner(), name="run_prefect_graph")
        def flow():
            context: dict[Node, Any] = {}
            tasks = [node._make_prefect_graph(t, context) for node in nodes_to_run]
            results = [task.result() for task in tasks]
            return results

        return flow()
