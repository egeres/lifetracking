from __future__ import annotations

import copy
import hashlib
import time
from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Callable, Generic, Iterable, TypeVar

import pandas as pd
from prefect import flow as prefect_flow
from prefect import task as prefect_task
from prefect.futures import PrefectFuture
from prefect.task_runners import ConcurrentTaskRunner
from prefect.utilities.asyncutils import Sync
from rich import print

from lifetracking.datatypes.Segments import Segments
from lifetracking.graph.Time_interval import Time_interval

T = TypeVar("T")
U = TypeVar("U")


class Node(ABC, Generic[T]):
    """Abstract class for a node in the graph"""

    def __init__(self):
        self.last_run_info: dict[str, Any] | None = None
        self.name: str | None = None
        self.default_export: Callable = self._print_default_export_hasnt_been_defined
        self.final: bool = False

    def __repr__(self) -> str:
        if getattr(self, "name", None) is not None:
            return f"{self.__class__.__name__}({self.name})"
        return self.__class__.__name__

    @property
    def children(self) -> list[Node]:
        return self._get_children()

    @property
    def available(self) -> bool:
        """Is the node available? 🤔"""
        return self._available()

    @abstractmethod
    def _get_children(self) -> list[Node]:
        """Returns a set with the children of the node"""
        ...  # pragma: no cover

    @abstractmethod
    def _hashstr(self) -> str:
        """Returns a custom hash of the node which should take into account
        configurations etc..."""
        return hashlib.md5(self.__class__.__name__.encode()).hexdigest()

    # @abstractmethod
    def _available(self) -> bool:
        """Returns whether the node is available to be run or not"""
        return self._children_are_available()

    @abstractmethod
    def _operation(self, t=None) -> T | None:
        """Main operation of the node"""
        ...  # pragma: no cover

    @abstractmethod
    def _run_sequential(self, t=None, context=None) -> T | None:
        """Runs the graph sequentially"""
        ...  # pragma: no cover

    @abstractmethod
    def _make_prefect_graph(self, t=None, context=None) -> PrefectFuture[T, Sync]:
        """Parses the graph to prefect"""
        ...  # pragma: no cover

    def run(
        self,
        t: Time_interval | None = None,
        prefect: bool = False,
        context: dict[Node, Any] | None = None,
    ) -> T | None:
        """Entry point to run the graph"""

        assert context is None or isinstance(context, dict)
        assert isinstance(prefect, bool)
        assert t is None or isinstance(t, Time_interval)

        t = copy.copy(t)

        # context is changed
        if context is None:
            context = {}

        # Prepare stuff
        self.last_run_info = {}
        t0 = time.time()

        # Actual run
        to_return: T | None = None
        if prefect:
            to_return = self._run_prefect_graph(t, context)
        else:
            to_return = self._run_sequential(t, context)

        # Post-run stuff
        self.last_run_info["time"] = time.time() - t0
        self.last_run_info["run_mode"] = "prefect" if prefect else "sequential"
        # TODO: Add executed steps count
        # self.last_run_info["steps_executed"] =
        self.last_run_info["t_in"] = t
        if isinstance(to_return, Segments) and len(to_return) > 0:
            self.last_run_info["t_out"] = Time_interval(
                min(to_return).start, max(to_return).end
            )
        if isinstance(to_return, (Segments, pd.DataFrame, list)):
            self.last_run_info["len"] = len(to_return)
        elif isinstance(to_return, int):
            self.last_run_info["len"] = 1
        elif to_return is None:
            self.last_run_info["len"] = None
        else:
            raise NotImplementedError

        self.last_run_info["type"] = type(to_return).__name__

        if isinstance(to_return, pd.DataFrame):
            self.last_run_info["columns"] = list(to_return.columns)

        return to_return

    def print_stats(self):
        """Prints some statistics"""

        if self.last_run_info is None:
            print("No statistics available")
        else:
            duration_t_out = None
            if "t_out" in self.last_run_info:
                duration_t_out = self.last_run_info["t_out"].duration_days
                if duration_t_out >= 365:
                    duration_t_out = f"{round(duration_t_out/365, 1)} years"
                else:
                    duration_t_out = f"{round(duration_t_out, 1)} days"

            a = ""
            # print("")
            a += "Stats..." + f"({self.name})\n" if self.name is not None else "" + "\n"
            a += f"\t✨ Nodes   : {len(self._get_children_all()) + 1}\n"
            a += f'\t✨ Time    : {round(self.last_run_info["time"], 2)} sec\n'
            a += f'\t✨ Run mode: {self.last_run_info["run_mode"]}\n'
            a += f'\t✨ t in    : {self.last_run_info["t_in"]}\n'
            if "t_out" in self.last_run_info:
                a += f'\t✨ t out   : {self.last_run_info["t_out"]}\n'
            if duration_t_out is not None:
                a += f'\t             {f"({duration_t_out})"}\n'
            if "len" in self.last_run_info:
                a += f'\t✨ Length  : {self.last_run_info["len"]}\n'

            a += f'\t✨ Type    : {self.last_run_info["type"]}\n'
            if self.last_run_info["type"] == "DataFrame":
                a += f'\t✨ Columns : {", ".join(self.last_run_info["columns"])}'

            # Print name if defined
            # print timespan input and output
            # TODO: Add how many nodes were executed
            # Also, how many caches were computed n stuff like that
            # Nodes that took the most?

            print(a)
            # print("")

    def _run_prefect_graph(self, t=None, context=None) -> T | None:
        """Run the graph using prefect concurrently"""

        # A flow is created
        @prefect_flow(task_runner=ConcurrentTaskRunner(), name="run_prefect_graph")
        def flow():
            return self._make_prefect_graph(t, context).result()

        # Then is executed
        return flow()

    def _get_children_all(self) -> set[Node]:
        """Returns a set with all the children of the node"""
        children = set(self._get_children())
        for child in children:
            children = children | child._get_children_all()
        return children

    @staticmethod
    def _get_value_from_context_or_run(
        node: Node[U], t=None, context: dict[Node, Any] | None = None
    ) -> U | None:
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
        node: Node[U], t=None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[U, Sync]:
        """This is intended to be used inside _make_prefect_graph"""
        out = None
        if context is not None:
            out = context.get(node)
        if out is None:
            out = node._make_prefect_graph(t, context)
        if context is not None and out is not None:
            context[node] = out
        return out

    def hash_tree(self) -> str:
        """Hashes the tree of nodes"""
        hashes = [x.hash_tree() for x in self.children]
        summed = reduce(lambda x, y: x + y, hashes, "")
        return hashlib.md5((summed + self._hashstr()).encode()).hexdigest()

    def _children_are_available(self) -> bool:
        """Returns whether all the children are available"""
        return all(x._available() for x in self.children)

    # TODO_2: Unify all the export_to_longcalendar into a coherent interface, pls
    @abstractmethod
    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        hour_offset: float = 0.0,
        color: str | Callable[[T], str] | None = None,
        opacity: float | Callable[[T], float] = 1.0,
        # tooltip: str | Callable[[T], str] | None = None,
        # tooltip_shows_length: bool = False,
    ) -> None: ...  # pragma: no cover

    def set_default_export(
        self,
        path_filename: str,
        color: str | Callable[[T], str] | None = None,
        opacity: float | Callable[[T], float] = 1.0,
        hour_offset: float = 0,
    ) -> Node[T]:
        def default_export_pajas(t: Time_interval | None = None):
            self.export_to_longcalendar(
                t,
                path_filename=path_filename,
                color=color,
                opacity=opacity,
                hour_offset=hour_offset,
            )

        self.default_export = default_export_pajas
        self.final = True
        return self

    def _print_default_export_hasnt_been_defined(self, *args, **kwargs):
        print(
            f"Woops! Default export hasn't been defined for {self}. "
            "Please, use the method set_default_export to define it 🙃"
        )


class Node_0child(Node[T]):
    def _get_children(self) -> list[Node]:
        return []

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> T | None:
        if not self._available():
            print(f"Node {self} is not available...")
            return None
        return self._operation(t)

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[T | None, Sync]:
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(t)


class Node_1child(Node[T]):
    @abstractmethod
    def _operation(self, n0: Node, t: Time_interval | None) -> T | None:
        """Main operation of the node"""
        raise NotImplementedError

    @property
    @abstractmethod
    def child(self) -> Node:
        pass

    def _get_children(self) -> list[Node]:
        return [self.child]

    def _run_sequential(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> T | None:
        # Node is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_run(self.child, t, context)
        if n0_out is None:
            return None
        return self._operation(
            n0_out,
            t,
        )

    def _make_prefect_graph(
        self, t: Time_interval | None = None, context: dict[Node, Any] | None = None
    ) -> PrefectFuture[T | None, Sync]:
        # Node graph is calculated if it's not in the context, then _operation is called
        n0_out = self._get_value_from_context_or_makegraph(self.child, t, context)
        return prefect_task(name=self.__class__.__name__)(self._operation).submit(
            n0_out,
            t,
        )


class Node_generate_None(Node_0child[None]):
    """Generates None when run

    It's mostly intended to be used for testing purposes"""

    def _hashstr(self) -> str:
        return hashlib.md5((super()._hashstr() + str(None)).encode()).hexdigest()

    def _available(self) -> bool:
        return True

    def _operation(self, t: Time_interval | None = None) -> None:
        return None

    def export_to_longcalendar(
        self,
        t: Time_interval | None,
        path_filename: str,
        color: str | Callable[[None], str] | None = None,
        opacity: float | Callable[[None], float] = 1.0,
        hour_offset: float = 0,
    ):
        raise NotImplementedError


def run_multiple(
    nodes_to_run: list[Node], t=None, prefect: bool = False
) -> Iterable[Any]:
    """Computes multiple nodes in sequence"""
    return [node.run(t, prefect) for node in nodes_to_run]


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

    # A flow is created
    @prefect_flow(task_runner=ConcurrentTaskRunner(), name="run_prefect_graph")
    def flow():
        context: dict[Node, Any] = {}
        tasks = [node._make_prefect_graph(t, context) for node in nodes_to_run]
        return [task.result() for task in tasks]

    return flow()


# TODO_3: Add a run_sequential to simplify debugging pls, something like:
# run_sequential(
#   node_0,
#   node_1,
#   node_2,
# )
