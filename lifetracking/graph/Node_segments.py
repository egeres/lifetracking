from __future__ import annotations

from lifetracking.datatypes.Segment import Segments
from lifetracking.graph.Node import Node


class Node_segments(Node[Segments]):
    def __init__(self) -> None:
        super().__init__()
