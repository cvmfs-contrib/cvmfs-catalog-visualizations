# -*- coding: utf-8 -*-
"""
Catalog tree builder for CVMFS visualization.

Traverses the catalog hierarchy and calculates cumulative download costs.
"""

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class CatalogNode:
    """Represents a node in the catalog hierarchy tree."""

    path: str
    hash: str
    size_bytes: int
    cumulative_cost: int
    depth: int
    children: List["CatalogNode"] = field(default_factory=list)
    is_large: bool = False
    is_root: bool = False
    is_virtual: bool = False  # True for intermediate path nodes without a catalog

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization.

        Omits fields that are redundant (computable from tree structure)
        or false-valued booleans to minimize JSON size.
        """
        d: dict = {
            "path": self.path,
            "hash": self.hash,
            "size": self.size_bytes,
        }
        if self.is_large:
            d["is_large"] = True
        if self.is_virtual:
            d["is_virtual"] = True
        if self.children:
            d["children"] = [child.to_dict() for child in self.children]
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "CatalogNode":
        """Construct a CatalogNode from a dictionary (inverse of to_dict()).

        Handles both the compact format (no depth/cumulative_cost/name/is_root)
        and the legacy format with all fields present. Missing depth and
        cumulative_cost are fixed by calling recalculate_tree() after loading.
        """
        return cls(
            path=data["path"],
            hash=data["hash"],
            size_bytes=data["size"],
            cumulative_cost=data.get("cumulative_cost", 0),
            depth=data.get("depth", 0),
            children=[cls.from_dict(c) for c in data.get("children", [])],
            is_large=data.get("is_large", False),
            is_root=data.get("is_root", False),
            is_virtual=data.get("is_virtual", False),
        )

    def find_or_create_child(self, path_segment: str, full_path: str, depth: int) -> "CatalogNode":
        """Find existing child with path or create a virtual intermediate node."""
        for child in self.children:
            if child.path == full_path:
                return child
            # Check if this child is along the path
            if full_path.startswith(child.path + "/"):
                return child

        # Create virtual intermediate node
        virtual = CatalogNode(
            path=full_path,
            hash="",
            size_bytes=0,
            cumulative_cost=self.cumulative_cost,
            depth=depth,
            is_virtual=True,
        )
        self.children.append(virtual)
        return virtual


def build_lookup(node: CatalogNode) -> Dict[str, CatalogNode]:
    """Build a path->node lookup dict via BFS for O(1) access."""
    lookup: Dict[str, CatalogNode] = {}
    queue_nodes = deque([node])
    while queue_nodes:
        current = queue_nodes.popleft()
        if not current.is_virtual:
            lookup[current.path] = current
        queue_nodes.extend(current.children)
    return lookup


def count_nodes(node: CatalogNode) -> int:
    """Count non-virtual nodes in a subtree."""
    count = 0
    stack = [node]
    while stack:
        current = stack.pop()
        if not current.is_virtual:
            count += 1
        stack.extend(current.children)
    return count


def recalculate_tree(root: CatalogNode) -> None:
    """Fix cumulative_cost and depth for all nodes top-down.

    Needed because grafted subtrees have stale values from the
    previous tree's parent chain.
    """
    stack = [(root, None)]
    while stack:
        node, parent = stack.pop()
        if parent is None:
            # Root node: depth 0, cost = own size
            node.depth = 0
            node.cumulative_cost = node.size_bytes
        else:
            node.depth = parent.depth + 1
            node.cumulative_cost = parent.cumulative_cost + node.size_bytes
        for child in node.children:
            stack.append((child, node))
