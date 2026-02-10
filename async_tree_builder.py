# -*- coding: utf-8 -*-
"""
Async catalog tree builder for CVMFS visualization.

Uses asyncio for efficient parallel catalog downloads with HTTP/2 multiplexing.
"""

import asyncio
import logging
import os
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

from tree_builder import CatalogNode, build_lookup, count_nodes, recalculate_tree


class AsyncCatalogTreeBuilder:
    """Async tree builder using work queue pattern for parallel downloads.

    Leverages HTTP/2 connection multiplexing through AsyncRepository
    for efficient concurrent catalog downloads.
    """

    DEFAULT_STOP_THRESHOLD = 2 * 1024 * 1024  # 2 MB

    def __init__(
        self,
        repository,
        stop_threshold: int = DEFAULT_STOP_THRESHOLD,
        max_depth: Optional[int] = None,
        max_catalogs: Optional[int] = None,
        ignore_paths: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[dict], None]] = None,
        max_workers: int = 50,
        previous_tree: Optional[CatalogNode] = None,
    ):
        """Initialize the async tree builder.

        Args:
            repository: AsyncRepository object
            stop_threshold: Stop descending when catalog size exceeds this (bytes)
            max_depth: Maximum depth to traverse (None for unlimited)
            max_catalogs: Maximum catalogs to download (None for unlimited)
            ignore_paths: List of path prefixes to ignore
            progress_callback: Optional callback for progress updates
            max_workers: Number of async workers (default: 10)
            previous_tree: Optional CatalogNode tree from a previous run for caching
        """
        self.repository = repository
        self.stop_threshold = stop_threshold
        self.max_depth = max_depth
        self.max_catalogs = max_catalogs
        self.ignore_paths = ignore_paths or []
        self.progress_callback = progress_callback
        self.max_workers = max_workers
        self._previous_tree = previous_tree
        self._previous_lookup = build_lookup(previous_tree) if previous_tree else {}

        # Statistics (protected by lock for concurrent access)
        self._lock = asyncio.Lock()
        self._catalogs_downloaded = 0
        self._total_bytes_downloaded = 0
        self._catalogs_found = 0
        self._large_catalogs_found = 0
        self._head_requests = 0
        self._bytes_skipped = 0
        self._ignored_count = 0
        self._cache_hits = 0
        self._bytes_from_cache = 0
        self._tree_cache_reused = 0

    async def build(self) -> CatalogNode:
        """Build the catalog tree starting from the root.

        Returns:
            Root CatalogNode with populated children
        """
        # Get root catalog hash from manifest
        root_hash = self.repository.get_root_hash()

        # Check if root hash matches previous tree (zero downloads needed)
        if self._previous_tree and self._previous_tree.hash == root_hash:
            self._tree_cache_reused = count_nodes(self._previous_tree)
            recalculate_tree(self._previous_tree)
            return self._previous_tree

        root_in_cache = self._is_catalog_in_cache(root_hash)

        # Retrieve root catalog
        root_catalog, _ = await self.repository.retrieve_catalog(root_hash)

        root_size = root_catalog.db_size()
        self._catalogs_downloaded = 1
        self._total_bytes_downloaded = root_size
        self._catalogs_found = 1

        if root_in_cache:
            self._cache_hits = 1
            self._bytes_from_cache = root_size

        is_large = root_size > self.stop_threshold
        if is_large:
            self._large_catalogs_found = 1

        self._report_progress("/")

        root_node = CatalogNode(
            path="/",
            hash=root_catalog.hash,
            size_bytes=root_size,
            cumulative_cost=root_size,
            depth=0,
            is_root=True,
            is_large=is_large,
        )

        # Only descend if root is not too large and depth allows
        if not root_node.is_large and (self.max_depth is None or self.max_depth > 0):
            await self._populate_children_async(root_node, root_catalog)

        recalculate_tree(root_node)
        return root_node

    def _should_ignore(self, path: str) -> bool:
        """Check if a path should be ignored based on ignore_paths."""
        for ignore_prefix in self.ignore_paths:
            if path == ignore_prefix or path.startswith(ignore_prefix + "/"):
                return True
        return False

    def _report_progress(self, path: str) -> None:
        """Report progress via callback if available."""
        if self.progress_callback:
            self.progress_callback(
                {
                    "path": path,
                    "catalogs_downloaded": self._catalogs_downloaded,
                    "bytes_downloaded": self._total_bytes_downloaded,
                    "catalogs_found": self._catalogs_found,
                    "large_catalogs_found": self._large_catalogs_found,
                    "head_requests": self._head_requests,
                    "bytes_skipped": self._bytes_skipped,
                    "cache_hits": self._cache_hits,
                    "bytes_from_cache": self._bytes_from_cache,
                }
            )

    def _is_catalog_in_cache(self, catalog_hash: str) -> bool:
        """Check if a catalog exists in the disk cache."""
        cache_path = self.repository._fetcher.get_cache_path()
        if not cache_path:
            return False
        file_path = os.path.join(
            cache_path, "data", catalog_hash[:2], catalog_hash[2:] + "C"
        )
        return os.path.exists(file_path)

    async def _get_catalog_size(self, catalog_hash: str, ref_size: int) -> int:
        """Get catalog size, using HEAD request if ref_size is unknown.

        Args:
            catalog_hash: The catalog's hash
            ref_size: Size from CatalogReference (0 if unknown)

        Returns:
            Estimated size in bytes
        """
        if ref_size > 0:
            return ref_size

        # Size unknown, use HEAD request to get compressed size
        async with self._lock:
            self._head_requests += 1

        compressed_size = await self.repository.get_object_size(catalog_hash, "C")
        if compressed_size is not None:
            return compressed_size

        return 0

    def _get_path_segments(self, parent_path: str, child_path: str) -> List[str]:
        """Get the intermediate path segments between parent and child."""
        if parent_path == "/":
            parent_path = ""

        if not child_path.startswith(parent_path):
            return [child_path]

        relative = child_path[len(parent_path):]
        if relative.startswith("/"):
            relative = relative[1:]

        parts = relative.split("/")
        segments = []
        current = parent_path

        for part in parts:
            current = current + "/" + part if current else "/" + part
            segments.append(current)

        return segments

    def _insert_at_path(
        self,
        root_node: CatalogNode,
        parent_path: str,
        catalog_path: str,
        catalog_hash: str,
        catalog_size: int,
        is_large: bool,
    ) -> CatalogNode:
        """Insert a catalog node at the correct path location."""
        segments = self._get_path_segments(parent_path, catalog_path)

        current = root_node
        for i, seg_path in enumerate(segments):
            is_final = i == len(segments) - 1
            seg_depth = current.depth + 1

            if is_final:
                child_cost = current.cumulative_cost + catalog_size
                child_node = CatalogNode(
                    path=catalog_path,
                    hash=catalog_hash,
                    size_bytes=catalog_size,
                    cumulative_cost=child_cost,
                    depth=seg_depth,
                    is_large=is_large,
                )
                current.children.append(child_node)
                return child_node
            else:
                current = current.find_or_create_child(
                    seg_path.split("/")[-1], seg_path, seg_depth
                )

        return current

    async def _populate_children_async(
        self, root_node: CatalogNode, root_catalog
    ) -> None:
        """Populate all children using async work queue pattern.

        Uses asyncio.Queue with multiple workers that process catalog refs.
        Catalogs are listed and closed eagerly before enqueueing to avoid
        accumulating open aiosqlite connections (each holds a thread).
        """
        # Work queue holds (parent_node, nested_refs) tuples.
        # Catalogs are closed before enqueueing so no open DB connections
        # sit on the queue — only workers hold open connections.
        work_queue: asyncio.Queue = asyncio.Queue()

        # List root refs and close root catalog before enqueueing
        root_refs = await root_catalog.list_nested()
        await root_catalog.close()

        await work_queue.put((root_node, root_refs))

        # Track items in flight to know when we're done
        items_in_flight = 1
        done_event = asyncio.Event()

        async def worker():
            nonlocal items_in_flight

            while True:
                try:
                    # Wait for work with timeout
                    try:
                        parent_node, nested_refs = await asyncio.wait_for(
                            work_queue.get(), timeout=0.1
                        )
                    except asyncio.TimeoutError:
                        # Check if we should exit (must hold lock to read items_in_flight)
                        async with self._lock:
                            if items_in_flight == 0:
                                return
                        continue

                    for ref in nested_refs:
                        try:
                            result = await self._process_single_ref(parent_node, ref)
                            if result is not None:
                                child_node, child_catalog = result
                                if child_catalog is not None:
                                    # List refs and close catalog before enqueueing
                                    child_refs = await child_catalog.list_nested()
                                    await child_catalog.close()
                                    if child_refs:
                                        async with self._lock:
                                            items_in_flight += 1
                                        await work_queue.put((child_node, child_refs))
                        except Exception as e:
                            logger.warning(
                                "Failed to process catalog ref %s: %s",
                                ref.root_path,
                                e,
                                exc_info=True,
                            )

                    # Mark this item as done
                    async with self._lock:
                        items_in_flight -= 1
                        if items_in_flight == 0:
                            done_event.set()

                except Exception:
                    logger.exception("Worker error processing catalog")
                    async with self._lock:
                        items_in_flight -= 1
                        if items_in_flight == 0:
                            done_event.set()

        # Start worker tasks
        workers = [asyncio.create_task(worker()) for _ in range(self.max_workers)]

        # Wait for all work to complete
        await done_event.wait()

        # Cancel remaining workers
        for w in workers:
            w.cancel()

        # Wait for workers to finish
        await asyncio.gather(*workers, return_exceptions=True)

    def _graft_at_path(
        self,
        root_node: CatalogNode,
        parent_path: str,
        cached_node: CatalogNode,
    ) -> CatalogNode:
        """Graft a cached subtree at the correct path location.

        Like _insert_at_path but appends the cached node (with all children)
        instead of creating a new node.
        """
        segments = self._get_path_segments(parent_path, cached_node.path)

        current = root_node
        for i, seg_path in enumerate(segments):
            is_final = i == len(segments) - 1

            if is_final:
                current.children.append(cached_node)
                return cached_node
            else:
                seg_depth = current.depth + 1
                current = current.find_or_create_child(
                    seg_path.split("/")[-1], seg_path, seg_depth
                )

        return current

    async def _process_single_ref(self, parent_node: CatalogNode, ref):
        """Process a single catalog reference.

        Returns:
            Tuple of (child_node, child_catalog) if should recurse, else None
        """
        # Check if this path should be ignored
        if self._should_ignore(ref.root_path):
            async with self._lock:
                self._ignored_count += 1
            return None

        # Check previous tree cache before downloading
        cached_node = self._previous_lookup.get(ref.root_path)
        if cached_node is not None and cached_node.hash == ref.hash:
            reused = count_nodes(cached_node)
            async with self._lock:
                self._tree_cache_reused += reused
                self._catalogs_found += reused
                grafted = self._graft_at_path(
                    parent_node, parent_node.path, cached_node
                )
            return grafted, None

        async with self._lock:
            self._catalogs_found += 1

        # Get size - use HEAD request if ref.size is 0
        child_size = await self._get_catalog_size(ref.hash, ref.size)
        is_large = child_size > self.stop_threshold

        if is_large:
            async with self._lock:
                self._large_catalogs_found += 1
                self._bytes_skipped += child_size

        # Insert at correct path location
        async with self._lock:
            child_node = self._insert_at_path(
                parent_node,
                parent_node.path,
                ref.root_path,
                ref.hash,
                child_size,
                is_large,
            )

        # Check max depth
        if self.max_depth is not None and child_node.depth > self.max_depth:
            return child_node, None

        # Check if we've hit the download limit
        if self.max_catalogs is not None:
            async with self._lock:
                if self._catalogs_downloaded >= self.max_catalogs:
                    return child_node, None

        # Only descend into non-large catalogs
        if not is_large:
            # Check if in cache before retrieving
            in_cache = self._is_catalog_in_cache(ref.hash)

            # Download this catalog
            child_catalog, _ = await self.repository.retrieve_catalog(ref.hash)

            async with self._lock:
                self._catalogs_downloaded += 1
                catalog_size = child_catalog.db_size()
                self._total_bytes_downloaded += catalog_size

                if in_cache:
                    self._cache_hits += 1
                    self._bytes_from_cache += catalog_size

            self._report_progress(ref.root_path)

            # Update size with actual value if it was 0
            if child_node.size_bytes == 0:
                actual_size = child_catalog.db_size()
                async with self._lock:
                    child_node.size_bytes = actual_size
                    child_node.cumulative_cost = (
                        child_node.cumulative_cost - child_node.size_bytes + actual_size
                    )
                    child_node.is_large = actual_size > self.stop_threshold
                    if child_node.is_large:
                        self._large_catalogs_found += 1

            # Return catalog for recursion if still not large and within depth
            if not child_node.is_large and (
                self.max_depth is None or child_node.depth < self.max_depth
            ):
                return child_node, child_catalog

        return child_node, None

    @property
    def catalogs_downloaded(self) -> int:
        """Number of catalogs downloaded during tree building."""
        return self._catalogs_downloaded

    @property
    def total_bytes_downloaded(self) -> int:
        """Total bytes downloaded during tree building."""
        return self._total_bytes_downloaded

    @property
    def catalogs_found(self) -> int:
        """Total number of catalogs found (including those not downloaded)."""
        return self._catalogs_found

    @property
    def large_catalogs_found(self) -> int:
        """Number of large catalogs found (exploration stopped)."""
        return self._large_catalogs_found

    @property
    def head_requests(self) -> int:
        """Number of HEAD requests made to check catalog sizes."""
        return self._head_requests

    @property
    def bytes_skipped(self) -> int:
        """Total bytes skipped by not downloading large catalogs."""
        return self._bytes_skipped

    @property
    def ignored_count(self) -> int:
        """Number of catalogs ignored due to ignore_paths."""
        return self._ignored_count

    @property
    def cache_hits(self) -> int:
        """Number of catalogs retrieved from cache."""
        return self._cache_hits

    @property
    def bytes_from_cache(self) -> int:
        """Total bytes retrieved from cache."""
        return self._bytes_from_cache

    @property
    def tree_cache_reused(self) -> int:
        """Number of catalog nodes reused from previous tree cache."""
        return self._tree_cache_reused
