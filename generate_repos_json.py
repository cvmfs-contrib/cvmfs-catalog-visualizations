#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate repos.json manifest from .json.zst data files.

Reads each data file, extracts metadata, and writes a repos.json manifest
for the viewer page.

Usage:
    python generate_repos_json.py site/data/
"""

import json
import os
import sys

import zstandard as zstd

MB = 1024 * 1024


def compute_catalog_stats(tree):
    """Walk tree and count catalogs in size buckets."""
    catalogs_10mb = 0
    catalogs_25mb = 0
    catalogs_100mb = 0
    total_catalogs = 0
    max_catalog_bytes = 0

    stack = [tree]
    while stack:
        node = stack.pop()
        size = node.get("size", 0)
        is_virtual = node.get("is_virtual", False)
        if not is_virtual and size > 0:
            total_catalogs += 1
            if size > max_catalog_bytes:
                max_catalog_bytes = size
            if size >= 100 * MB:
                catalogs_100mb += 1
            elif size >= 25 * MB:
                catalogs_25mb += 1
            elif size >= 10 * MB:
                catalogs_10mb += 1
        for child in node.get("children", []):
            stack.append(child)

    return {
        "catalogs_10mb": catalogs_10mb,
        "catalogs_25mb": catalogs_25mb,
        "catalogs_100mb": catalogs_100mb,
        "total_catalogs": total_catalogs,
        "max_catalog_mb": round(max_catalog_bytes / MB, 1),
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_repos_json.py <data_directory>", file=sys.stderr)
        sys.exit(1)

    data_dir = sys.argv[1]
    if not os.path.isdir(data_dir):
        print(f"Error: {data_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    repos = []
    for f in sorted(os.listdir(data_dir)):
        if not f.endswith(".json.zst"):
            continue
        path = os.path.join(data_dir, f)
        size_bytes = os.path.getsize(path)
        repo_name = f.removesuffix(".json.zst")
        # Extract metadata from the envelope
        try:
            raw = zstd.ZstdDecompressor().decompress(open(path, "rb").read())
            envelope = json.loads(raw)
            generated_at = envelope.get("generated_at", "")
            max_catalogs = envelope.get("max_catalogs", 0)
            catalogs_downloaded = envelope.get("catalogs_downloaded", 0)
            # Check if incomplete
            incomplete = False
            if max_catalogs > 0 and catalogs_downloaded >= max_catalogs:
                incomplete = True
            # Check for is_large in tree (quick scan of JSON string)
            if not incomplete and '"is_large":true' in raw.decode(
                "utf-8", errors="ignore"
            ):
                incomplete = True
            catalog_stats = compute_catalog_stats(envelope.get("tree", {}))
        except Exception:
            generated_at = ""
            incomplete = False
            catalog_stats = {}
        repos.append(
            {
                "name": repo_name,
                "generated_at": generated_at,
                "incomplete": incomplete,
                "data_file": "data/" + f,
                "size_bytes": size_bytes,
                **catalog_stats,
            }
        )

    # Write repos.json next to the data directory
    output_path = os.path.join(os.path.dirname(data_dir.rstrip("/")), "repos.json")
    with open(output_path, "w") as fh:
        json.dump(repos, fh, separators=(",", ":"))
    print(f"Generated repos.json with {len(repos)} repositories")


if __name__ == "__main__":
    main()
