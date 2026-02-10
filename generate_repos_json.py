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
        except Exception:
            generated_at = ""
            incomplete = False
        repos.append(
            {
                "name": repo_name,
                "generated_at": generated_at,
                "incomplete": incomplete,
                "data_file": "data/" + f,
                "size_bytes": size_bytes,
            }
        )

    # Write repos.json next to the data directory
    output_path = os.path.join(os.path.dirname(data_dir.rstrip("/")), "repos.json")
    with open(output_path, "w") as fh:
        json.dump(repos, fh, separators=(",", ":"))
    print(f"Generated repos.json with {len(repos)} repositories")


if __name__ == "__main__":
    main()
