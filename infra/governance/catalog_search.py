#!/usr/bin/env python
"""Demonstrate the OneLake Catalog Search API — proves this project's items
are programmatically discoverable across the tenant. Run catalog_setup.py
first so items have descriptions to match against.

Usage:
    python infra/governance/catalog_search.py --search "cross-source"
    python infra/governance/catalog_search.py --search Fraud --type Warehouse
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--search", default="Fraud", help="text query — matches display name, workspace name, description")
    parser.add_argument("--type", default=None, help="optional item type filter, e.g. Lakehouse, Warehouse, Notebook, DataPipeline")
    parser.add_argument("--page-size", type=int, default=25)
    args = parser.parse_args()

    body = {"search": args.search, "pageSize": args.page_size}
    if args.type:
        body["filter"] = f"Type eq '{args.type}'"

    session = get_session()
    resp = session.post(f"{FABRIC_API}/catalog/search", json=body)
    resp.raise_for_status()
    results = resp.json()["value"]

    if not results:
        print(f"No catalog entries found for search={args.search!r}.")
        return

    print(f"{'Type':<15} {'Name':<35} {'Workspace':<35} Description")
    print("-" * 130)
    for entry in results:
        ws = entry.get("hierarchy", {}).get("workspace", {}).get("displayName", "")
        desc = (entry.get("description") or "")[:50]
        print(f"{entry['type']:<15} {entry['displayName']:<35} {ws:<35} {desc}")


if __name__ == "__main__":
    sys.exit(main())
