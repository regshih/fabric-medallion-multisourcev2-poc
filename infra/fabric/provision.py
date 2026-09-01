#!/usr/bin/env python
"""Provision the Fabric workspace + Silver/Gold medallion items.

Creates (idempotently — safe to re-run):
  - a workspace assigned to $FABRIC_CAPACITY_NAME
  - Lakehouse items: silver_lh, gold_lh
  - a Warehouse item: gold_wh

Deliberately does NOT create a bronze_lh Lakehouse. Bronze in this POC is
source-aligned: the mirrored Azure Databricks catalog (metadata mirror +
OneLake shortcuts to Delta, no physical copy) and the mirrored Azure Cosmos
DB database (physical Delta replica in OneLake) ARE Bronze. Creating an
empty or duplicate physical Bronze Lakehouse on top of that would just
re-copy data Fabric already makes available for zero-copy access to
Databricks, and would duplicate what Cosmos mirroring already replicates.
See docs/architecture-decisions.md.

The two source mirror items themselves are created through Fabric's
mirroring setup (see infra/fabric/mirror_databricks.py and
infra/fabric/mirror_cosmos.py) once the Databricks/Cosmos specialists report
their deployed resource identifiers — resolved by name here via
resolve_source_items(), not created by this script.

Fabric items are not ARM resources (only the capacity itself is), so this
talks to https://api.fabric.microsoft.com/v1 directly. Auth via
DefaultAzureCredential (run `az login` first).

Usage: python infra/fabric/provision.py [--help]
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from .auth import FabricSession, FABRIC_API, get_session
from .common import list_all, wait_for_lro

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("provision_fabric")

LAKEHOUSE_ITEMS = ["silver_lh", "gold_lh"]
WAREHOUSE_ITEMS = ["gold_wh"]


def find_capacity_id(session: FabricSession, capacity_name: str) -> str:
    for cap in list_all(session, f"{FABRIC_API}/capacities"):
        if cap["displayName"].lower() == capacity_name.lower():
            return cap["id"]
    raise RuntimeError(f"capacity {capacity_name!r} not found or not visible to this account")


def find_workspace(session: FabricSession, name: str) -> dict | None:
    for ws in list_all(session, f"{FABRIC_API}/workspaces"):
        if ws["displayName"] == name:
            return ws
    return None


def ensure_workspace(session: FabricSession, name: str, capacity_id: str) -> str:
    existing = find_workspace(session, name)
    if existing:
        log.info("workspace %s already exists (id=%s)", name, existing["id"])
        if existing.get("capacityId") != capacity_id:
            log.info("reassigning workspace %s to capacity %s", name, capacity_id)
            resp = session.post(f"{FABRIC_API}/workspaces/{existing['id']}/assignToCapacity",
                                 json={"capacityId": capacity_id})
            resp.raise_for_status()
        return existing["id"]

    log.info("creating workspace %s", name)
    resp = session.post(f"{FABRIC_API}/workspaces", json={
        "displayName": name,
        "description": "Multisource medallion POC: Azure Databricks + Cosmos DB -> Fabric. See ARCHITECTURE.md",
        "capacityId": capacity_id,
    })
    resp.raise_for_status()
    return resp.json()["id"]


def ensure_item(session: FabricSession, workspace_id: str, name: str, item_type: str) -> str:
    for item in list_all(session, f"{FABRIC_API}/workspaces/{workspace_id}/items"):
        if item["displayName"] == name and item["type"] == item_type:
            log.info("item %s (%s) already exists (id=%s)", name, item_type, item["id"])
            return item["id"]

    log.info("creating item %s (%s)", name, item_type)
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items", json={
        "displayName": name,
        "type": item_type,
    })
    if resp.status_code not in (200, 201, 202):
        resp.raise_for_status()
    result = wait_for_lro(session, resp)
    item = result if result else resp.json()
    return item["id"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    load_dotenv()
    capacity_name = os.environ["FABRIC_CAPACITY_NAME"]
    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]

    session = get_session()

    capacity_id = find_capacity_id(session, capacity_name)
    log.info("capacity %s id=%s", capacity_name, capacity_id)

    workspace_id = ensure_workspace(session, workspace_name, capacity_id)
    log.info("workspace %s id=%s", workspace_name, workspace_id)

    item_ids: dict[str, str] = {}
    for name in LAKEHOUSE_ITEMS:
        item_ids[name] = ensure_item(session, workspace_id, name, "Lakehouse")
    for name in WAREHOUSE_ITEMS:
        item_ids[name] = ensure_item(session, workspace_id, name, "Warehouse")

    log.info("provisioning complete")
    print(f"WORKSPACE_ID={workspace_id}")
    for name, item_id in item_ids.items():
        print(f"{name.upper()}_ID={item_id}")


if __name__ == "__main__":
    main()
