#!/usr/bin/env python
"""Create (idempotently) OneLake shortcuts in silver_lh pointing at the
Mirrored Azure Databricks Catalog's tables.

Why this exists: a Mirrored Azure Databricks catalog item is NOT directly
queryable from a Spark notebook — confirmed live (2026-09-01):
`spark.sql("SHOW CATALOGS")` never lists it, three-part SQL naming against
its display name raises AnalysisException, and reading its OneLake
`Tables/<schema>/<table>` path directly via `spark.read.format("delta").load(...)`
fails even though the ADLS Gen2 DFS List Path API confirms the files exist
(the mirror's storage credentials are resolved through OneLake's
shortcut-resolution layer, which a raw abfss+delta read bypasses).

Per Microsoft's own tutorial ("Create Lakehouse shortcuts to the Databricks
catalog item", https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks-tutorial),
the documented, supported path for Spark notebook access is: create a OneLake
shortcut FROM a Lakehouse TO the mirrored catalog's tables, then read the
shortcut like any other Lakehouse table. This script does that against
silver_lh (already the notebooks' working Lakehouse), using shortcut names
prefixed `src_databricks_` so they never collide with Silver's own output
tables of similar names (`transactions`, `merchants`).

Usage:
    python infra/fabric/shortcut_databricks_mirror.py --help
    python infra/fabric/shortcut_databricks_mirror.py

Reads from .env: FABRIC_WORKSPACE_NAME.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

from .auth import FABRIC_API, get_session
from .common import find_item, find_workspace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("shortcut_databricks_mirror")

MIRROR_ITEM_NAME = "fmv2poc_databricks_banking_mirror"
MIRROR_SCHEMA = "banking"
TABLES = ["transactions", "transaction_risk_scores", "merchants"]
SHORTCUT_PREFIX = "src_databricks_"
TARGET_LAKEHOUSE = "silver_lh"


def list_shortcuts(session, workspace_id: str, lakehouse_id: str) -> list[dict]:
    resp = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts")
    resp.raise_for_status()
    return resp.json().get("value", [])


def ensure_shortcut(session, workspace_id: str, lakehouse_id: str, mirror_item_id: str,
                     table: str, existing: list[dict]) -> dict:
    shortcut_name = f"{SHORTCUT_PREFIX}{table}"
    match = next((s for s in existing if s.get("name") == shortcut_name and s.get("path") == "Tables"), None)
    if match:
        log.info("Shortcut %r already exists -- reusing.", shortcut_name)
        return {"table": table, "shortcut_name": shortcut_name, "action": "reused"}

    body = {
        "path": "Tables",
        "name": shortcut_name,
        "target": {
            "oneLake": {
                "workspaceId": workspace_id,
                "itemId": mirror_item_id,
                "path": f"Tables/{MIRROR_SCHEMA}/{table}",
            }
        },
    }
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts", json=body)
    if not resp.ok:
        log.error("POST shortcuts -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    log.info("Created shortcut %r -> mirror %s/%s", shortcut_name, MIRROR_SCHEMA, table)
    return {"table": table, "shortcut_name": shortcut_name, "action": "created"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    load_dotenv()
    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]

    session = get_session()
    workspace = find_workspace(session, workspace_name)
    if not workspace:
        raise RuntimeError(f"Fabric workspace {workspace_name!r} not found")
    workspace_id = workspace["id"]

    lakehouse = find_item(session, workspace_id, TARGET_LAKEHOUSE, "Lakehouse")
    if not lakehouse:
        raise RuntimeError(f"Lakehouse {TARGET_LAKEHOUSE!r} not found")
    lakehouse_id = lakehouse["id"]

    mirror = find_item(session, workspace_id, MIRROR_ITEM_NAME, "MirroredAzureDatabricksCatalog")
    if not mirror:
        raise RuntimeError(f"MirroredAzureDatabricksCatalog {MIRROR_ITEM_NAME!r} not found")
    mirror_item_id = mirror["id"]

    existing = list_shortcuts(session, workspace_id, lakehouse_id)
    results = [ensure_shortcut(session, workspace_id, lakehouse_id, mirror_item_id, t, existing) for t in TABLES]

    print(json.dumps({
        "workspace_id": workspace_id,
        "lakehouse_id": lakehouse_id,
        "mirror_item_id": mirror_item_id,
        "shortcuts": results,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
