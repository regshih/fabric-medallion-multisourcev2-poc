#!/usr/bin/env python
"""Query a Lakehouse's SQL analytics endpoint or a Warehouse for row counts —
the "counts match expectations" verification step for each medallion layer.

Usage:
  python infra/verify_row_counts.py silver_lh transactions,transaction_risk,merchants,sessions,devices,fraud_alerts
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from fabric.auth import FABRIC_API, get_session  # noqa: E402
from sql_conn import get_sql_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("verify_row_counts")


def resolve_connection_string(session, workspace_id: str, item_name: str) -> str:
    """Also force-refreshes the SQL analytics endpoint's Delta metadata sync
    for Lakehouses — that sync lags behind actual OneLake writes, so
    querying right after a notebook run can hit "Invalid object name" for
    tables that exist fine in the underlying Delta log. Warehouses are
    native SQL, no sync needed."""
    resp = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/items")
    resp.raise_for_status()
    item = next((i for i in resp.json()["value"] if i["displayName"] == item_name
                 and i["type"] in ("Lakehouse", "Warehouse")), None)
    if not item:
        raise RuntimeError(f"no Lakehouse/Warehouse named {item_name!r} in workspace {workspace_id}")

    if item["type"] == "Lakehouse":
        detail = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{item['id']}")
        detail.raise_for_status()
        props = detail.json()["properties"]["sqlEndpointProperties"]
        refresh = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/sqlEndpoints/{props['id']}/refreshMetadata")
        refresh.raise_for_status()
        return props["connectionString"]

    detail = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/warehouses/{item['id']}")
    detail.raise_for_status()
    return detail.json()["properties"]["connectionString"]


def main() -> None:
    load_dotenv()
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    item_name, tables_csv = sys.argv[1], sys.argv[2]
    # Spark/Hive lowercases table names in the metastore regardless of the
    # case used in saveAsTable() — matters for our PascalCase Gold tables.
    tables = [t.strip().lower() for t in tables_csv.split(",")]

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    session = get_session()
    resp = session.get(f"{FABRIC_API}/workspaces")
    resp.raise_for_status()
    workspace = next((w for w in resp.json()["value"] if w["displayName"] == workspace_name), None)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found")

    conn_str = resolve_connection_string(session, workspace["id"], item_name)
    log.info("connecting to %s / %s", conn_str, item_name)
    conn = get_sql_connection(conn_str, item_name)
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        count = cursor.fetchone()[0]
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
