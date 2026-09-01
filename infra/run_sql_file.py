#!/usr/bin/env python
"""Execute a .sql file's batches (split on standalone `GO` lines) against a
Fabric Warehouse.

Usage: python infra/run_sql_file.py gold_wh warehouse/00_refresh_gold_serving.sql
"""
from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from fabric.auth import FABRIC_API, get_session  # noqa: E402
from sql_conn import get_sql_connection  # noqa: E402
from verify_row_counts import resolve_connection_string  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("run_sql_file")

GO_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)


def main() -> None:
    load_dotenv()
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    item_name, sql_path = sys.argv[1], Path(sys.argv[2])

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    session = get_session()
    resp = session.get(f"{FABRIC_API}/workspaces")
    resp.raise_for_status()
    workspace = next((w for w in resp.json()["value"] if w["displayName"] == workspace_name), None)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found")

    conn_str = resolve_connection_string(session, workspace["id"], item_name)
    conn = get_sql_connection(conn_str, item_name)
    conn.autocommit = True
    cursor = conn.cursor()

    batches = [b.strip() for b in GO_RE.split(sql_path.read_text(encoding="utf-8")) if b.strip()]
    for i, batch in enumerate(batches, 1):
        cursor.execute(batch)
        log.info("batch %d/%d ok", i, len(batches))
    log.info("%s applied to %s (%d batches)", sql_path, item_name, len(batches))


if __name__ == "__main__":
    main()
