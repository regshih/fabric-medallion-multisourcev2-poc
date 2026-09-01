#!/usr/bin/env python
"""Deploy a notebook source file from /notebooks to the Fabric workspace and
(optionally) run it, blocking until completion.

The .py file must already be in Fabric's notebook-content.py source format.
Display name in the workspace is the filename without the `nb_` prefix, e.g.
nb_silver_transform.py -> "silver_transform".

Usage: python infra/deploy_notebook.py notebooks/nb_silver_transform.py [--run] [--param key=value ...]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402
from infra.fabric.common import deploy_notebook, run_notebook  # noqa: E402
from infra.fabric.definitions import logical_id  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("deploy_notebook")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("notebook_path", help="Path to a notebook-content.py source file under /notebooks")
    parser.add_argument("--run", action="store_true", help="Trigger a run and block until it completes")
    parser.add_argument("--param", action="append", default=[], help="key=value notebook parameter override")
    args = parser.parse_args()

    path = Path(args.notebook_path)
    display_name = path.stem.removeprefix("nb_")
    content = path.read_text(encoding="utf-8")
    lid = logical_id("Notebook", display_name)

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    session = get_session()

    resp = session.get(f"{FABRIC_API}/workspaces")
    resp.raise_for_status()
    workspace = next((w for w in resp.json()["value"] if w["displayName"] == workspace_name), None)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found — run infra/fabric/provision.py first")
    workspace_id = workspace["id"]

    log.info("deploying %s as notebook %r", path, display_name)
    item_id = deploy_notebook(session, workspace_id, display_name, content, lid)
    log.info("notebook %s id=%s", display_name, item_id)

    if args.run:
        params = {}
        for kv in args.param:
            key, _, value = kv.partition("=")
            params[key] = {"value": value, "type": "string"}
        log.info("running notebook %s ...", display_name)
        result = run_notebook(session, workspace_id, item_id, parameters=params or None)
        log.info("run completed: %s", result["status"])


if __name__ == "__main__":
    main()
