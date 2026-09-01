#!/usr/bin/env python
"""Deploy a Fabric Data Pipeline from a placeholder-templated
pipeline-content.json file, and optionally run it and/or (re)set a schedule.

The checked-in pipeline JSON uses {{WORKSPACE_ID}} / {{NOTEBOOK_ID:name}} /
{{ITEM_ID:name}} / {{ITEM_NAME:name}} placeholders (see
infra/fabric/definitions.py) — resolved here against the live workspace's
notebook ids by display name, never hardcoded, since this is a from-scratch
build where every item id is newly minted on each provisioning run.

Usage:
  python infra/deploy_pipeline.py pipelines/pl_multisource_medallion.json [--run] [--schedule]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402
from infra.fabric.common import find_item, list_all, run_pipeline, wait_for_lro  # noqa: E402
from infra.fabric.definitions import bind_pipeline, logical_id, pipeline_definition  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("deploy_pipeline")


def deploy_pipeline(session, workspace_id: str, display_name: str, pipeline_json: dict) -> str:
    definition = pipeline_definition(display_name, pipeline_json)
    existing = find_item(session, workspace_id, display_name, "DataPipeline")
    if existing:
        resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items/{existing['id']}/updateDefinition",
                             json={"definition": definition})
        if resp.status_code not in (200, 202):
            resp.raise_for_status()
        wait_for_lro(session, resp)
        return existing["id"]

    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items", json={
        "displayName": display_name, "type": "DataPipeline", "definition": definition,
    })
    if resp.status_code not in (200, 201, 202):
        resp.raise_for_status()
    result = wait_for_lro(session, resp)
    return result["id"] if result else resp.json()["id"]


def set_schedule(session, workspace_id: str, item_id: str, interval_minutes: int, hour_utc: int) -> None:
    body = {
        "enabled": True,
        "configuration": {
            "type": "Cron",
            "interval": interval_minutes,
            "startDateTime": datetime.now(timezone.utc).strftime(f"%Y-%m-%dT{hour_utc:02d}:00:00"),
            "endDateTime": "2030-01-01T00:00:00",
            "localTimeZoneId": "UTC",
        },
    }
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/Pipeline/schedules", json=body)
    resp.raise_for_status()
    log.info("schedule created: %s", resp.json())


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pipeline_path")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--run-date", default="", help="ad hoc run_date override, e.g. 2026-08-31")
    parser.add_argument("--schedule", action="store_true", help="(re)create a nightly 02:00 UTC schedule")
    args = parser.parse_args()

    path = Path(args.pipeline_path)
    display_name = path.stem
    template = json.loads(path.read_text(encoding="utf-8"))

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    session = get_session()
    resp = session.get(f"{FABRIC_API}/workspaces")
    resp.raise_for_status()
    workspace = next((w for w in resp.json()["value"] if w["displayName"] == workspace_name), None)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found")
    workspace_id = workspace["id"]

    notebook_ids = {
        item["displayName"]: item["id"]
        for item in list_all(session, f"{FABRIC_API}/workspaces/{workspace_id}/items")
        if item["type"] == "Notebook"
    }
    item_ids = {
        item["displayName"]: item["id"]
        for item in list_all(session, f"{FABRIC_API}/workspaces/{workspace_id}/items")
    }
    pipeline_json = bind_pipeline(template, workspace_id, notebook_ids, item_ids=item_ids)

    log.info("deploying %s as pipeline %r", path, display_name)
    item_id = deploy_pipeline(session, workspace_id, display_name, pipeline_json)
    log.info("pipeline %s id=%s", display_name, item_id)

    if args.schedule:
        set_schedule(session, workspace_id, item_id, interval_minutes=1440, hour_utc=2)

    if args.run:
        params = {"run_date": args.run_date} if args.run_date else None
        log.info("running pipeline %s (run_date=%r) ...", display_name, args.run_date)
        result = run_pipeline(session, workspace_id, item_id, parameters=params)
        log.info("run completed: %s", result["status"])


if __name__ == "__main__":
    main()
