"""Shared Fabric REST API helpers: LRO polling, paginated listing, and
notebook definition build/deploy/run.

Ported from fabric-medallion-banking-poc's live-verified infra/fabric_common.py
(2026-08-14/15), with two additions the single-workspace banking POC never
needed: pagination (this workspace will hold two source mirrors + 9+
notebooks + a pipeline + a warehouse, easily past a single response page) and
HTTP-date Retry-After parsing (some Fabric endpoints return a date, not a
plain integer, in Retry-After).
"""
from __future__ import annotations

import base64
import json
import time
from email.utils import parsedate_to_datetime

import requests

from .auth import FABRIC_API


def _retry_after_seconds(headers: dict, default: int = 5) -> int:
    raw = headers.get("Retry-After")
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        delta = (dt - dt.now(dt.tzinfo)).total_seconds()
        return max(1, int(delta))
    except Exception:
        return default


def wait_for_lro(session: requests.Session, response: requests.Response) -> dict | None:
    """Poll a long-running Fabric operation (202 + Location header) to completion."""
    if response.status_code != 202:
        return response.json() if response.content else None

    op_url = response.headers["Location"]
    retry_after = _retry_after_seconds(response.headers)
    while True:
        time.sleep(retry_after)
        op = session.get(op_url)
        op.raise_for_status()
        body = op.json()
        if body["status"] == "Succeeded":
            result = session.get(f"{op_url}/result")
            return result.json() if result.status_code == 200 and result.content else None
        if body["status"] == "Failed":
            raise RuntimeError(f"Fabric operation failed: {body}")
        retry_after = _retry_after_seconds(op.headers, retry_after)


def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def build_notebook_definition(display_name: str, notebook_content: str, logical_id: str) -> dict:
    platform = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id},
    })
    return {
        "parts": [
            {"path": "notebook-content.py", "payload": _b64(notebook_content), "payloadType": "InlineBase64"},
            {"path": ".platform", "payload": _b64(platform), "payloadType": "InlineBase64"},
        ]
    }


def list_all(session: requests.Session, url: str) -> list[dict]:
    """GET url, following continuationToken/continuationUri until exhausted."""
    items: list[dict] = []
    next_url = url
    while next_url:
        resp = session.get(next_url)
        resp.raise_for_status()
        body = resp.json()
        items.extend(body.get("value", []))
        next_url = body.get("continuationUri")
    return items


def find_workspace(session: requests.Session, name: str) -> dict | None:
    for ws in list_all(session, f"{FABRIC_API}/workspaces"):
        if ws["displayName"] == name:
            return ws
    return None


def find_item(session: requests.Session, workspace_id: str, name: str, item_type: str) -> dict | None:
    for item in list_all(session, f"{FABRIC_API}/workspaces/{workspace_id}/items"):
        if item["displayName"] == name and item["type"] == item_type:
            return item
    return None


def deploy_notebook(session: requests.Session, workspace_id: str, display_name: str,
                     notebook_content: str, logical_id: str) -> str:
    """Create the notebook if missing, otherwise push a new definition. Returns item id."""
    definition = build_notebook_definition(display_name, notebook_content, logical_id)
    existing = find_item(session, workspace_id, display_name, "Notebook")
    if existing:
        resp = session.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{existing['id']}/updateDefinition",
            json={"definition": definition},
        )
        if resp.status_code not in (200, 202):
            resp.raise_for_status()
        wait_for_lro(session, resp)
        return existing["id"]

    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/items", json={
        "displayName": display_name,
        "type": "Notebook",
        "definition": definition,
    })
    if resp.status_code not in (200, 201, 202):
        resp.raise_for_status()
    result = wait_for_lro(session, resp)
    if result:
        return result["id"]
    return resp.json()["id"]


def run_notebook(session: requests.Session, workspace_id: str, item_id: str,
                  parameters: dict | None = None, poll_seconds: int = 10) -> dict:
    """Trigger a RunNotebook job and block until it finishes. Raises on failure."""
    body = {"executionData": {"parameters": parameters}} if parameters else None
    resp = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=RunNotebook",
        json=body,
    )
    resp.raise_for_status()
    loc = resp.headers["Location"]
    while True:
        time.sleep(poll_seconds)
        status_resp = session.get(loc)
        status_resp.raise_for_status()
        body = status_resp.json()
        if body["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            if body["status"] != "Completed":
                raise RuntimeError(f"notebook run {body['status']}: {body}")
            return body


def run_pipeline(session: requests.Session, workspace_id: str, item_id: str,
                  parameters: dict | None = None, poll_seconds: int = 15) -> dict:
    """Trigger a pipeline job and block until it finishes.

    Pipeline jobs take FLAT parameter values ({"run_date": "..."}), not the
    {"value":..., "type":...} wrapped shape RunNotebook uses — confirmed live
    in fabric-medallion-banking-poc (a wrapped value fails fast with
    "Unable to convert run_date to string").
    """
    body = {"executionData": {"parameters": parameters}} if parameters else None
    resp = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=Pipeline",
        json=body,
    )
    resp.raise_for_status()
    loc = resp.headers["Location"]
    while True:
        time.sleep(poll_seconds)
        status_resp = session.get(loc)
        status_resp.raise_for_status()
        body = status_resp.json()
        if body["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            if body["status"] != "Completed":
                raise RuntimeError(f"pipeline run {body['status']}: {body}")
            return body


def refresh_sql_endpoint_metadata(session: requests.Session, workspace_id: str, sql_endpoint_id: str) -> None:
    """Force the Lakehouse SQL analytics endpoint to sync with the Delta log.

    Without this, querying right after a notebook write can 42S02 "Invalid
    object name" on a table that exists fine in Delta — confirmed live in
    fabric-medallion-banking-poc. Synchronous (200), not an LRO. Not needed
    for Warehouses (those are native SQL, not Delta-synced).
    """
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata")
    resp.raise_for_status()
