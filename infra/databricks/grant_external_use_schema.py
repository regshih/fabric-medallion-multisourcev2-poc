#!/usr/bin/env python3
"""Idempotently grant EXTERNAL USE SCHEMA on a Unity Catalog schema to the
identity that will create the Fabric mirrored Azure Databricks catalog
connection.

Why this is needed: per docs/databricks-fabric-integration.md ("Authentication
and required privileges"), the Fabric connection identity needs
EXTERNAL USE SCHEMA on the schema Fabric reads from, in addition to whatever
ordinary Unity Catalog read privileges are needed to see the catalog/schema/
tables. This is a normal Unity Catalog grant (not a metastore-admin
operation) — see setup_unity_catalog.py for the account-admin-only
external-data-access step this is downstream of.

Identity choice for this POC: the signed-in AAD user's own UPN, so ad hoc
queries/validation also work under that identity. This grant is independent
of what credential type the Fabric connection itself ends up using — see
infra/fabric/mirror_databricks.py's module docstring for why that connection
uses a Databricks personal access token (credentialType "Key"), not
Organizational-account OAuth2 (needs an interactive browser redirect,
confirmed unusable unattended) or a Service Principal (SP creation was
denied by this session's own credential-minting safety controls). A PAT
inherits whichever Databricks identity created it, so this grant should
target the same principal that runs infra/fabric/mirror_databricks.py.

Usage:
    python infra/databricks/grant_external_use_schema.py --help
    python infra/databricks/grant_external_use_schema.py \\
        --workspace-host https://adb-xxxx.azuredatabricks.net \\
        --workspace-resource-id /subscriptions/.../workspaces/dbw-fmv2poc-915d \\
        --catalog dbw_fmv2poc_915d --schema banking \\
        --principal regshih@MngEnvMCAP048770.onmicrosoft.com

Windows/Git Bash note: same MSYS path-mangling issue as setup_unity_catalog.py
applies to --workspace-resource-id. Run from PowerShell, or set
MSYS_NO_PATHCONV=1 in Git Bash.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import time

import requests
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("grant_external_use_schema")

# NOTE: this deliberately calls the UC permissions REST API directly with
# `requests` instead of the SDK's GrantsAPI.get()/update(). Confirmed live:
# GrantsAPI.get()/update() add an `X-Databricks-Workspace-Id` header whenever
# WorkspaceClient.config.workspace_id is set (true here -- azure-cli auth
# against an azure_workspace_resource_id populates it with the numeric
# Databricks org ID, 7405604763364719 for this workspace). With that header
# present, /api/2.1/unity-catalog/permissions/schema/{full_name} 400s with a
# non-JSON "Invalid resource ID." body, which the SDK then fails to parse and
# re-raises as a confusing "unable to parse response... this is likely a bug"
# error. The identical request without that header (plain `requests`, using
# only ws.config.authenticate()'s headers) succeeds with a normal 200. This
# looks like a genuine databricks-sdk 0.133.0 bug specific to the
# Azure-workspace-resource-id auth path; worth reporting upstream, but not
# blocking here since the direct REST call works fine.
SCHEMA_PERMISSIONS_PATH = "/api/2.1/unity-catalog/permissions/schema/{full_name}"


def _get_with_retry(ws: WorkspaceClient, url: str, attempts: int = 3) -> dict:
    # Defensive retry for transient failures against this endpoint. NOTE: a
    # reproducible 400 "Invalid resource ID." seen during development turned
    # out to be a Git Bash/MSYS issue, not API flakiness -- MSYS rewrites a
    # leading-`/` CLI argument (like --workspace-resource-id /subscriptions/
    # ...) into a bogus Windows path before Python ever sees it, so the
    # workspace resource ID baked into the auth headers was silently wrong.
    # Run this from PowerShell, or set MSYS_NO_PATHCONV=1 in Git Bash, per
    # the module docstring. This retry loop is kept for genuine transient
    # errors, not as a workaround for that issue.
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        headers = ws.config.authenticate()
        resp = requests.get(url, headers=headers)
        if resp.ok:
            return resp.json()
        last_exc = requests.exceptions.HTTPError(f"{resp.status_code}: {resp.text}")
        logger.warning("GET %s attempt %d/%d -> %s: %s", url, attempt, attempts, resp.status_code, resp.text)
        time.sleep(2 * attempt)
    raise last_exc  # type: ignore[misc]


def ensure_external_use_schema(ws: WorkspaceClient, catalog: str, schema: str, principal: str) -> dict:
    full_name = f"{catalog}.{schema}"
    url = ws.config.host.rstrip("/") + SCHEMA_PERMISSIONS_PATH.format(full_name=full_name)

    existing = _get_with_retry(ws, url)
    already_granted = any(
        assignment.get("principal", "").lower() == principal.lower()
        and "EXTERNAL_USE_SCHEMA" in (assignment.get("privileges") or [])
        for assignment in existing.get("privilege_assignments", [])
    )
    if already_granted:
        logger.info("%s already has EXTERNAL USE SCHEMA on %s -- skipping.", principal, full_name)
        return {"full_name": full_name, "principal": principal, "action": "none (already granted)"}

    logger.info("Granting EXTERNAL USE SCHEMA on %s to %s.", full_name, principal)
    body = {"changes": [{"principal": principal, "add": ["EXTERNAL_USE_SCHEMA"]}]}
    resp = requests.patch(url, headers=ws.config.authenticate(), json=body)
    if not resp.ok:
        logger.error("PATCH %s -> %s: %s", url, resp.status_code, resp.text)
    resp.raise_for_status()

    after = _get_with_retry(ws, url)
    confirmed = any(
        assignment.get("principal", "").lower() == principal.lower()
        and "EXTERNAL_USE_SCHEMA" in (assignment.get("privileges") or [])
        for assignment in after.get("privilege_assignments", [])
    )
    return {
        "full_name": full_name,
        "principal": principal,
        "action": "granted" if confirmed else "update call made but grant not confirmed on read-back",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-host", required=True, help="https://adb-....azuredatabricks.net")
    parser.add_argument("--workspace-resource-id", required=True, help="ARM resource ID of the Databricks workspace")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Schema name within the catalog")
    parser.add_argument("--principal", required=True, help="UPN/email of the user (or application ID of a service principal) to grant EXTERNAL USE SCHEMA to")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ws = WorkspaceClient(host=args.workspace_host, azure_workspace_resource_id=args.workspace_resource_id, auth_type="azure-cli")
    result = ensure_external_use_schema(ws, args.catalog, args.schema, args.principal)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
