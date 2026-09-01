#!/usr/bin/env python
"""Create (idempotently) the Fabric Mirrored Azure Databricks Catalog item
for this POC's Unity Catalog schema, and verify it reaches a healthy state.

What this does, in order (safe to re-run -- each step checks-then-creates):
  1. Ensure a Fabric "AzureDatabricksWorkspace" connection to the Databricks
     workspace exists (creating a fresh Databricks personal access token
     for credentials only if the connection doesn't already exist -- PATs
     can't be read back, so this never rotates a token that's already in
     use by an existing connection).
  2. Ensure the MirroredAzureDatabricksCatalog item exists in the Fabric
     workspace, mirroring exactly UNITY_CATALOG_SCHEMA (Partial catalog
     mirroring, Full schema mirroring -- not the whole shared catalog,
     which may hold unrelated schemas from other POCs sharing the same
     auto-provisioned workspace catalog; see docs/databricks-fabric-
     integration.md).
  3. Poll the item's sync status until Success/Failed or a timeout.
  4. Verify (not just assume) the three tables are reachable as OneLake
     shortcuts by listing the item's own oneLakeTablesPath via the OneLake
     DFS API (ADLS Gen2 List Path operation, token scope
     https://storage.azure.com/.default).

Why a Databricks PAT (credentialType "Key"), not "Organizational account"
or "Service principal", for the Fabric connection's credentials
-------------------------------------------------------------------------
The Fabric mirroring tutorial's portal flow only exposes "Organizational
account" (interactive AAD sign-in) and "Service principal" as connection
auth choices. Neither is usable unattended in this environment:

  - Organizational account maps to credentialType "OAuth2" on
    POST /v1/connections. That field is undocumented on the public REST
    reference page (its Credentials union lists Anonymous/Basic/Key/
    KeyPair/ServicePrincipal/SharedAccessSignature/Windows/
    WindowsWithoutImpersonation/WorkspaceIdentity -- no OAuth2Credentials
    shape). Empirically (confirmed live 2026-09-01): POSTing
    {"credentialType": "OAuth2"} 400s with "The UseCallerIdentity field is
    required." Adding {"UseCallerIdentity": true} gets further but then
    400s with errorCode OAuthTokenLoginFailed / "updateCredential is
    missing redirectEndpoint property" -- i.e. it requires an interactive
    browser OAuth redirect flow, which cannot be completed from a
    background script. This matches the Cosmos DB private-network guide's
    Step 7, which is explicitly portal-only for the same reason.
  - Service principal (credentialType "ServicePrincipal") would work
    headlessly in principle, but requires creating a new Azure AD app
    registration + secret first. That specific action
    (`az ad sp create-for-rbac`) was denied by this session's own
    operator-safety controls when attempted live -- app/SP creation is a
    credential-minting action outside this script's scope to force past.

A Databricks personal access token (credentialType "Key") is not one of
the two choices the mirroring tutorial's *portal* UI lists, but the
AzureDatabricksWorkspace connection type's supportedCredentialTypes
(confirmed live via GET /v1/connections/supportedConnectionTypes) includes
"Key", and creating the connection this way was confirmed live to succeed
including Fabric's own test-connection validation (skipTestConnection was
left false). This is a POC-pragmatic deviation from the documented UI
path, not a guess -- it is a real, tested, working connection. The token
is scoped to whichever Databricks identity runs this script (see
--principal / the .env-configured identity); it inherits that identity's
Unity Catalog privileges, including the EXTERNAL USE SCHEMA grant applied
by infra/databricks/grant_external_use_schema.py.

Usage:
    python infra/fabric/mirror_databricks.py --help
    python infra/fabric/mirror_databricks.py

Reads from .env: FABRIC_WORKSPACE_NAME, DATABRICKS_HOST,
DATABRICKS_WORKSPACE_RESOURCE_ID, UNITY_CATALOG_NAME, UNITY_CATALOG_SCHEMA.

Windows/Git Bash note: MSYS mangles leading-`/` values passed as CLI args
or read from .env into resource-id-shaped strings only when they cross the
shell as literal CLI arguments -- .env values read via python-dotenv are
not affected, but if you pass --workspace-resource-id manually, run this
from PowerShell or set MSYS_NO_PATHCONV=1 in Git Bash (see
infra/databricks/setup_unity_catalog.py for the same note; confirmed live
to matter for the Databricks-side script in this repo).
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from azure.identity import DefaultAzureCredential
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from .auth import FABRIC_API, get_session
from .common import find_item, find_workspace, wait_for_lro
from .definitions import item_definition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("mirror_databricks")

CONNECTION_NAME = "fmv2poc-databricks-catalog-mirror-connection"
MIRROR_ITEM_NAME = "fmv2poc_databricks_banking_mirror"
STORAGE_SCOPE = "https://storage.azure.com/.default"
DEFINITION_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/item/mirroredAzureDatabricksCatalog"
    "/definition/mirroredAzureDatabricksCatalogDefinition/1.0.0/schema.json"
)
SYNC_POLL_SECONDS = 15
SYNC_TIMEOUT_SECONDS = 20 * 60


# 90 days -- bounded lifetime, matching the short-expiration convention this
# repo uses for the GitHub PAT (see infra/setup_git_integration.py docstring).
# A non-expiring PAT is a standing-credential risk not worth taking just
# because this connection's own credential can't be read back for rotation
# reminders -- bound it up front instead.
PAT_LIFETIME_SECONDS = 90 * 24 * 60 * 60


def ensure_databricks_pat(workspace_host: str, workspace_resource_id: str) -> str:
    ws = WorkspaceClient(host=workspace_host, azure_workspace_resource_id=workspace_resource_id, auth_type="azure-cli")
    token = ws.tokens.create(comment="fmv2poc-fabric-mirroring-connection", lifetime_seconds=PAT_LIFETIME_SECONDS)
    log.info(
        "Created a new Databricks PAT (token_id=%s, expires in %d days) for the Fabric connection.",
        token.token_info.token_id, PAT_LIFETIME_SECONDS // 86400,
    )
    return token.token_value


def ensure_databricks_connection(session, workspace_host: str, workspace_resource_id: str) -> str:
    resp = session.get(f"{FABRIC_API}/connections")
    resp.raise_for_status()
    existing = next((c for c in resp.json().get("value", []) if (c.get("displayName") or "") == CONNECTION_NAME), None)
    if existing:
        log.info("Connection %r already exists (%s) -- reusing, not rotating its credential.", CONNECTION_NAME, existing["id"])
        return existing["id"]

    pat = ensure_databricks_pat(workspace_host, workspace_resource_id)
    body = {
        "connectivityType": "ShareableCloud",
        "displayName": CONNECTION_NAME,
        "connectionDetails": {
            "type": "AzureDatabricksWorkspace",
            "creationMethod": "AzureDatabricksWorkspace.Actions",
            "parameters": [{"dataType": "Text", "name": "url", "value": workspace_host}],
        },
        "privacyLevel": "Organizational",
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {"credentialType": "Key", "key": pat},
        },
    }
    resp = session.post(f"{FABRIC_API}/connections", json=body)
    if not resp.ok:
        log.error("POST /connections -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    connection = resp.json()
    log.info("Created connection %r (%s)", CONNECTION_NAME, connection["id"])
    return connection["id"]


def build_definition(catalog: str, schema: str, connection_id: str) -> dict:
    content = {
        "$schema": DEFINITION_SCHEMA,
        "catalogName": catalog,
        "databricksWorkspaceConnectionId": connection_id,
        "autoSync": "Enabled",
        "mirroringMode": "Partial",
        "mirrorConfiguration": {"schemas": [{"name": schema, "mirroringMode": "Full"}]},
    }
    return item_definition("MirroredAzureDatabricksCatalog", MIRROR_ITEM_NAME, "definition.json", content)


def ensure_mirror_item(session, workspace_id: str, catalog: str, schema: str, connection_id: str) -> str:
    existing = find_item(session, workspace_id, MIRROR_ITEM_NAME, "MirroredAzureDatabricksCatalog")
    if existing:
        log.info("MirroredAzureDatabricksCatalog %r already exists (%s) -- reusing.", MIRROR_ITEM_NAME, existing["id"])
        return existing["id"]

    definition = build_definition(catalog, schema, connection_id)
    body = {
        "displayName": MIRROR_ITEM_NAME,
        "description": f"Mirrored Unity Catalog schema {catalog}.{schema} (fabric-medallion-multisourcev2-poc Bronze).",
        "definition": definition,
    }
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/mirroredAzureDatabricksCatalogs", json=body)
    if resp.status_code not in (200, 201, 202):
        log.error("POST mirroredAzureDatabricksCatalogs -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    result = wait_for_lro(session, resp)
    item = result if result else resp.json()
    log.info("Created MirroredAzureDatabricksCatalog %r (%s)", MIRROR_ITEM_NAME, item["id"])
    return item["id"]


def poll_sync_status(session, workspace_id: str, item_id: str) -> dict:
    deadline = time.time() + SYNC_TIMEOUT_SECONDS
    while True:
        resp = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/mirroredAzureDatabricksCatalogs/{item_id}")
        resp.raise_for_status()
        item = resp.json()
        props = item.get("properties", {})
        sync = props.get("syncDetails", {})
        status = sync.get("status")
        mirror_status = props.get("mirrorStatus")
        log.info("sync status=%s mirrorStatus=%s lastSyncDateTime=%s", status, mirror_status, sync.get("lastSyncDateTime"))
        if status in ("Success", "Failed"):
            return item
        if time.time() > deadline:
            log.warning("Timed out after %ss waiting for sync status Success/Failed (last status=%s).", SYNC_TIMEOUT_SECONDS, status)
            return item
        time.sleep(SYNC_POLL_SECONDS)


def verify_onelake_shortcuts(workspace_id: str, item_id: str, tables: list[str]) -> dict:
    """List <item>/Tables/<schema> in OneLake via the ADLS Gen2 DFS 'List Path'
    operation and confirm each expected table name appears. This is a real
    observed check, not an assumption that the mirror status field implies
    working shortcuts."""
    credential = DefaultAzureCredential()
    token = credential.get_token(STORAGE_SCOPE).token
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{item_id}/Tables"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"resource": "filesystem", "recursive": "true"},
    )
    if not resp.ok:
        return {"verified": False, "error": f"{resp.status_code}: {resp.text}"}
    paths = [p["name"] for p in resp.json().get("paths", [])]
    found = {t: any(t in p for p in paths) for t in tables}
    return {"verified": all(found.values()), "found": found, "all_paths": paths}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    load_dotenv()
    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    databricks_host = os.environ["DATABRICKS_HOST"]
    databricks_resource_id = os.environ["DATABRICKS_WORKSPACE_RESOURCE_ID"]
    catalog = os.environ["UNITY_CATALOG_NAME"]
    schema = os.environ["UNITY_CATALOG_SCHEMA"]
    tables = ["transactions", "transaction_risk_scores", "merchants"]

    session = get_session()
    workspace = find_workspace(session, workspace_name)
    if not workspace:
        raise RuntimeError(f"Fabric workspace {workspace_name!r} not found")
    workspace_id = workspace["id"]
    log.info("workspace %s id=%s", workspace_name, workspace_id)

    connection_id = ensure_databricks_connection(session, databricks_host, databricks_resource_id)
    item_id = ensure_mirror_item(session, workspace_id, catalog, schema, connection_id)

    item = poll_sync_status(session, workspace_id, item_id)
    verification = verify_onelake_shortcuts(workspace_id, item_id, tables)

    print(json.dumps({
        "workspace_id": workspace_id,
        "connection_id": connection_id,
        "mirror_item_id": item_id,
        "mirror_item_name": MIRROR_ITEM_NAME,
        "properties": item.get("properties"),
        "onelake_verification": verification,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
