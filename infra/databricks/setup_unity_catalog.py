#!/usr/bin/env python3
"""Idempotently set up Unity Catalog for this POC: confirm the metastore
assignment, ensure a catalog + schema exist, and report/attempt to enable
metastore "external data access" (required for Fabric's mirrored Azure
Databricks catalog).

Design note — why this reuses the workspace's auto-provisioned catalog:
Azure Databricks account-level Unity Catalog metastores are one per
region/tenant, not one per workspace. In this tenant, the westus3 metastore
(``metastore_azure_westus3``) already existed before this POC (shared with
an unrelated prior attempt) and has no metastore-level default storage root
configured — creating a brand new catalog therefore requires either (a) a
storage credential + external location backed by a new storage account, or
(b) Databricks Account "Default Storage", which auto-provisions one managed
catalog per workspace at workspace-creation time. This workspace already
got such a catalog (named after the workspace) with ISOLATED isolation mode
(usable only from this workspace) and working managed storage. Reusing it
is simpler and just as safe for a POC than standing up a dedicated storage
account + access connector + credential + external location — so that is
the default here (``--catalog`` defaults to the auto-provisioned name).
Pass ``--managed-location`` to instead create a genuinely new catalog backed
by an existing external location/storage credential, if one is available.

All catalog/schema operations use only workspace-scoped Unity Catalog REST
calls (no Databricks *account*-level API, and therefore no Databricks
account_id needed) via the SDK's "azure-cli" auth type (AAD, `az login`).
Enabling external data access on the metastore, however, is a metastore-
admin-only operation; this script reports precisely whether the caller has
that right and, if not, exactly what is needed to unblock it.

Usage:
    python infra/databricks/setup_unity_catalog.py --help
    python infra/databricks/setup_unity_catalog.py \\
        --workspace-host https://adb-XXXXXXXXXXXXXXXX.XX.azuredatabricks.net \\
        --workspace-resource-id /subscriptions/.../workspaces/dbw-fmv2poc-915d \\
        --schema banking --enable-external-access
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import UpdateAccountsMetastore

ACCOUNTS_HOST = "https://accounts.azuredatabricks.net"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("setup_unity_catalog")


def workspace_client(host: str, resource_id: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, azure_workspace_resource_id=resource_id, auth_type="azure-cli")


def ensure_catalog_and_schema(ws: WorkspaceClient, catalog: str, schema: str, managed_location: str | None) -> dict:
    result: dict = {}
    existing_catalogs = {c.name: c for c in ws.catalogs.list()}
    if catalog in existing_catalogs:
        logger.info("Reusing existing catalog %r (isolation_mode=%s).", catalog, existing_catalogs[catalog].isolation_mode)
        result["catalog_created"] = False
    else:
        logger.info("Creating catalog %r%s.", catalog, f" MANAGED LOCATION {managed_location}" if managed_location else "")
        kwargs = {"name": catalog, "comment": "Synthetic Databricks-source data for fabric-medallion-multisourcev2-poc"}
        if managed_location:
            kwargs["storage_root"] = managed_location
        ws.catalogs.create(**kwargs)
        result["catalog_created"] = True

    existing_schemas = {s.name for s in ws.schemas.list(catalog_name=catalog)}
    if schema in existing_schemas:
        logger.info("Schema %s.%s already exists.", catalog, schema)
        result["schema_created"] = False
    else:
        logger.info("Creating schema %s.%s.", catalog, schema)
        ws.schemas.create(name=schema, catalog_name=catalog, comment="Synthetic banking/fraud source tables")
        result["schema_created"] = True

    return result


def check_and_maybe_enable_external_access(ws: WorkspaceClient, metastore_id: str, enable: bool) -> dict:
    summary = ws.metastores.summary()
    enabled = bool(summary.external_access_enabled)
    result = {
        "metastore_id": metastore_id,
        "metastore_name": summary.name,
        "external_access_enabled_before": enabled,
    }
    if enabled:
        logger.info("Metastore %s already has external data access enabled.", metastore_id)
        result["external_access_enabled_after"] = True
        result["action"] = "none (already enabled)"
        return result

    if not enable:
        logger.warning(
            "Metastore %s (%s) does NOT have external data access enabled, and "
            "--enable-external-access was not passed. This blocks Fabric's mirrored Azure "
            "Databricks catalog from reading the underlying Delta data via shortcuts.",
            metastore_id, summary.name,
        )
        result["external_access_enabled_after"] = False
        result["action"] = "not attempted (pass --enable-external-access)"
        return result

    # NOTE: the workspace-scoped `PATCH /api/2.1/unity-catalog/metastores/{id}`
    # (i.e. WorkspaceClient.metastores.update) enforces metastore-admin on a
    # per-workspace-session basis and returned PERMISSION_DENIED for an
    # account-admin identity during development of this script, even though
    # that identity *does* have the needed rights. The account-level API
    # (AccountClient.metastores.update) is the one that actually works for an
    # account-admin identity, so that is what this function uses. The
    # account_id is auto-discovered by the SDK's azure-cli auth type
    # (ws.config.account_id) — no manual account-console lookup needed.
    account_id = ws.config.account_id
    if not account_id:
        result["external_access_enabled_after"] = False
        result["action"] = "BLOCKED: could not auto-discover Databricks account_id from the SDK config"
        logger.error(
            "Could not auto-discover the Databricks account_id from this workspace's SDK config. "
            "Pass it explicitly and call AccountClient(host=%r, account_id=<GUID>, auth_type='azure-cli')"
            ".metastores.update(metastore_id=%r, metastore_info=UpdateAccountsMetastore(external_access_enabled=True)).",
            ACCOUNTS_HOST, metastore_id,
        )
        return result

    acct = AccountClient(host=ACCOUNTS_HOST, account_id=account_id, auth_type="azure-cli")
    try:
        acct.metastores.update(
            metastore_id=metastore_id,
            metastore_info=UpdateAccountsMetastore(external_access_enabled=True),
        )
        after = ws.metastores.summary()
        result["external_access_enabled_after"] = bool(after.external_access_enabled)
        result["action"] = "enabled" if after.external_access_enabled else "update call made but flag still false"
        result["account_id"] = account_id
        logger.info("external_access_enabled now = %s (account_id=%s)", after.external_access_enabled, account_id)
    except DatabricksError as exc:
        result["external_access_enabled_after"] = False
        result["action"] = f"BLOCKED: {exc}"
        result["account_id"] = account_id
        logger.error(
            "Could not enable external data access on metastore %s (%s) via account_id=%s: %s\n"
            "This is a metastore-admin-only operation at the account level. Unblock by having an "
            "account admin either (a) grant this principal account-admin or metastore-admin rights "
            "in the Databricks account console (https://accounts.azuredatabricks.net -> User "
            "management), or (b) run directly: AccountClient(host=%r, account_id=%r, "
            "auth_type='azure-cli').metastores.update(metastore_id=%r, "
            "metastore_info=UpdateAccountsMetastore(external_access_enabled=True)).",
            metastore_id, summary.name, account_id, exc, ACCOUNTS_HOST, account_id, metastore_id,
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-host", required=True, help="https://adb-....azuredatabricks.net")
    parser.add_argument("--workspace-resource-id", required=True, help="ARM resource ID of the Databricks workspace")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name to use/create (default: reuse the workspace's auto-provisioned catalog)")
    parser.add_argument("--schema", required=True, help="Schema name to create in the catalog")
    parser.add_argument("--managed-location", default=None, help="abfss:// URL for a new catalog's managed storage root (only used if --catalog does not already exist and this is provided)")
    parser.add_argument("--enable-external-access", action="store_true", help="Attempt to enable metastore external data access (requires metastore-admin)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ws = workspace_client(args.workspace_host, args.workspace_resource_id)

    summary = ws.metastores.summary()
    result: dict = {
        "workspace_host": args.workspace_host,
        "metastore_id": summary.metastore_id,
        "metastore_name": summary.name,
        "metastore_region": summary.region,
    }
    logger.info("Workspace is assigned to metastore %r (%s) in region %s.", summary.name, summary.metastore_id, summary.region)

    catalog_schema_result = ensure_catalog_and_schema(ws, args.catalog, args.schema, args.managed_location)
    result.update(catalog_schema_result)
    result["catalog"] = args.catalog
    result["schema"] = args.schema

    access_result = check_and_maybe_enable_external_access(ws, summary.metastore_id, args.enable_external_access)
    result["external_data_access"] = access_result
    result["status"] = "ok"

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
