#!/usr/bin/env python3
"""Idempotently set up Unity Catalog for this POC: metastore discovery/
assignment, catalog, and schema.

Order of operations:
  1. Find the Databricks account ID (from an already-provisioned workspace's
     metastore summary, or accept it explicitly via --account-id).
  2. Find an existing Unity Catalog metastore in the workspace's region
     (account-level metastores are one-per-region; reuse rather than create).
     If none exists, create one.
  3. Ensure the metastore is assigned to the target workspace.
  4. Create the POC catalog and schema (idempotent CREATE ... IF NOT EXISTS
     equivalents via the SDK).
  5. Report whether "external data access" is enabled on the metastore
     (required for Fabric's mirrored Azure Databricks catalog) and, if the
     caller has metastore-admin rights, enable it.

Auth: AAD via `az login`, exchanged for Databricks tokens through the SDK's
"azure-cli" auth type. No secrets are read, stored, or printed.

Usage:
    python infra/databricks/setup_unity_catalog.py --help
    python infra/databricks/setup_unity_catalog.py \\
        --workspace-host https://adb-XXXXXXXXXXXXXXXX.XX.azuredatabricks.net \\
        --workspace-resource-id /subscriptions/.../workspaces/dbw-fmv2poc-915d \\
        --catalog multisourcev2poc --schema banking
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import DatabricksError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("setup_unity_catalog")

ACCOUNTS_HOST = "https://accounts.azuredatabricks.net"


def workspace_client(host: str, resource_id: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, azure_workspace_resource_id=resource_id, auth_type="azure-cli")


def discover_account_id(ws: WorkspaceClient) -> str | None:
    """Try to learn the Databricks account_id from the workspace's own
    metastore summary (present once a metastore is assigned)."""
    try:
        summary = ws.metastores.summary()
        if summary.account_id:
            logger.info("Discovered account_id=%s from workspace metastore summary.", summary.account_id)
            return summary.account_id
    except DatabricksError as exc:
        logger.info("No metastore currently assigned to this workspace (%s).", exc)
    return None


def account_client(account_id: str) -> AccountClient:
    return AccountClient(host=ACCOUNTS_HOST, account_id=account_id, auth_type="azure-cli")


def get_or_create_metastore(acct: AccountClient, region: str, name: str):
    existing = list(acct.metastores.list())
    for m in existing:
        if m.region == region:
            logger.info("Reusing existing metastore %r (id=%s) in region %s.", m.name, m.metastore_id, region)
            return m, False
    logger.info("No metastore found in region %s among %d account metastore(s) — creating %r.", region, len(existing), name)
    from databricks.sdk.service.catalog import MetastoreInfo

    created = acct.metastores.create(name=name, region=region)
    return created, True


def ensure_metastore_assigned(acct: AccountClient, ws_id: int, metastore_id: str, default_catalog: str) -> None:
    assignments = list(acct.metastore_assignments.list(metastore_id))
    if any(a == ws_id for a in assignments):
        logger.info("Metastore %s already assigned to workspace %s.", metastore_id, ws_id)
        return
    logger.info("Assigning metastore %s to workspace %s.", metastore_id, ws_id)
    from databricks.sdk.service.catalog import MetastoreAssignment

    acct.metastore_assignments.create(
        ws_id, metastore_id, MetastoreAssignment(metastore_id=metastore_id, default_catalog_name=default_catalog)
    )


def ensure_catalog_and_schema(ws: WorkspaceClient, catalog: str, schema: str) -> None:
    existing_catalogs = {c.name for c in ws.catalogs.list()}
    if catalog not in existing_catalogs:
        logger.info("Creating catalog %r.", catalog)
        ws.catalogs.create(name=catalog, comment="Synthetic Databricks-source data for fabric-medallion-multisourcev2-poc")
    else:
        logger.info("Catalog %r already exists.", catalog)

    existing_schemas = {s.name for s in ws.schemas.list(catalog_name=catalog)}
    if schema not in existing_schemas:
        logger.info("Creating schema %s.%s.", catalog, schema)
        ws.schemas.create(name=schema, catalog_name=catalog, comment="Synthetic banking/fraud source tables")
    else:
        logger.info("Schema %s.%s already exists.", catalog, schema)


def check_and_maybe_enable_external_access(acct: AccountClient, metastore_id: str, enable: bool) -> dict:
    info = acct.metastores.get(metastore_id)
    enabled = bool(getattr(info, "external_access_enabled", False))
    result = {"metastore_id": metastore_id, "external_access_enabled_before": enabled}
    if enabled:
        logger.info("Metastore %s already has external data access enabled.", metastore_id)
        result["external_access_enabled_after"] = True
        result["action"] = "none (already enabled)"
        return result
    if not enable:
        logger.warning(
            "Metastore %s does NOT have external data access enabled, and --enable-external-access "
            "was not passed. This blocks Fabric's mirrored Azure Databricks catalog from reading "
            "the underlying Delta data via shortcuts. Only a metastore admin can enable this.",
            metastore_id,
        )
        result["external_access_enabled_after"] = False
        result["action"] = "not attempted (pass --enable-external-access with metastore-admin rights)"
        return result
    try:
        from databricks.sdk.service.catalog import UpdateMetastore

        acct.metastores.update(metastore_id, external_access_enabled=True)
        info2 = acct.metastores.get(metastore_id)
        after = bool(getattr(info2, "external_access_enabled", False))
        result["external_access_enabled_after"] = after
        result["action"] = "enabled" if after else "update call made but flag still false"
        logger.info("external_access_enabled now = %s", after)
    except DatabricksError as exc:
        result["external_access_enabled_after"] = False
        result["action"] = f"BLOCKED: {exc}"
        logger.error(
            "Could not enable external data access on metastore %s: %s. "
            "This requires metastore-admin rights on this account-level metastore. "
            "Ask the account admin to either grant this principal metastore-admin, or run: "
            "AccountClient(...).metastores.update(metastore_id=%r, external_access_enabled=True)",
            metastore_id, exc, metastore_id,
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-host", required=True, help="https://adb-....azuredatabricks.net")
    parser.add_argument("--workspace-resource-id", required=True, help="ARM resource ID of the Databricks workspace")
    parser.add_argument("--workspace-id", type=int, default=None, help="Numeric Databricks workspace ID (auto-detected if omitted)")
    parser.add_argument("--region", default="westus3", help="Azure region for metastore lookup/creation (default westus3)")
    parser.add_argument("--account-id", default=None, help="Databricks account UUID (auto-discovered if a metastore is already assigned)")
    parser.add_argument("--metastore-name", default="fabric-multisourcev2-poc-westus3", help="Name to use if a new metastore must be created")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name to create")
    parser.add_argument("--schema", required=True, help="Schema name to create in the catalog")
    parser.add_argument("--enable-external-access", action="store_true", help="Attempt to enable metastore external data access (requires metastore-admin)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ws = workspace_client(args.workspace_host, args.workspace_resource_id)

    ws_id = args.workspace_id
    if ws_id is None:
        # workspace_id is embedded as the numeric prefix of the ADB host, e.g. adb-7405606923779130.10...
        host_num = args.workspace_host.split("//adb-")[-1].split(".")[0]
        ws_id = int(host_num)
        logger.info("Derived numeric workspace_id=%d from host.", ws_id)

    account_id = args.account_id or discover_account_id(ws)
    result: dict = {"workspace_host": args.workspace_host, "workspace_id": ws_id}

    if not account_id:
        logger.error(
            "Could not determine the Databricks account_id automatically (no metastore is yet "
            "assigned to this workspace, so its summary is empty). Pass --account-id explicitly. "
            "Find it at https://accounts.azuredatabricks.net after signing in with this AAD identity."
        )
        result["status"] = "BLOCKED: unknown account_id"
        print(json.dumps(result, indent=2))
        return 1

    acct = account_client(account_id)
    result["account_id"] = account_id

    metastore, created = get_or_create_metastore(acct, args.region, args.metastore_name)
    result["metastore_id"] = metastore.metastore_id
    result["metastore_name"] = metastore.name
    result["metastore_created"] = created

    ensure_metastore_assigned(acct, ws_id, metastore.metastore_id, default_catalog=args.catalog)

    access_result = check_and_maybe_enable_external_access(acct, metastore.metastore_id, args.enable_external_access)
    result["external_data_access"] = access_result

    ensure_catalog_and_schema(ws, args.catalog, args.schema)
    result["catalog"] = args.catalog
    result["schema"] = args.schema
    result["status"] = "ok"

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
