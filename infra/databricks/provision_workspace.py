#!/usr/bin/env python3
"""Idempotently provision the Azure Databricks Premium workspace for this POC.

Premium tier is required for Unity Catalog support and for Fabric's mirrored
Azure Databricks catalog integration. Auth is via ``DefaultAzureCredential``
(``az login``) — no keys or service principal secrets are used or written
anywhere.

Usage:
    python infra/databricks/provision_workspace.py --help
    python infra/databricks/provision_workspace.py \\
        --subscription-id <sub-id> \\
        --resource-group rg-fabric-medallion-multisourcev2-poc-westus3 \\
        --workspace-name dbw-fmv2poc-915d --location westus3

Safe to re-run: if a workspace with the given name already exists in the
resource group, its properties are printed and no create call is made.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.databricks.models import Sku, Workspace

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("provision_workspace")


def get_or_create_workspace(
    subscription_id: str,
    resource_group: str,
    workspace_name: str,
    location: str,
    tags: dict[str, str],
) -> Workspace:
    credential = DefaultAzureCredential()
    client = AzureDatabricksManagementClient(credential, subscription_id)

    try:
        existing = client.workspaces.get(resource_group, workspace_name)
        logger.info(
            "Workspace %r already exists (provisioningState=%s, sku=%s) — skipping create.",
            workspace_name,
            existing.provisioning_state,
            existing.sku.name if existing.sku else None,
        )
        return existing
    except Exception as exc:  # azure.core.exceptions.ResourceNotFoundError, but keep it broad-safe
        if "ResourceNotFound" not in type(exc).__name__ and "ResourceNotFound" not in str(exc):
            raise
        logger.info("Workspace %r not found in %s — creating.", workspace_name, resource_group)

    managed_rg_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/rg-{workspace_name}-managed"
    )
    workspace = Workspace(
        location=location,
        sku=Sku(name="premium"),
        tags=tags,
        managed_resource_group_id=managed_rg_id,
        parameters=None,
    )
    poller = client.workspaces.begin_create_or_update(
        resource_group, workspace_name, workspace
    )
    logger.info("Create/update submitted; waiting for provisioning to complete (this can take ~10 minutes)...")
    result = poller.result()
    logger.info(
        "Workspace %r provisioning finished: state=%s, url=%s",
        workspace_name,
        result.provisioning_state,
        result.workspace_url,
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--subscription-id", required=True, help="Azure subscription ID")
    parser.add_argument("--resource-group", required=True, help="Resource group (must already exist)")
    parser.add_argument("--workspace-name", required=True, help="Databricks workspace name (globally-unique-ish)")
    parser.add_argument("--location", default="westus3", help="Azure region (default: westus3)")
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra tag, may be repeated. Defaults already include project/environment/synthetic-data tags.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tags = {
        "project": "fabric-medallion-multisourcev2-poc",
        "environment": "poc",
        "synthetic-data": "true",
    }
    for kv in args.tag:
        if "=" not in kv:
            raise ValueError(f"--tag must be KEY=VALUE, got: {kv!r}")
        key, value = kv.split("=", 1)
        tags[key] = value

    workspace = get_or_create_workspace(
        subscription_id=args.subscription_id,
        resource_group=args.resource_group,
        workspace_name=args.workspace_name,
        location=args.location,
        tags=tags,
    )
    print(
        json.dumps(
            {
                "name": workspace.name,
                "id": workspace.id,
                "workspace_url": workspace.workspace_url,
                "provisioning_state": workspace.provisioning_state,
                "location": workspace.location,
                "sku": workspace.sku.name if workspace.sku else None,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
