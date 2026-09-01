#!/usr/bin/env python
"""Create (idempotently) as much of the Fabric Mirrored Azure Cosmos DB
database item as this session can complete unattended, and report the
precise manual step that remains.

This follows the private-network mirroring path documented in
docs/cosmos-fabric-mirroring.md and
https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-private-network
(steps numbered as in that guide):

  Steps 1-2 (delegated snet-fabric subnet)      -- done: infra/cosmos/main.bicep
  Steps 3-5 (Cosmos RBAC + Network ACL Bypass)   -- BLOCKED this session, see below
  Step 6   (Fabric virtual network data gateway) -- done by this script (idempotent)
  Step 7   (Cosmos DB v2 connection through the
            gateway, OAuth 2.0)                  -- BLOCKED, portal-only (see below)
  Step 8   (mirrored database item + startMirroring) -- this script will do it,
            but only once step 7 has been completed by a human and a
            matching connection exists to find

Status of steps 3-5, empirically (not assumed)
------------------------------------------------
This script does NOT attempt steps 3-5. They were attempted directly
(both via the repo's infra/cosmos/enable-fabric-mirroring.ps1 and via
equivalent `az cosmosdb` CLI calls) during this session and every mutating
call -- the Cosmos DB SQL role assignment, and the `az cosmosdb update`
calls for the EnableFabricNetworkAclBypass capability and networkAclBypass
trusted-workspace authorization -- was denied by this session's own
auto-mode operator-safety controls (account-level security/IAM mutations
are blocked by design, not by a Cosmos or Fabric API error). This is a
session-permission blocker, not a product limitation: a human operator (or
a session with those specific Bash permissions granted) can run

    pwsh infra/cosmos/enable-fabric-mirroring.ps1 \\
        -ResourceGroup rg-fabric-medallion-multisourcev2-poc-westus3 \\
        -AccountName cosmosfabricmsv2915d \\
        -FabricWorkspaceId 7e206237-aef1-4932-9f94-1f6ae343407a

directly (needs an interactive `Connect-AzAccount` first -- this session
has az CLI auth but no interactive Az PowerShell session) to complete
steps 3-5. Verify with:

    az cosmosdb show -g rg-fabric-medallion-multisourcev2-poc-westus3 \\
        -n cosmosfabricmsv2915d \\
        --query "{capabilities:capabilities[].name,networkAclBypass:networkAclBypass,bypassIds:networkAclBypassResourceIds}"

Status of the gateway (step 6): DONE, verified live
-----------------------------------------------------
POST https://api.fabric.microsoft.com/v1/gateways with
{"type": "VirtualNetwork", "displayName": "fmv2poc-cosmos-vnet-gateway",
 "capacityId": "<fabricmsv2poc915d capacity id>",
 "virtualNetworkAzureResource": {"subscriptionId": ..., "resourceGroupName":
 ..., "virtualNetworkName": "cosmosfabricmsv2915d-vnet", "subnetName":
 "snet-fabric"}, "inactivityMinutesBeforeSleep": 30,
 "numberOfMemberGateways": 1} returned 201 with a real gateway id, confirmed
via a follow-up GET. This is a genuine Fabric-managed cloud resource
(unlike an on-premises gateway, there's no local installer/registration
step) so this part of the guide's Step 6 -- despite the Microsoft Learn
walkthrough presenting it as a portal-only action -- is in fact fully
REST-API-automatable and unattended. This script re-runs that check first.
The subnet still has no NAT gateway attached (confirmed via `az network
vnet subnet show`), which the guide says is required after March 31, 2026
for the gateway's own outbound OAuth sign-in to Entra ID to succeed --
this doesn't block the script from creating the gateway *resource*, but it
will make Step 7's OAuth sign-in fail even once a human reaches the portal
step, until a NAT gateway is attached to snet-fabric (see the guide's
"Gateway OAuth invalid token error" section, and infra/cosmos/main.bicep
which does not yet provision one).

Status of the connection (step 7): confirmed BLOCKED via a live API error,
not just inferred from docs
-----------------------------------------------------------------------------
POST https://api.fabric.microsoft.com/v1/connections with
connectivityType="VirtualNetworkGateway", gatewayId=<the gateway above>,
connectionDetails.type="CosmosDB", credentialDetails.credentials=
{"credentialType": "OAuth2", "UseCallerIdentity": true} returned:

    400 OAuth2CredentialsNotSupportedForConnection
    "The connectivity type 'VirtualNetworkGateway' is not supported for
    OAuth2 credentials."

This is a hard REST API rejection, not an interactive-flow error message
(contrast with the Databricks connection's OAuth2 attempt, which got
further before failing on a redirect requirement -- see
mirror_databricks.py's docstring). Combined with the Cosmos private-network
guide's own statement that "Private-network mirroring supports OAuth-based
authentication only" (Key/account-key auth is separately a dead end here
regardless, since this account has disableLocalAuth=true), there is no
programmatic way to create this specific connection. The Fabric portal's
"New connection" dialog (Settings -> Manage connections and gateways ->
Connections -> + New -> connectivity type Virtual network -> connection
type "Azure Cosmos DB v2" -> Authentication method "OAuth 2.0" -> Edit
credentials -> sign in) is the only way to complete this step. This is a
genuine, confirmed product gap for unattended/API-only Cosmos DB private-
network mirroring setup, not a workaround-able one.

What this script actually does
-------------------------------
1. Ensure the virtual network data gateway exists (idempotent; the real
   step 6 work).
2. Look for an existing VirtualNetworkGateway-connectivity CosmosDB
   connection pointed at this Cosmos account (i.e. has a human already
   done step 7 in the portal since this was last run).
   - If found: ensure the MirroredDatabase item exists referencing it,
     call startMirroring, and report status (step 8, completable
     end-to-end once step 7 is done by a human).
   - If not found: print the exact manual action needed and exit 0
     (this is an expected, not erroneous, stopping point -- re-run this
     script after a human completes step 7).

Usage:
    python infra/fabric/mirror_cosmos.py --help
    python infra/fabric/mirror_cosmos.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from .auth import FABRIC_API, get_session
from .common import find_item, find_workspace, list_all
from .definitions import item_definition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
log = logging.getLogger("mirror_cosmos")

GATEWAY_NAME = "fmv2poc-cosmos-vnet-gateway"
MIRROR_ITEM_NAME = "fmv2poc_cosmos_multisource_mirror"
MIRRORING_STATUS_POLL_SECONDS = 15
MIRRORING_STATUS_TIMEOUT_SECONDS = 5 * 60


def ensure_gateway(session, capacity_id: str, subscription_id: str, resource_group: str, vnet_name: str, subnet_name: str) -> str:
    gateways = list_all(session, f"{FABRIC_API}/gateways")
    existing = next((g for g in gateways if (g.get("displayName") or "") == GATEWAY_NAME), None)
    if existing:
        log.info("Gateway %r already exists (%s) -- reusing.", GATEWAY_NAME, existing["id"])
        return existing["id"]

    body = {
        "type": "VirtualNetwork",
        "displayName": GATEWAY_NAME,
        "capacityId": capacity_id,
        "virtualNetworkAzureResource": {
            "subscriptionId": subscription_id,
            "resourceGroupName": resource_group,
            "virtualNetworkName": vnet_name,
            "subnetName": subnet_name,
        },
        "inactivityMinutesBeforeSleep": 30,
        "numberOfMemberGateways": 1,
    }
    resp = session.post(f"{FABRIC_API}/gateways", json=body)
    if not resp.ok:
        log.error("POST /gateways -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    gateway = resp.json()
    log.info("Created gateway %r (%s)", GATEWAY_NAME, gateway["id"])
    return gateway["id"]


def find_cosmos_vnet_connection(session, cosmos_host: str, gateway_id: str) -> dict | None:
    """Prefers a connection bound to our own gateway_id -- more than one
    VirtualNetworkGateway CosmosDB connection to the same host can exist
    (e.g. a leftover from an earlier, unrelated attempt at this POC), and
    picking the wrong one would silently mirror through a gateway we don't
    control or haven't verified."""
    connections = list_all(session, f"{FABRIC_API}/connections")
    candidates = [
        c for c in connections
        if (c.get("connectionDetails") or {}).get("type") == "CosmosDB"
        and c.get("connectivityType") == "VirtualNetworkGateway"
        and cosmos_host in ((c.get("connectionDetails") or {}).get("path") or "")
    ]
    on_our_gateway = [c for c in candidates if c.get("gatewayId") == gateway_id]
    if on_our_gateway:
        return on_our_gateway[0]
    if len(candidates) > 1:
        log.warning(
            "%d Cosmos VNet connections found for host %r, none bound to our gateway %r -- "
            "using the first (%r, %s). Verify this is the intended one.",
            len(candidates), cosmos_host, gateway_id, candidates[0].get("displayName"), candidates[0]["id"],
        )
    return candidates[0] if candidates else None


def ensure_mirrored_database(session, workspace_id: str, connection_id: str, cosmos_database: str) -> str:
    existing = find_item(session, workspace_id, MIRROR_ITEM_NAME, "MirroredDatabase")
    if existing:
        log.info("MirroredDatabase %r already exists (%s) -- reusing.", MIRROR_ITEM_NAME, existing["id"])
        return existing["id"]

    mirroring_json = {
        "properties": {
            "source": {"type": "CosmosDb", "typeProperties": {"connection": connection_id, "database": cosmos_database}},
            "target": {"type": "MountedRelationalDatabase", "typeProperties": {"defaultSchema": "dbo", "format": "Delta"}},
        }
    }
    definition = item_definition("MirroredDatabase", MIRROR_ITEM_NAME, "mirroring.json", mirroring_json)
    body = {
        "displayName": MIRROR_ITEM_NAME,
        "description": "Mirrored Azure Cosmos DB database (multisource) -- fabric-medallion-multisourcev2-poc Bronze.",
        "definition": definition,
    }
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/mirroredDatabases", json=body)
    if not resp.ok:
        log.error("POST mirroredDatabases -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    item = resp.json()
    log.info("Created MirroredDatabase %r (%s)", MIRROR_ITEM_NAME, item["id"])
    return item["id"]


def start_and_poll_mirroring(session, workspace_id: str, item_id: str) -> dict:
    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/mirroredDatabases/{item_id}/startMirroring")
    if resp.status_code not in (200, 202):
        log.error("startMirroring -> %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()

    deadline = time.time() + MIRRORING_STATUS_TIMEOUT_SECONDS
    while True:
        resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getMirroringStatus")
        resp.raise_for_status()
        status = resp.json()
        log.info("mirroring status=%s", status.get("status"))
        if status.get("status") in ("Running", "Stopped", "Failed"):
            return status
        if time.time() > deadline:
            log.warning("Timed out waiting for a terminal mirroring status.")
            return status
        time.sleep(MIRRORING_STATUS_POLL_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    load_dotenv()
    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    capacity_name = os.environ["FABRIC_CAPACITY_NAME"]
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["RESOURCE_GROUP"]
    cosmos_account = os.environ["COSMOS_ACCOUNT_NAME"]
    cosmos_database = os.environ["COSMOS_DATABASE_NAME"]
    cosmos_host = f"{cosmos_account}.documents.azure.com"
    vnet_name = f"{cosmos_account}-vnet"

    session = get_session()
    workspace = find_workspace(session, workspace_name)
    if not workspace:
        raise RuntimeError(f"Fabric workspace {workspace_name!r} not found")
    workspace_id = workspace["id"]

    capacities = list_all(session, f"{FABRIC_API}/capacities")
    capacity = next((c for c in capacities if c["displayName"].lower() == capacity_name.lower()), None)
    if not capacity:
        raise RuntimeError(f"capacity {capacity_name!r} not found")

    gateway_id = ensure_gateway(session, capacity["id"], subscription_id, resource_group, vnet_name, "snet-fabric")

    connection = find_cosmos_vnet_connection(session, cosmos_host, gateway_id)
    if not connection:
        message = (
            f"BLOCKED at Step 7 (manual): no VirtualNetworkGateway Azure Cosmos DB v2 connection "
            f"found for host {cosmos_host!r}. This step cannot be done via the REST API -- confirmed "
            f"live, POSTing credentialType OAuth2 for a VirtualNetworkGateway connection returns "
            f"400 OAuth2CredentialsNotSupportedForConnection. A human must open the Fabric portal: "
            f"Settings -> Manage connections and gateways -> Connections -> + New -> "
            f"connectivity type 'Virtual network' -> Gateway cluster name '{GATEWAY_NAME}' -> "
            f"connection type 'Azure Cosmos DB v2' -> Cosmos DB Endpoint 'https://{cosmos_host}:443/' -> "
            f"Authentication method 'OAuth 2.0' -> Edit credentials -> sign in. Also attach a NAT "
            f"gateway to the snet-fabric subnet first if not already done (required for the "
            f"gateway's own outbound OAuth sign-in to Entra ID) -- see docs/cosmos-fabric-mirroring.md. "
            f"Re-run this script after that connection exists to complete Step 8 (mirrored database "
            f"item + startMirroring)."
        )
        log.warning(message)
        print(json.dumps({
            "workspace_id": workspace_id,
            "gateway_id": gateway_id,
            "status": "blocked",
            "blocked_at": "step_7_cosmos_v2_connection_portal_only",
            "message": message,
        }, indent=2))
        return 0

    log.info("Found existing Cosmos VNet gateway connection %r (%s) -- proceeding to mirrored database.", connection["displayName"], connection["id"])
    item_id = ensure_mirrored_database(session, workspace_id, connection["id"], cosmos_database)
    status = start_and_poll_mirroring(session, workspace_id, item_id)

    print(json.dumps({
        "workspace_id": workspace_id,
        "gateway_id": gateway_id,
        "connection_id": connection["id"],
        "mirror_item_id": item_id,
        "mirror_item_name": MIRROR_ITEM_NAME,
        "mirroring_status": status,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
