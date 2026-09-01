# Azure Cosmos DB → Fabric mirroring: what it requires, and what this POC built

Everything below was checked against current Microsoft Learn pages (fetched and read in
full, not recalled from memory) on 2026-09-01. Sources are cited inline; recheck them
before relying on this document for anything beyond this POC, since mirroring, networking,
and security limitations for it change.

- [Mirroring Azure Cosmos DB (overview)](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db)
- [Tutorial: Configure Fabric mirrored databases from Azure Cosmos DB](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-tutorial)
- [Limitations in Fabric mirrored databases from Azure Cosmos DB](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations)
- [FAQ: Fabric mirrored databases from Azure Cosmos DB](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-faq)
- [How to: Configure private networks for Azure Cosmos DB Fabric Mirroring](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-private-network) (last updated 2026-08-11 per the page itself)

## What Cosmos DB configuration mirroring requires

- **API for NoSQL only.** Mongo (RU-based), Gremlin, Table, Cassandra, and vCore-based
  DocumentDB accounts are not supported mirroring sources. ([limitations](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations))
  This POC's account is a plain API-for-NoSQL account, so this is satisfied by construction.
- **Continuous backup, 7-day or 30-day tier, and it can never be turned off again once
  enabled.** All of continuous backup's own limitations carry over to mirroring, including
  no support for multi-region write accounts. 7-day continuous backup is free; 30-day is
  billed. ([limitations](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations), [FAQ](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-faq))
  `infra/cosmos/main.bicep` enables `Continuous7Days` and the account is single-region
  (`isZoneRedundant: false`, one `locations` entry), so the multi-region-write restriction
  doesn't bind here — but it's a real constraint to flag if this pattern is ever reused for
  a production account that needs multi-region writes.
- **Connection authentication: read-write account keys, or Microsoft Entra ID with RBAC.
  Read-only account keys and managed identities are explicitly NOT supported for this
  connection.** ([limitations](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations))
  The required data-plane RBAC actions are `Microsoft.DocumentDB/databaseAccounts/readMetadata`
  and `readAnalytics`. This account has `disableLocalAuth: true` (no keys work at all, ever),
  which forces the only viable path to be Entra ID + RBAC — consistent with, and stricter
  than, what mirroring needs. Note this is about the auth Fabric's *mirroring connection*
  itself uses, not about how our own generator/loader code writes data into Cosmos (that
  part already uses `DefaultAzureCredential` + RBAC exclusively — see `cosmos/common.py`).
- **No customer-managed keys, and OneLake-side mirrored data doesn't support private
  endpoints or double encryption**, regardless of how the source account itself is
  configured. ([limitations](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations))
- Enabling mirroring in a workspace needs Admin or Member role on that workspace.

## Nested objects and arrays: confirmed representation

**Nested JSON objects and arrays in Cosmos DB documents are represented as JSON strings in
the mirrored Warehouse tables** — not flattened, not exploded into related tables. `OPENJSON`,
`CROSS APPLY`, and `OUTER APPLY` are the documented ways to expand them selectively in
T-SQL; Power Query's `ToJson` does the same in that surface. There's no schema constraint on
nesting depth. ([limitations, "Nested data limitations"](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations))

This directly affects how `digitalSessions.device`/`.geo`/`.authentication`/`.activities`,
`devices.geoHistory`/`.riskSignals`, and `fraudAlerts.signals`/`.investigatorNotes`/`.resolution`
will show up once mirrored: each as a single JSON-string column, not as queryable nested
columns, until unpacked with `OPENJSON` downstream.

Related schema behavior worth knowing before treating the mirror as a stable contract:
- New properties are detected automatically and become new columns; documents missing a
  property get `null` in that column — this POC's deliberate schema variation (some
  `devices` docs omit `geoHistory`; alerts only get `resolution` once closed) is exactly
  this case, by design (see `generators/generate_cosmos_data.py`'s docstring).
- If the same property holds different types across documents, mirroring upcasts where
  possible (parity with the Cosmos analytical store's own upcast rules) and otherwise
  produces `null` — e.g. an array property later seen as a string becomes `null`, not an
  error.
- There is no full-fidelity, versioned schema guarantee. Mirroring continuously tracks
  property/type changes rather than enforcing one.

## Partition-key considerations for mirroring

**Mirroring does not support custom partitioning** — whatever partition key the source
container uses is what mirroring uses; there's no separate mirrored-side partitioning
scheme to design. ([limitations, "Replication limitations"](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-limitations))

All three containers (`digitalSessions`, `devices`, `fraudAlerts`) use **`/customerId`** as
the partition key. Rationale: the dominant expected operational access pattern for this
POC's fraud/session domain is "get everything for this customer" — a fraud investigator or
an operational dashboard pulling a customer's sessions, devices, and alerts together, which
`/customerId` serves as single-partition reads. It also keeps the *cross-source join key*
(`customerId`, shared with the Databricks side — see below) as the physical partition
boundary, which is convenient for anyone joining the two mirrored sources downstream.

This is a genuine tradeoff, not a free win, and is being stated as one rather than swept
under the rug:
- **Potential hot partition for very high-volume customers.** A customer generating an
  unusually large number of sessions/alerts concentrates load and storage on one logical
  partition. At this POC's scale (hundreds to low thousands of documents per container) this
  never manifests, but it's the first thing to revisit before scaling this pattern up.
- **Queries by device, transaction, time, or status alone fan out across all partitions** —
  there's no secondary partitioning to avoid that, by mirroring's own limitation above.
- A customer's records can't be moved to a different logical partition after the fact
  without a full document rewrite (Cosmos DB doesn't support in-place partition-key changes).
- This POC's population (thousands of documents) is far too small to validate real
  production-scale distribution or skew. Production sizing needs observed RU, storage, and
  logical-partition metrics that this POC doesn't produce.

## Networking: what mirroring actually requires for a private-endpoint account, and what's built here

The tenant this POC runs in silently forces every new Cosmos DB account's
`publicNetworkAccess` back to `Disabled` on deployment, confirmed empirically (not assumed):
`infra/cosmos/main.bicep` was deployed once requesting `Enabled` with a single-IP allow-list
and the account came back `Disabled` anyway; a direct `az cosmosdb update
--public-network-access Enabled` afterward was independently blocked by this environment's
own operator safety controls. So this account is private-endpoint-only, unconditionally —
see `infra/cosmos/main.bicep`'s top-of-file comment for the exact language.

Given that starting point, Fabric's mirroring connection needs a way to reach a Cosmos DB
account that has no public network access at all. The current, GA path (per the
private-network guide) is:

1. **Control-plane access** (metadata reads Fabric needs during setup) is handled by the
   **Network ACL Bypass** feature: enabling the `EnableFabricNetworkAclBypass` capability on
   the Cosmos account, then authorizing a specific Fabric workspace's resource ID as a
   trusted resource via `networkAclBypass=AzureServices` +
   `networkAclBypassResourceIds=/tenants/<tenant>/subscriptions/.../workspaces/<workspaceId>`.
   Neither setting has a portal control; both are CLI/PowerShell-only.
2. **Data-plane access** (the actual replication traffic) is handled by a **Fabric virtual
   network data gateway** — a gateway Fabric provisions inside your own VNet, on a
   dedicated, delegated (`Microsoft.PowerPlatform/vnetaccesslinks`) subnet of at least `/27`.
   The gateway resolves the Cosmos account through its private endpoint (private DNS), so
   Fabric's service-tag IP ranges never need to be allow-listed anywhere.
3. Because the Fabric portal's mirroring UI can't select a virtual-network-gateway
   connection, **the mirrored database item itself has to be created via the Fabric REST
   API** (not the "New mirrored Azure Cosmos DB" portal flow), referencing the gateway
   connection by ID. This is a documented current product gap, not a mistake.
4. The gateway subnet needs outbound internet access to reach Microsoft Entra ID for its
   own OAuth sign-in — as of March 31, 2026 Azure retired default outbound access for new
   subnets, so a NAT gateway on that subnet is now required, not optional.

**What's actually built and deployed here:** `infra/cosmos/main.bicep` creates the VNet
(`cosmosfabricmsv2915d-vnet`), the private endpoint + private DNS zone
(`privatelink.documents.azure.com`, linked to the VNet), and a **reserved, empty, delegated
`snet-fabric` subnet** (`/27`, delegated to `Microsoft.PowerPlatform/vnetaccesslinks`) sized
and configured exactly per the guide above, ready for a Fabric virtual network data gateway.
`infra/cosmos/enable-fabric-mirroring.ps1` implements steps 3-5 of the guide (the RBAC
custom role, the ACL bypass capability, and the trusted-workspace authorization) —
parametrized on a Fabric workspace ID.

**What's not done, and exactly why:** steps 3-5 above need a Fabric workspace ID to
authorize, and step 6 needs that same workspace to host the gateway. No Fabric workspace
for this POC exists yet (`fabric-medallion-multisourcev2-poc` — confirmed absent from this
tenant's workspace list at the time of writing; workspace creation is out of this Cosmos
work's scope, see `infra/fabric/provision.py` in this repo). So `enable-fabric-mirroring.ps1`
has been written and is ready, but has never been run, and the gateway/connection/mirror
item (steps 6-8) haven't been attempted at all. **To unblock:** create the Fabric workspace,
pass its ID to `enable-fabric-mirroring.ps1`, then follow steps 6-8 of the private-network
guide (create the virtual network data gateway on `snet-fabric` with a NAT gateway attached,
create the OAuth-based Cosmos DB v2 connection through it, then create the mirrored database
via the Fabric REST API).

**How the real data got loaded without any of that being usable yet:** since the account
has no reachable public endpoint and no gateway exists, `cosmos/load_initial.py` and
`cosmos/load_incremental.py` couldn't run from an ordinary dev machine either.
`infra/cosmos/run_loader.ps1` runs them from a short-lived VM placed inside the same VNet
instead (see that script's docstring for why a VM was used instead of a VNet-integrated
Azure Container Instance — ACI could not reach the instance metadata service needed for
managed identity in this environment, confirmed by direct TCP testing, not just assumed).
The VM is deleted immediately after use; nothing is left running.

## Cost and performance notes (from the FAQ, not measured independently)

- Mirroring's replication itself does not consume the source account's Request Units or
  affect transactional workload performance or cost — it's built on continuous backup, not
  on querying the live database. ([FAQ](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-faq))
- Fabric's compute to replicate data is free; mirroring storage is free up to a
  capacity-based limit. Querying the mirrored data (SQL analytics endpoint, Power BI, Spark)
  is billed at normal Fabric compute rates. ([FAQ](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-faq))
- Using the Fabric Data Explorer against the *live* source data (as opposed to the mirrored
  copy) does consume RUs on the source account, unlike mirroring proper.

## Cross-source join convention (coordination with the Databricks side)

`customerId` and `deviceId` use the exact same string format as the Databricks-side
generator (`generators/generate_databricks_data.py`, built independently in parallel by
another agent in this same repo): `CUST-` + 6-digit zero-padded number (e.g. `CUST-000042`),
`DEV-` + 6-digit zero-padded number (e.g. `DEV-000042`). `fraudAlerts.transactionId` uses
that same generator's `TXN-` + 9-digit zero-padded convention (e.g. `TXN-000000001`), so a
fraud alert here can reference a Databricks transaction by ID even though the two sources
were never directly connected. This Cosmos side draws `customerId` values from `1..25000`
(`CUSTOMER_COUNT` in `.env.example`), a subset of the Databricks generator's own default
customer population — so overlap is guaranteed by construction, not verified against the
Databricks side's actual generated output (this repo's author hasn't cross-checked the two
datasets against each other; that's worth doing once both sides have run for real).
