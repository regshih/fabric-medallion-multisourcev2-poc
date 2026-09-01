# Known limitations and findings

## Resolved blockers (kept for the record — see README's evidence table for current status)

Both mirrors were fully deployed and reported healthy well before their data was actually
readable. Both blockers were the same shape: an automation-mintable credential
(`credentialType: "Key"` for Databricks, no connection at all for Cosmos, since the required
combination hard-rejects the only unattended credential type) that Fabric's read paths don't
accept — fixed only by a human doing an interactive OAuth2 sign-in in the Fabric portal.

- **Databricks**: connection `fmv2poc-databricks-catalog-mirror-connection` switched from
  Key (PAT) to OAuth2. Confirmed live across the same three paths that originally failed
  (direct mirror read, Lakehouse shortcut, SQL analytics endpoint) — exact row-count match to
  source (750,025 / 750,025 / 1,501). Notably, the fix did not take effect immediately: reads
  kept failing with the pre-fix error for roughly 20 minutes after the credential edit, ruled
  out shortcut-level caching (recreated a shortcut from scratch, same error), and only started
  working after that propagation window passed. See
  [docs/databricks-fabric-integration.md](databricks-fabric-integration.md) for the full
  trace.
- **Cosmos**: connection `fmv2poc-cosmos-vnet-connection` created via the portal-only flow
  (Virtual network / gateway / Azure Cosmos DB v2 / OAuth2). `startMirroring` succeeded,
  status `Running`, exact row-count match to source (1,201 / 400 / 151) confirmed via direct
  SQL query against the mirror's own analytics endpoint. See
  [docs/cosmos-fabric-mirroring.md](cosmos-fabric-mirroring.md) for the full trace.

## Product limitations affecting the design

### Mirrored Azure Databricks catalog

- Metadata + OneLake shortcuts only — the underlying Delta data is never copied into OneLake.
  Propagation of underlying changes can take seconds to minutes; no latency SLA.
- Unity Catalog policies (RLS/column-mask/ABAC, table/schema/catalog permissions) do **not**
  carry over to Fabric — the mirror is a new, separate authorization boundary. Tables with
  RLS/column-mask policies are excluded from mirroring outright.
- Views, materialized/streaming/federated/Delta Sharing tables, and non-Delta external tables
  are not mirrorable. Schema/table renames break mirror tracking.

Full detail, with Microsoft Learn citations: [docs/databricks-fabric-integration.md](databricks-fabric-integration.md).

### Mirrored Azure Cosmos DB

- API for NoSQL only. Continuous backup (7- or 30-day) is required and, once enabled, can
  never be disabled again — a one-way door on the source account.
- Nested objects/arrays land as JSON **string** columns in the mirror, not flattened —
  `OPENJSON`/`CROSS APPLY` needed downstream.
- No custom target partitioning — the mirror inherits the source container's partition key
  exactly.
- Mirrored OneLake data doesn't support private endpoints, customer-managed keys, or double
  encryption, regardless of the source account's own configuration.

Full detail: [docs/cosmos-fabric-mirroring.md](cosmos-fabric-mirroring.md).

### POC implementation choices

- Silver and Gold are full refreshes on every pipeline run — deterministic and retry-safe at
  this data volume, not a production incremental-processing design.
- `AggCustomerRiskProfile`'s score is an **explainable synthetic heuristic**, not a trained,
  calibrated, or decision-suitable model — see [ARCHITECTURE.md](../ARCHITECTURE.md).
- The two sources use independent, source-relative 30-day watermarks rather than one shared
  enterprise as-of timestamp — intentional, since the two sources' clocks are asynchronous by
  construction (generated/loaded independently).
- Warehouse security-bearing objects (`_base_DimCustomer`, `_base_DimDevice`,
  `_base_AggCustomerRiskProfile`) are physical CTAS copies, not zero-copy views — native DDM/
  RLS cannot attach to a cross-database view or to a Lakehouse table directly (live-verified
  constraint, see the sibling banking POC's CLAUDE.md).
- Neither Warehouse masking nor OneLake column-level security enforcement could be
  independently verified against this project's own provisioning identity — both are
  configured and confirmed stored, not confirmed enforced against a genuinely
  lesser-privileged principal (none was available in this environment).
- The Databricks-side generator's default customer population (50,000) is larger than the
  Cosmos-side generator's (25,000) — intentional: every Cosmos `customerId` is guaranteed to
  exist on the Databricks side (subset by construction), while roughly half of Databricks
  customers correctly have no session/device/alert data, a realistic "transacts but never
  uses digital banking" case for `AggCustomerRiskProfile`, not a data bug.

## Documentation freshness

Fabric mirroring, networking, and security product behavior changes. Recheck the linked
Microsoft Learn pages before relying on this document beyond this POC's own point-in-time
findings (reviewed 2026-09-01).
