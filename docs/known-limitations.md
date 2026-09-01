# Known limitations and findings

## Current blockers (as of 2026-09-01 — see README's evidence table for current status)

- Cosmos DB mirroring for a private-endpoint-only account needs a Fabric virtual network data
  gateway on a reserved subnet — a real, documented product requirement, not a workaround
  avoided out of laziness. All networking is done and verified; only the portal-only OAuth
  connection sign-in remains, human-only. See [docs/cosmos-fabric-mirroring.md](cosmos-fabric-mirroring.md)
  for exactly what's built vs. what remains.
- The Databricks mirror item is deployed and healthy but its data isn't readable from any
  Fabric surface (Spark, Lakehouse shortcuts, or the SQL analytics endpoint) because its
  connection uses a Databricks PAT (`credentialType: "Key"`), which Fabric doesn't support
  for OneLake-shortcut-resolution-based reads — confirmed live across three independent
  access paths, not inferred. Fixing this needs real interactive auth (Organizational-account
  OAuth sign-in, or creating a Service Principal, both blocked in this automated session) —
  human-only. See [docs/databricks-fabric-integration.md](databricks-fabric-integration.md)
  "Fabric mirror: deployed, but not consumable" for the full trace and exact fix steps.
- Warehouse governance SQL and the pipeline's Silver/Gold/reconciliation stages depend on
  both source mirrors being readable — sequenced after mirroring in the deployment order.

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
