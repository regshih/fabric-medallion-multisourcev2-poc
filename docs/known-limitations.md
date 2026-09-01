# Known limitations and findings

## Resolved blockers (kept for the record — see README's evidence table for current status)

Both mirrors were fully deployed and reported healthy well before their data was actually
readable. Both blockers were the same shape: an automation-mintable credential
(`credentialType: "Key"` for Databricks, no connection at all for Cosmos, since the required
combination hard-rejects the only unattended credential type) that Fabric's read paths don't
accept — fixed only by a human doing an interactive OAuth2 sign-in in the Fabric portal.

- **Databricks**: connection `fmv2poc-databricks-catalog-mirror-connection` switched from
  Key (PAT) to OAuth2. Confirmed live across the same three paths that originally failed
  (direct mirror read, Lakehouse shortcut, SQL analytics endpoint). Notably, the fix did not
  take effect immediately: reads kept failing with the pre-fix error for roughly 20 minutes
  after the credential edit, ruled out shortcut-level caching (recreated a shortcut from
  scratch, same error), and only started working after that propagation window passed. A
  second, unrelated blocker then surfaced (see "Databricks Runtime auto-upgrades" below).
  Exact row-count match to source confirmed after both fixes: 750,000 / 750,000 / 1,500. See
  [docs/databricks-fabric-integration.md](databricks-fabric-integration.md) for the full
  trace.
- **Cosmos**: connection `fmv2poc-cosmos-vnet-connection` created via the portal-only flow
  (Virtual network / gateway / Azure Cosmos DB v2 / OAuth2). `startMirroring` succeeded,
  status `Running`, exact row-count match to source (1,201 / 400 / 151) confirmed via direct
  SQL query against the mirror's own analytics endpoint. See
  [docs/cosmos-fabric-mirroring.md](cosmos-fabric-mirroring.md) for the full trace.
- **Pipeline execution**: `pl_multisource_medallion` now runs end to end with all 20
  reconciliation checks passing. Getting there required fixing a chain of real, independently
  discovered bugs (each found by actually running the pipeline, not by static review):
  1. **Databricks Runtime auto-upgrades new Unity Catalog managed tables to the
     `catalogManaged` (coordinated-commits) table feature by default on sufficiently recent
     runtimes** (confirmed live: DBR 16.4 has this default, DBR 15.4 LTS does not). Fabric's
     mirrored-Databricks-catalog OneLake shortcuts cannot resolve `catalogManaged` tables at
     all — not a Delta-reader-version problem (a newer Fabric Spark runtime recognizes the
     feature flag but then fails differently, `IllegalStateException: Couldn't locate commit
     coordinator`, since the shortcut has no live connection back to Unity Catalog's own
     commit coordinator service). Fixed by pinning the seed job's Databricks Runtime selection
     to 15.4 LTS instead of always picking the newest available version
     (`infra/databricks/run_job.py`) and re-seeding. **This is a forward-looking risk, not a
     one-time fix**: a future Databricks Runtime LTS release could reintroduce this default,
     silently breaking the mirror again. Re-verify before bumping `PINNED_SPARK_VERSION_PREFIX`.
  2. Recreating the Unity Catalog tables (to apply fix 1) gave them new internal UUIDs, which
     broke the Fabric mirror's shortcut resolution (`TABLE_DOES_NOT_EXIST`) even though the
     table names were unchanged — `autoSync: Enabled` did not pick this up on its own within
     15 minutes of passive waiting. Fixed by re-applying the mirror item's own definition via
     `updateDefinition` (identical content), which forced an immediate resync.
  3. **A Fabric Data Pipeline `TridentNotebook` activity internally invokes its target
     notebook via `notebookutils.notebook.run`, which enforces that the notebook being run and
     the "root" pipeline context share the same default lakehouse** — confirmed live via
     `NotebookExecutionException: Cannot reference a Notebook that attaching to a different
     default lakehouse`. Every notebook in this pipeline that logs its own completion via a
     `notebookutils.notebook.run("pipeline_log", ...)` call needed
     `"useRootDefaultLakehouse": True` added to that call's arguments dict — including
     notebooks whose own default lakehouse already matched `pipeline_log`'s, added for
     consistency. See the [Notebook.run documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-notebook-run).
  4. Several Cosmos document field names assumed in `nb_silver_transform.py` and
     `nb_gold_build.py`, written speculatively before the real Cosmos generator was finalized,
     didn't match reality: `device.os`→`device.operatingSystem`, no `device.browser` field at
     all, `geo.ipAddress` doesn't exist (the real field, `ipAddress`, is top-level, not nested
     under `geo`), `authentication.success` doesn't exist (real field: `failedAttempts`, an
     int, not a boolean), `fraudAlerts.alertTimestamp`→`createdTimestamp`, and `severity`
     values are lowercase (`low/medium/high/critical`), not PascalCase — the original
     PascalCase quality check would have silently quarantined every real fraud alert row.
     `GeographicAnomalyCount`'s entire original design assumed each `geoHistory[]` entry
     carried its own `isAnomaly` flag; no such field exists anywhere in the real schema —
     redefined using data that actually exists (a session is anomalous when its country isn't
     one the device's own travel history has ever recorded).
  5. `FactTransactions`'s `.select()` computed a `CustomerSK` surrogate key but never carried
     the natural `CustomerID` column through to the final Gold table — a plain oversight,
     unrelated to the Cosmos-schema issues above, that broke `AggCustomerRiskProfile`'s
     `groupBy("CustomerID")`.
  6. `nb_reconciliation.py`'s Silver-vs-Gold check list had `("merchants", "merchants")` where
     the real Gold table name is `dimmerchant` — a copy-paste slip. Separately, its Databricks
     source-vs-Silver checks tried `spark.table()` on a Lakehouse shortcut living in a
     *different* lakehouse (`silver_lh`) than this notebook's own default (`gold_lh`) — same
     underlying constraint as item 3, fixed by reading via the shortcut's OneLake ABFSS path
     instead of the Spark catalog.

## Product limitations affecting the design

### OneLake column-level security

Only `columnEffect: "Permit"` is supported for a `dataAccessRoles` column constraint —
`"Deny"` (this project's original design: a blanket `Path: "*"` Permit plus an explicit Deny
on the sensitive columns) is rejected outright server-side with a `PolicyValidationError`
("Column level security only supports Permit effect"), confirmed live 2026-09-01, not assumed
from docs. `infra/governance/onelake_security.py` was redesigned as an allow-list: each
governed table gets an explicit Permit naming every column *except* the sensitive one(s) —
anything not named is implicitly denied. This means the constraint list must be kept in sync
with the table's actual schema (a new column added to `DimCustomer`/`DimDevice` needs adding
to `TABLE_ALLOWED_COLUMNS` too, or it silently becomes invisible under the role).

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
