# Microsoft Fabric Multisource Medallion POC

A Microsoft Fabric medallion (Bronze/Silver/Gold) proof of concept combining two source
systems with deliberately different mirroring mechanics — **Azure Databricks (Unity
Catalog)**, mirrored as metadata + zero-copy OneLake shortcuts, and **Azure Cosmos DB for
NoSQL**, mirrored as a continuously-replicated physical Delta copy — into one Fabric
workspace. The primary analytical outcome, `AggCustomerRiskProfile`, answers a question
neither source can answer alone: a per-customer fraud-risk score blending Databricks
transaction/risk data with Cosmos digital-session/device/fraud-alert data.

All data is synthetic and fictional — see [SECURITY.md](SECURITY.md).

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design (business-key contract, Silver
quality rules, the exact Gold schema and risk-score formula, Warehouse governance design).

## Evidence status

Labels used consistently below and throughout this repo's docs: **Implemented** (code
exists, passes local checks) · **Deployed** (a real Azure/Fabric resource exists) ·
**Executed** (a job/pipeline actually ran) · **Verified** (an explicit check observed the
expected result) · **Blocked** (a named prerequisite prevents the next step). Never infer
Verified from Deployed.

As of 2026-09-01:

| Area | Status |
|---|---|
| Azure resource group, dedicated Fabric capacity (F2) | Deployed |
| Azure Databricks Premium workspace, Unity Catalog | Deployed |
| Unity Catalog external data access | Blocked in an earlier, unrelated attempt at this POC; **resolved here** via the account-level API — see [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md) |
| Databricks synthetic data (transactions, risk scores, merchants) | Executed and **verified**: 750,000 transactions / 750,000 risk scores / 1,500 merchants live in Unity Catalog, all quality checks green (`validation/validate_databricks.py --mode remote`). Re-seeded once on Databricks Runtime 15.4 LTS (was 16.4) — see the Databricks mirror row below; counts are a clean round number here because the re-seed reset a small incremental-batch demo on top of the original count. |
| Azure Cosmos DB (serverless, private-endpoint-only, continuous backup) | Deployed |
| Cosmos synthetic data (sessions, devices, fraud alerts) | Executed and **verified**: 1,201 digitalSessions / 400 devices / 151 fraudAlerts live, correct partition keys, deliberate schema variation confirmed present (`validation/validate_cosmos.py`) |
| Fabric workspace, `silver_lh`/`gold_lh`/`gold_wh` items | Deployed |
| Fabric notebooks (7) and `pl_multisource_medallion` pipeline | Deployed to the workspace |
| Mirrored Azure Databricks catalog (Bronze, Databricks side) | **Deployed, executed, and verified**: connection `fmv2poc-databricks-catalog-mirror-connection` switched from a Key-type (PAT) credential to OAuth2 (human interactive sign-in), which fixed the previously-confirmed read failure. A second, independent blocker surfaced only once reads actually worked: the seed job's default "always pick the newest Databricks Runtime" logic had drifted onto DBR 16.4, which auto-upgrades new Unity Catalog managed tables to the `catalogManaged` (coordinated-commits) table feature — a protocol Fabric's mirroring/shortcut read path can't resolve regardless of Spark/Delta version, confirmed live. Fixed by pinning the seed job to DBR 15.4 LTS (confirmed empirically not to have this auto-upgrade) and re-seeding. Exact row-count match to source confirmed via the mirror's own SQL analytics endpoint: 750,000 / 750,000 / 1,500. See [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md) for the full trace. |
| Mirrored Azure Cosmos DB database (Bronze, Cosmos side) | **Deployed, executed, and verified**: connection `fmv2poc-cosmos-vnet-connection` created (Virtual network / `fmv2poc-cosmos-vnet-gateway` / Azure Cosmos DB v2 / OAuth2, human interactive sign-in). `MirroredDatabase` item `fmv2poc_cosmos_multisource_mirror` created, `startMirroring` succeeded, status `Running`. Queried the mirror's own SQL analytics endpoint directly: exact row-count match to source — 1,201 digitalSessions / 400 devices / 151 fraudAlerts. See [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md) for the full trace. |
| Pipeline execution (Silver/Gold/Warehouse/reconciliation) | **Executed and verified**: `pl_multisource_medallion` ran end to end (`ValidateDatabricks → ValidateCosmos → SilverTransform → GoldBuild → WarehousePublish → Reconciliation → CompletionLogging`, ~17 minutes), status `Completed`. All 20 reconciliation checks — Databricks source-vs-Silver, Cosmos source-vs-Silver, every Silver-vs-Gold table, every quarantine table — report `PASS` (queried `gold_lh.reconciliation_results` directly by the actual run id, not assumed from pipeline status alone). Getting here surfaced and fixed a chain of real bugs along the way — a Fabric platform default-lakehouse constraint on cross-lakehouse `notebookutils.notebook.run` calls, several stale assumed Cosmos document field names predating the real generator, a missing `CustomerID` column in `FactTransactions`, and a wrong Gold table name in the reconciliation check itself — see the notebooks' own code comments and this session's commit history for the specifics. |
| OneLake Catalog (item descriptions, domain assignment) | **Verified**: all 13 items described, workspace assigned to the `Retail Banking Analytics` domain, confirmed discoverable via Catalog Search |
| OneLake security (column-level constraints) | **Applied and verified stored**: `DimCustomer.CustomerID` and `DimDevice.DeviceFingerprint` restricted on the `DefaultReader` role. Fabric's column-level security only supports `columnEffect: "Permit"` — a `"Deny"` constraint (the original design) is rejected outright (`PolicyValidationError`, confirmed live); redesigned as an allow-list naming every other column instead. Enforcement against a genuinely lesser-privileged principal still not independently verified (see docs/known-limitations.md). |
| Warehouse governance (masking, RLS) | **Executed**: `00_refresh_gold_serving.sql` → `10_apply_security.sql` → `20_validate_security.sql` all ran successfully against `gold_wh` against real Gold data (no longer blocked on empty tables). |
| Fabric Git integration | **Deployed and verified**: connection `fabric-medallion-multisourcev2-poc-git-sync` created with a human-provided fine-grained GitHub PAT (`gh auth token`'s OAuth token was rejected — see [docs/known-limitations.md](docs/known-limitations.md)), workspace connected to `regshih/fabric-medallion-multisourcev2-poc` (`fabric_git` state `ConnectedAndInitialized`). Fabric pushed a real commit (`d746e60`, "Sync via infra/setup_git_integration.py") serializing all 12 workspace items into `fabric_git/` — confirmed live by pulling it locally. |
| Public repository security review | Not yet performed — repo is **private** until it passes |

This table is maintained honestly as work progresses — see the git history for how each row
was resolved, and [docs/known-limitations.md](docs/known-limitations.md) for anything that
stays a real, permanent limitation rather than a temporary in-progress gap.

## Repository map

| Path | Purpose |
|---|---|
| `generators/` | Deterministic synthetic source-data generation (Databricks + Cosmos) |
| `databricks/` | Unity Catalog seed + incremental Delta jobs (run via `infra/databricks/run_job.py`) |
| `cosmos/` | Passwordless (Entra ID only) Cosmos DB loaders |
| `infra/databricks/` | Databricks workspace/Unity Catalog provisioning and job automation |
| `infra/cosmos/` | Cosmos DB Bicep, private-networking setup, VM-based loader runner |
| `infra/fabric/` | Fabric REST API auth, LRO polling, workspace/item provisioning, pipeline templating |
| `infra/governance/` | OneLake Catalog descriptions/search, domain assignment, OneLake security |
| `notebooks/` | Source validation, Silver transform, Gold build, Warehouse publish, reconciliation, shared pipeline logger, consumption demo |
| `pipelines/` | `pl_multisource_medallion` — placeholder-templated pipeline definition |
| `warehouse/` | Gold serving views, masking, RLS, and validation SQL (fixed execution order: 00 → 10 → 20) |
| `validation/` | Live validators for both sources |
| `fabric_git/` | Dedicated target for Fabric-managed Git artifacts |
| `docs/` | Architecture decisions, per-source integration findings, governance, limitations |

## Quick start

```bash
python -m venv .venv && source .venv/Scripts/activate   # or .venv\Scripts\activate on cmd
pip install -r requirements.txt
cp .env.example .env   # fill in identifiers only — never secrets
az login
```

See [docs/deployment.md](docs/deployment.md) for the full ordered deployment sequence and
[docs/runbook.md](docs/runbook.md) for day-2 operations, incremental-change demonstration,
and cleanup/cost guidance.

## Human-only steps

- ~~Databricks mirror connection credential~~ and ~~Cosmos DB v2 connection~~ — **done**.
  Both required a human interactive sign-in (Fabric portal, OAuth2) and are now verified
  live with exact source row-count matches — see the evidence table above and
  [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md) /
  [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md) for the full traces.
- Confirm the `fabricmsv2poc915d` capacity (F2, West US 3, resource group
  `rg-fabric-medallion-multisourcev2-poc-westus3`) is Active before running provisioning or
  deploy scripts — Fabric trial/dev capacities can auto-pause between sessions.
- Build the Power BI Direct Lake report (out of scope for this POC's automation, same as the
  sibling reference POCs).

## Official documentation

- [Fabric mirroring overview](https://learn.microsoft.com/en-us/fabric/mirroring/overview)
- [Mirroring Azure Databricks (Unity Catalog)](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks)
- [Mirroring Azure Cosmos DB](https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db)
- [OneLake security](https://learn.microsoft.com/en-us/fabric/onelake/security/fabric-onelake-security)
- [OneLake catalog](https://learn.microsoft.com/en-us/fabric/governance/onelake-catalog-overview)

## Documentation

- [Architecture and source lineage](ARCHITECTURE.md)
- [Databricks-to-Fabric integration findings](docs/databricks-fabric-integration.md)
- [Cosmos DB mirroring and partition design](docs/cosmos-fabric-mirroring.md)
- [Deployment guide](docs/deployment.md)
- [Operations runbook](docs/runbook.md)
- [Known limitations](docs/known-limitations.md)
