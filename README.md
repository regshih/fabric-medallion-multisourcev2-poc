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
| Databricks synthetic data (transactions, risk scores, merchants) | Executed and **verified**: 750,025 transactions / 750,025 risk scores / 1,501 merchants live in Unity Catalog, all quality checks green (`validation/validate_databricks.py --mode remote`) |
| Azure Cosmos DB (serverless, private-endpoint-only, continuous backup) | Deployed |
| Cosmos synthetic data (sessions, devices, fraud alerts) | Executed and **verified**: 1,201 digitalSessions / 400 devices / 151 fraudAlerts live, correct partition keys, deliberate schema variation confirmed present (`validation/validate_cosmos.py`) |
| Fabric workspace, `silver_lh`/`gold_lh`/`gold_wh` items | Deployed |
| Fabric notebooks (7) and `pl_multisource_medallion` pipeline | Deployed to the workspace |
| Mirrored Azure Databricks catalog (Bronze, Databricks side) | Item deployed and healthy (`fmv2poc_databricks_banking_mirror`, `mirrorStatus: "Mirrored"`). **Blocked, human-only** on actually reading it: the connection uses a Databricks PAT (`credentialType: "Key"`), which Fabric rejects for OneLake-shortcut-resolution-based reads — confirmed across 3 independent paths (direct mirror read, a Lakehouse shortcut, and the mirror's own SQL analytics endpoint, which 400s on `refreshMetadata`). Needs a human to redo the connection with OAuth2 or a Service Principal credential — both require real interactive auth this session couldn't complete. See [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md) "Fabric mirror: deployed, but not consumable." |
| Mirrored Azure Cosmos DB database (Bronze, Cosmos side) | Networking fully done (VNet gateway + Network ACL Bypass, all 8 steps of Microsoft's private-network guide except one, all verified live). **Blocked, human-only** on step 7: Fabric's Cosmos DB v2 connection over a virtual-network gateway only supports interactive OAuth 2.0 sign-in in the Fabric portal — confirmed via a hard `400 OAuth2CredentialsNotSupportedForConnection` REST rejection, not inferred from docs. See [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md) for the exact portal steps; re-run `python -m infra.fabric.mirror_cosmos` afterward to finish unattended. |
| Pipeline execution (Silver/Gold/Warehouse/reconciliation) | Not yet executed — depends on both mirrors being readable |
| OneLake Catalog (item descriptions, domain assignment) | **Verified**: all 13 items described, workspace assigned to the `Retail Banking Analytics` domain, confirmed discoverable via Catalog Search |
| OneLake security (column-level constraints) | Implemented, not yet applied |
| Warehouse governance (masking, RLS) | Implemented, not yet executed (depends on Gold data existing) |
| Fabric Git integration | **Blocked, human-only**: Fabric's GitHub connector requires a real classic/fine-grained GitHub PAT — `gh auth token`'s OAuth token is explicitly rejected ("Unexpected PAT detected"). See "Human-only steps" below. |
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

- **Databricks mirror connection credential**: the mirror item is healthy but unreadable
  (Key-type credential unsupported for OneLake-shortcut reads — confirmed live, see
  [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md) "Fabric
  mirror: deployed, but not consumable"). Either (a) in the Fabric portal, edit the
  `fmv2poc-databricks-catalog-mirror-connection` connection's credentials to Organizational
  account and sign in interactively, or (b) run `az login` interactively (this session's
  Conditional Access policy blocked headless Microsoft Graph writes needed to create a
  Service Principal), then `az ad sp create-for-rbac`, grant it `EXTERNAL USE SCHEMA` via
  `python -m infra.databricks.grant_external_use_schema --principal <app-id> ...`, and update
  the connection to a `ServicePrincipal` credential. No notebook code change needed either
  way — `src_databricks_*` Lakehouse shortcuts already exist and point at the right place.
- **GitHub PAT for Fabric Git integration**: create a fine-grained PAT scoped to just this
  repo (Contents: Read and write, short expiration) at
  https://github.com/settings/personal-access-tokens/new, set it as `GITHUB_PAT` in `.env`
  (never commit it), then run `python -m infra.setup_git_integration`. `gh auth token`'s
  OAuth token does not work here — confirmed live, Fabric's connector explicitly rejects it.
- Confirm the `fabricmsv2poc915d` capacity (F2, West US 3, resource group
  `rg-fabric-medallion-multisourcev2-poc-westus3`) is Active before running provisioning or
  deploy scripts — Fabric trial/dev capacities can auto-pause between sessions.
- **Cosmos DB mirroring's Azure Cosmos DB v2 connection**: confirmed portal-only (Fabric's
  REST API hard-rejects OAuth2 credentials for a `VirtualNetworkGateway` connection —
  `400 OAuth2CredentialsNotSupportedForConnection`). All networking prerequisites (VNet
  gateway, NAT gateway, Network ACL Bypass) are already done and verified. In the Fabric
  portal: **Settings → Manage connections and gateways → Connections → + New → connectivity
  type "Virtual network" → gateway `fmv2poc-cosmos-vnet-gateway` → connection type "Azure
  Cosmos DB v2" → endpoint `https://cosmosfabricmsv2915d.documents.azure.com:443/` →
  Authentication method "OAuth 2.0" → sign in.** Then run
  `python -m infra.fabric.mirror_cosmos` to finish (mirrored database item + start
  mirroring) unattended. See [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md)
  for the full trace.
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
