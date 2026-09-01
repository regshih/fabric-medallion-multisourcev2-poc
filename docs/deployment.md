# Deployment guide

Ordered sequence actually used to build this POC. Every script is idempotent
(check-if-exists-then-create) and safe to re-run. Real Azure/Fabric resources are created —
this is not a dry run.

## 0. Prerequisites

```bash
python -m venv .venv && source .venv/Scripts/activate
pip install -r requirements.txt
cp .env.example .env   # fill in identifiers — never secrets
az login
```

`.env` needs, at minimum: `AZURE_SUBSCRIPTION_ID`, `RESOURCE_GROUP`, `FABRIC_CAPACITY_NAME`,
`FABRIC_WORKSPACE_NAME`. The Databricks/Cosmos sections fill in as those resources are
created in steps 1-2 below (or copy the real values already in this repo's history via
`.env.example`, since this POC's actual identifiers are non-secret).

## 1. Databricks

```bash
python -m infra.databricks.provision_workspace   # Premium workspace
python -m infra.databricks.setup_unity_catalog   # catalog/schema/volume + external-data-access check
python generators/generate_databricks_data.py --rows 750000 --customers 50000 --merchants 1500
python -m infra.databricks.upload_to_workspace --workspace-host <host> --workspace-resource-id <id> \
    --catalog <catalog> --schema banking --volume landing
python -m infra.databricks.run_job \
    --workspace-host <host> --workspace-resource-id <id> \
    --local-notebook databricks/01_seed_delta_tables.py \
    --workspace-path /Shared/fabric-medallion-multisourcev2-poc/01_seed_delta_tables \
    --param catalog=<catalog> --param schema=banking --param volume=landing
python validation/validate_databricks.py --mode remote
```

**Windows/Git Bash note**: set `MSYS_NO_PATHCONV=1` before running any script that takes a
leading-`/` argument (`--workspace-resource-id /subscriptions/...`, `--workspace-path
/Shared/...`) — Git Bash's MSYS layer otherwise silently rewrites these into bogus Windows
paths, which Databricks then rejects as "Invalid resource ID." This is a shell quirk, not a
script bug. PowerShell doesn't have this problem.

Verified live: 750,025 transactions, 750,025 risk scores, 1,501 merchants, all quality
checks green.

## 2. Cosmos DB

```powershell
.\infra\cosmos\setup.ps1                 # account, VNet, private endpoint, managed identity
python generators\generate_cosmos_data.py --batch all
.\infra\cosmos\run_loader.ps1 -ResourceGroup <rg> -AccountName <account> -Run all
```

The account is private-endpoint-only by tenant policy — `run_loader.ps1` runs the loaders
from a short-lived VM placed inside the account's own VNet (deleted immediately after use),
since an ordinary dev machine can't reach the endpoint directly. See
[docs/cosmos-fabric-mirroring.md](../docs/cosmos-fabric-mirroring.md) for why.

Verified live: 1,201 digitalSessions, 400 devices, 151 fraudAlerts, correct partition keys.

## 3. Fabric workspace + items

```bash
python -m infra.fabric.provision
```

Creates the workspace (assigned to the dedicated capacity) and `silver_lh`/`gold_lh`/`gold_wh`
— deliberately no `bronze_lh`, see [ARCHITECTURE.md](../ARCHITECTURE.md) "Sources and Bronze".

After this runs for the first time, the notebook files under `notebooks/` need their
`WORKSPACE_ID`/`SILVER_LH_ID` placeholder constants (and `.platform` METADATA blocks) filled
in with the real IDs printed by this command — a from-scratch rebuild mints new IDs every
time, unlike a single-workspace, never-rebuilt POC.

## 4. Fabric source mirrors

```bash
python -m infra.fabric.mirror_databricks
python -m infra.fabric.mirror_cosmos   # see docs/cosmos-fabric-mirroring.md for the
                                        # virtual-network-gateway prerequisite
```

## 5. Deploy notebooks and pipeline

```bash
for nb in notebooks/nb_*.py; do python -m infra.deploy_notebook "$nb"; done
python -m infra.deploy_pipeline pipelines/pl_multisource_medallion.json
```

## 6. Governance

```bash
python -m infra.governance.catalog_setup --try-domain
python -m infra.governance.catalog_search --search "cross-source"
python -m infra.governance.onelake_security --apply
```

## 7. Run the pipeline

```bash
python -m infra.deploy_pipeline pipelines/pl_multisource_medallion.json --run
```

## 8. Warehouse governance SQL (fixed order — see warehouse/README.md)

```bash
python -m infra.run_sql_file gold_wh warehouse/00_refresh_gold_serving.sql
python -m infra.run_sql_file gold_wh warehouse/10_apply_security.sql
python -m infra.run_sql_file gold_wh warehouse/20_validate_security.sql
```

## 9. Git integration (human-only PAT step first — see README)

```bash
python -m infra.setup_git_integration
```
