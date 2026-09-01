# Operations runbook

## Capacity

The dedicated Fabric capacity `fabricmsv2poc915d` (F2, West US 3, resource group
`rg-fabric-medallion-multisourcev2-poc-westus3`) may auto-pause between sessions on
trial/dev-tier usage. Check and resume before running any Fabric script:

```bash
az resource show --resource-group rg-fabric-medallion-multisourcev2-poc-westus3 \
  --name fabricmsv2poc915d --resource-type "Microsoft.Fabric/capacities" \
  --query "properties.state" -o tsv
az resource invoke-action --resource-group rg-fabric-medallion-multisourcev2-poc-westus3 \
  --name fabricmsv2poc915d --resource-type "Microsoft.Fabric/capacities" --action resume
```

## Incremental-change demonstration

Both sources have a deterministic incremental batch, separate from the initial seed:

```bash
python -m infra.databricks.run_job \
    --local-notebook databricks/02_apply_incremental_batch.py \
    --workspace-path /Shared/fabric-medallion-multisourcev2-poc/02_apply_incremental_batch \
    --param catalog=<catalog> --param schema=banking --param batch_id=<unique-id>

.\infra\cosmos\run_loader.ps1 -ResourceGroup <rg> -AccountName <account> -Run incremental
```

Databricks incremental batch: new transactions, a changed risk score, a new merchant.
Cosmos incremental batch: a new session, a device flipping `trusted: true -> false` with a
new risk signal, a new fraud alert, and one existing alert's `status` changing.

Re-run the pipeline (`infra.deploy_pipeline ... --run`) after loading an incremental batch
and confirm `gold_lh.reconciliation_results` and `gold_lh.control_pipeline_run_log` reflect
the change — this is the "observe the second cycle" step from the task brief. Do not claim
any specific propagation-latency SLA; measure and report what's actually observed.

## Reconciliation

`notebooks/nb_reconciliation.py` writes one row per check to `gold_lh.reconciliation_results`
— source-vs-Silver counts for both sources (documenting explicitly that the Databricks
comparison validates two access paths to the same Delta data, not true replication, while the
Cosmos comparison validates a genuinely replicated target), Silver-vs-Gold counts with grain
mismatches called out (never silently treated as a false failure), and quarantine counts
(zero is a real PASS signal). Query it directly:

```sql
SELECT * FROM reconciliation_results WHERE status = 'FAIL';
```

## Cost and cleanup

- **Databricks**: jobs run on ephemeral, auto-terminating job clusters (`num_workers: 0`,
  single-node) — nothing stays running between job runs. The Premium workspace itself and its
  managed storage are the standing cost; delete the resource group to remove them entirely.
- **Cosmos DB**: serverless (pay-per-request, no standing compute cost) with 7-day continuous
  backup (free tier). The private endpoint, VNet, and reserved `snet-fabric` subnet have no
  meaningful standing cost on their own.
- **Fabric capacity**: F2 is the smallest paid SKU. Pause it when not actively demoing:
  ```bash
  az resource invoke-action --resource-group rg-fabric-medallion-multisourcev2-poc-westus3 \
    --name fabricmsv2poc915d --resource-type "Microsoft.Fabric/capacities" --action suspend
  ```
- **Full teardown**: `az group delete --name rg-fabric-medallion-multisourcev2-poc-westus3`
  removes every Azure resource this POC created. The Fabric workspace itself is deleted
  separately via the Fabric API/portal (it is not an ARM resource).

## Troubleshooting

- **Windows/Git Bash**: set `MSYS_NO_PATHCONV=1` before any script call with a leading-`/`
  argument, or use PowerShell instead — see [docs/deployment.md](deployment.md).
- **Bare 500 on `git/connect`**: check the Fabric capacity isn't Paused before assuming a
  real server error — this was a live-verified false lead in the sibling banking POC.
- **`Invalid object name` right after a notebook write**: the Lakehouse SQL analytics
  endpoint lags the Delta log; `infra/verify_row_counts.py` force-refreshes it before
  querying — reuse that pattern for any ad hoc query.
