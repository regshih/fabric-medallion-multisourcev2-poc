# pl_multisource_medallion

`pl_multisource_medallion.json` is the checked-in, placeholder-templated
Data Factory-shaped pipeline definition. `infra/deploy_pipeline.py` resolves
`{{WORKSPACE_ID}}` and `{{NOTEBOOK_ID:<name>}}` against the live workspace at
deploy time (see `infra/fabric/definitions.py`); nothing in this file is a
real GUID.

## Shape

`properties.parameters.run_date` (string, default `""`) is passed straight
through to every notebook activity as `@pipeline().parameters.run_date` —
no `@if(...)`/`formatDateTime` wrapper in the pipeline itself. Per the
notebook contract, each notebook computes today's UTC date internally when
it receives an empty string, so the pipeline does no date math. (This
differs from the sibling banking POC's orchestrator, which does the
`@if(equals(...))` substitution in the pipeline; that's a deliberate choice
here, not an oversight.)

13 activities total: 6 substantive stages + 6 paired `Log<Stage>Failure`
activities + 1 `CompletionLogging` activity.

| Activity | Type | Depends on | Notebook | Key params |
|---|---|---|---|---|
| `ValidateDatabricks` | TridentNotebook | — | `source_validation` | `source="databricks"` |
| `LogValidateDatabricksFailure` | TridentNotebook | `ValidateDatabricks` (Failed) | `pipeline_log` | `stage="ValidateDatabricks"`, `status="Failed"` |
| `ValidateCosmos` | TridentNotebook | — | `source_validation` | `source="cosmos"` |
| `LogValidateCosmosFailure` | TridentNotebook | `ValidateCosmos` (Failed) | `pipeline_log` | `stage="ValidateCosmos"`, `status="Failed"` |
| `SilverTransform` | TridentNotebook | `ValidateDatabricks`, `ValidateCosmos` (both Succeeded) | `silver_transform` | — |
| `LogSilverTransformFailure` | TridentNotebook | `SilverTransform` (Failed) | `pipeline_log` | `stage="SilverTransform"`, `status="Failed"` |
| `GoldBuild` | TridentNotebook | `SilverTransform` (Succeeded) | `gold_build` | — |
| `LogGoldBuildFailure` | TridentNotebook | `GoldBuild` (Failed) | `pipeline_log` | `stage="GoldBuild"`, `status="Failed"` |
| `WarehousePublish` | TridentNotebook | `GoldBuild` (Succeeded) | `warehouse_publish` | — |
| `LogWarehousePublishFailure` | TridentNotebook | `WarehousePublish` (Failed) | `pipeline_log` | `stage="WarehousePublish"`, `status="Failed"` |
| `Reconciliation` | TridentNotebook | `WarehousePublish` (Succeeded) | `reconciliation` | — |
| `LogReconciliationFailure` | TridentNotebook | `Reconciliation` (Failed) | `pipeline_log` | `stage="Reconciliation"`, `status="Failed"` |
| `CompletionLogging` | TridentNotebook | `Reconciliation` (Succeeded) | `pipeline_log` | `stage="PipelineComplete"`, `status="Succeeded"`, `error_message=""` |

Every notebook activity's `run_id` resolves to `@pipeline().RunId`.

## Judgment calls flagged for review

- **`@activity('<Stage>').Error.message` — UNVERIFIED SYNTAX.** This is the
  best-supported Data Factory/Fabric expression for surfacing a failed
  activity's error text, but it has not been confirmed against a live run
  in this repo. Needs checking against a real failed pipeline run before
  being relied on for anything beyond a demo.
- `start_ts` / `end_ts` on every `Log*Failure` and `CompletionLogging`
  activity both use `@utcNow()` (evaluated at that activity's own run time,
  so they'll differ by a few ms, not a real duration). The failed stage's
  actual start/end aren't easily reachable as pipeline expressions without
  System Variables the notebook contract doesn't expose, so this is a
  placeholder, not a measured duration.
- `rows_read` / `rows_written` on every `pipeline_log` invocation from the
  pipeline (all `Log*Failure` activities plus `CompletionLogging`) are
  hardcoded `"0"` (string type). The task only specifies real row counts
  for each stage's own internal self-logging on success; the orchestration
  layer has no row counts to report for a failure or for the top-level
  completion marker, so `0` is a placeholder, not a measured value.
- `rows_read` / `rows_written` parameter **type** is `string` (not
  int) for consistency with every other wrapped parameter in this file —
  the notebook contract left this open ("your call").
- `policy` block (`timeout: 0.12:00:00`, `retry: 0`,
  `retryIntervalInSeconds: 30`) is copied verbatim from the sibling
  banking POC's `pl_medallion_orchestrator.json` for consistency across
  the two repos' pipelines — not independently re-derived.
