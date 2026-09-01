# Architecture

## Sources and Bronze

Two source systems, mirrored into Fabric with genuinely different mechanics — this asymmetry is the point, not an inconsistency:

- **Azure Databricks (Unity Catalog)** — Fabric's mirrored Azure Databricks catalog integration syncs Unity Catalog **metadata** and creates OneLake **shortcuts** to the underlying Delta tables. It does **not** copy the Delta data itself; reads go straight through the shortcut to the original ADLS-backed Delta files. Zero-copy. See [docs/databricks-fabric-integration.md](docs/databricks-fabric-integration.md).
- **Azure Cosmos DB for NoSQL** — Fabric's mirrored Cosmos DB integration continuously and incrementally replicates documents into **physical Delta tables** in OneLake. Nested objects/arrays land as JSON string columns. See [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md).

Both are treated as **source-aligned Bronze access** — there is deliberately **no physical `bronze_lh` Lakehouse**. Building one would either re-copy data Databricks already exposes zero-copy (defeating the point of shortcuts) or duplicate what Cosmos mirroring already physically replicates (pure redundancy, no benefit). Silver reads directly from the two mirrored source items.

## Business-key contract (fixed — do not deviate)

Both sources are generated independently but must join. Every generator/loader uses these exact formats:

| Key | Format | Example |
|---|---|---|
| `CustomerID` | `CUST-` + 6 digits | `CUST-000042` |
| `AccountID` | `ACCT` + 9 digits | `ACCT000000123` |
| `DeviceID` | `DEVICE-` + 6 digits | `DEVICE-000007` |
| `MerchantID` | `MER` + 6 digits | `MER000015` |
| `TransactionID` | `TXN-` + 9 digits | `TXN-000000042` |

Databricks Unity Catalog: catalog `multisourcev2poc`, schema `banking`, tables `transactions`, `transaction_risk_scores`, `merchants`.

Cosmos DB: database `multisource`, containers `digitalSessions`, `devices`, `fraudAlerts`, all partitioned on `/customerId`.

## Silver layer (`silver_lh`)

Reads directly from the Databricks shortcut tables and the Cosmos mirror's Delta tables. Per-table quality rules quarantine (never silently drop) invalid rows into a paired `quarantine_*` table — every quarantine table is always written, even when empty, so its row count is a real signal:

| Silver table | Source | Key quality rule |
|---|---|---|
| `transactions` | Databricks | `TransactionID` matches `TXN-\d{9}`, non-null `CustomerID`/`TransactionTimestamp`, `Amount >= 0` |
| `transaction_risk` | Databricks | `RiskScore` in [0,100]; `TransactionID` must exist in the validated `transactions` set (cross-table check) |
| `merchants` | Databricks | non-null `MerchantID`/`MerchantCategory` |
| `sessions` | Cosmos | non-null `customerId`/`sessionId`; nested `device`/`geo`/`authentication` flattened to columns; `activities[]` kept as a JSON string |
| `devices` | Cosmos | non-null `deviceId`/`customerId`; `riskSignals[]`/`geoHistory[]` kept as JSON strings |
| `fraud_alerts` | Cosmos | non-null `alertId`/`customerId`; `severity` in `{Low,Medium,High,Critical}` |

Use `F.coalesce(condition, F.lit(False))` for every quarantine predicate — a NULL condition must fail closed into quarantine, not silently vanish from both the valid and invalid sets. Every date/timestamp column gets an explicit `.cast("date")` or `.cast("timestamp")` here (not deferred to Gold) — `timestamp_ntz` columns are otherwise invisible on the SQL analytics endpoint (see the sibling banking POC's CLAUDE.md for the live-verified root cause).

## Gold layer (`gold_lh`)

Star schema, surrogate keys via `xxhash64(business_key)` (stable, no SCD requirement in this POC — every dimension is a plain current-state hash-keyed dimension, not SCD2):

- `DimCustomer`, `DimAccount`, `DimMerchant`, `DimDevice`, `DimDate`
- `FactTransactions`, `FactDigitalSessions`, `FactFraudAlerts`
- `AggCustomerRiskProfile` — the cross-source model, see below
- `control_pipeline_run_log`, `reconciliation_results` (audit/validation, live in `gold_lh`, no separate control database)

### `AggCustomerRiskProfile` — the cross-source analytical outcome

One row per customer seen in *any* of the four fact sources (union, not inner join — a customer with only a fraud alert and no transactions still gets a row). Grain: `CustomerID`.

**Independent source-relative 30-day watermarks**, not one global as-of date — `txn_as_of = max(TransactionTimestamp date)` from Databricks-derived facts, `session_as_of = max(LoginTimestamp date)` from Cosmos-derived facts, each windowed independently over its own trailing 30 days. This is intentional: the two sources have asynchronous clocks by design (they're generated/loaded independently), and a single shared as-of date would silently zero out one side's aggregates whenever the two batches land at different times.

Columns: `CustomerID`, `TransactionCount30D`, `TotalTransactionAmount30D`, `AverageTransactionRiskScore`, `HighRiskTransactionCount`, `FraudAlertCount`, `FailedLoginCount`, `DistinctDeviceCount`, `UntrustedDeviceCount`, `GeographicAnomalyCount`, `CustomerRiskScore`, `CustomerRiskBand`.

Score — a capped composite, each factor capped before summing so no single signal dominates:

```python
CustomerRiskScore = round(min(100.0,
    coalesce(AverageTransactionRiskScore, 0) * 0.45 +
    min(FraudAlertCount * 12.0, 24.0) +
    min(FailedLoginCount * 2.0, 12.0) +
    min(UntrustedDeviceCount * 5.0, 10.0) +
    min(GeographicAnomalyCount * 3.0, 9.0)
), 2)
CustomerRiskBand = "High" if >= 80 else "Medium" if >= 45 else "Low"
```

This is an **explainable synthetic heuristic**, not a trained or calibrated model, and is not suitable for real risk decisions — every doc referencing it must carry that caveat.

## Warehouse (`gold_wh`)

Governed SQL serving layer. Native Dynamic Data Masking and `SESSION_CONTEXT`-gated row-level security can only target physical tables the Warehouse itself owns — never a cross-database view, and Lakehouses have no `ALTER TABLE` at all (live-verified constraint, see the sibling banking POC's CLAUDE.md "Warehouse governance constraints"). Objects carrying sensitive columns (`DimCustomer`, `DimDevice`, `AggCustomerRiskProfile`) are materialized as physical `_base_*` CTAS copies specifically so governance has something to attach to; everything else stays a zero-copy cross-database view. Execution order is fixed and load-bearing: refresh (drop+recreate `_base_*`) → apply masking → apply RLS, every run — masking/RLS are wiped whenever the base table is recreated.

## Governance

- OneLake Catalog: description on every item (highest-leverage, low-effort discoverability lever); domain assignment attempted (`Retail Banking Analytics` domain, reused if it already exists from the sibling banking POC) but requires an Entra directory role this session may not carry — documented as attempted/blocked rather than assumed.
- OneLake security: column-level constraints on `gold_lh.DimDevice` (device fingerprint) and `gold_lh.DimCustomer` (customer identifiers), applied via the ETag/`If-Match`-safe full-replace pattern (never a blind overwrite of other roles).
- Both governance layers cannot be proven enforced against this session's own identity — Admin/Member/Contributor workspace roles bypass both DDM and OneLake Read-based restrictions. Documented as configured-but-unverified, consistent with the sibling POC's own honest accounting of the same gap.

## Pipeline (`pl_multisource_medallion`)

`Validate Databricks` + `Validate Cosmos` (parallel) → `Silver Transform` → `Gold Build` → `Warehouse Publish` → `Reconciliation` → `Completion Logging`, with a paired `LogXFailure` activity on every stage (`dependsOn` on `Failed`) writing to `control_pipeline_run_log` before re-raising — unlike the sibling banking POC (which has no failure-path logging at all), this is a real requirement here and is implemented, not deferred.
