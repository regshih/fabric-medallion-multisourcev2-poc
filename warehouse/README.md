# warehouse/

Fixed execution order, every run: **`00_refresh_gold_serving.sql` →
`10_apply_security.sql` → `20_validate_security.sql`**. Never run `10` or
`20` standalone against a table `00` didn't just (re)create.

```
python infra/run_sql_file.py gold_wh warehouse/00_refresh_gold_serving.sql
python infra/run_sql_file.py gold_wh warehouse/10_apply_security.sql
python infra/run_sql_file.py gold_wh warehouse/20_validate_security.sql
```

## Why the order is load-bearing

`00_refresh_gold_serving.sql` drops and recreates the three governed
`_base_*` physical tables (`_base_DimCustomer`, `_base_DimDevice`,
`_base_AggCustomerRiskProfile`) via CTAS from `gold_lh` every run, so Gold
changes actually land in `gold_wh`. Dynamic Data Masking and row-level
security are attached **per physical table object, not per name** — so
every `00` run silently wipes whatever masking/RLS the *previous*
incarnation of those tables carried. `10_apply_security.sql` must therefore
run immediately after `00`, every time, to reapply masking + RLS to the
fresh tables — and `20_validate_security.sql` only means anything once `10`
has actually run against the current tables.

This mirrors the sibling `fabric-medallion-banking-poc`'s
live-verified finding (see its CLAUDE.md "Warehouse governance
constraints"): native DDM/RLS can only target a physical table the
Warehouse itself owns (a Lakehouse has no `ALTER TABLE` at all, and a
cross-database view isn't a valid target either), which is why
`DimCustomer`, `DimDevice`, and `AggCustomerRiskProfile` — the three
objects carrying sensitive columns per ARCHITECTURE.md — are materialized
as physical `_base_*` copies while everything else stays a zero-copy
cross-database view straight over `gold_lh`.

## A note on `20_validate_security.sql`

`infra/run_sql_file.py` executes each `GO`-separated batch but does not
fetch or print query results — it's built for applying DDL, not for
producing a report. Running `20_validate_security.sql` through it will
apply the `sp_set_session_context` calls correctly but won't show you the
`SELECT` output. Use a Fabric SQL query editor (portal) or SSMS against
`gold_wh` to actually eyeball the verification queries in this file.
