# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b598e5c-9cff-497e-b43b-7a5d1b91c0df",
# META       "default_lakehouse_name": "gold_lh",
# META       "default_lakehouse_workspace_id": "7e206237-aef1-4932-9f94-1f6ae343407a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_id = "manual"
run_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deliberately NOT the v1 pattern (a notebook writing a "contract table" for
# a pipeline Script activity to consume — that indirection was never
# verified to work and isn't reused here). The actual Warehouse SQL
# execution (refresh _base_* tables, apply masking, apply RLS) happens by
# running warehouse/00_refresh_gold_serving.sql -> 10_apply_security.sql ->
# 20_validate_security.sql directly against gold_wh's SQL endpoint via
# infra/run_sql_file.py (pyodbc) — either a manual runbook step or a
# pipeline Script/Copy activity the pipeline engineer wires up separately.
#
# This notebook's only real job: confirm Gold is actually ready to publish
# (every table non-empty) and log a WarehousePublish stage row. Fails
# loudly if anything is empty — a downstream SQL refresh from an empty/
# partial Gold would be a worse failure mode than stopping here.
from datetime import datetime, timezone

from pyspark.sql.utils import AnalysisException

_start_ts = datetime.now(timezone.utc)
if not run_date:
    run_date = _start_ts.strftime("%Y-%m-%d")

GOLD_TABLES = [
    "dimcustomer", "dimaccount", "dimmerchant", "dimdevice", "dimdate",
    "facttransactions", "factdigitalsessions", "factfraudalerts",
    "aggcustomerriskprofile",
]

counts: dict[str, int] = {}
for table in GOLD_TABLES:
    try:
        n = spark.table(table).count()
    except AnalysisException as exc:
        raise RuntimeError(f"gold_lh.{table} is not queryable — has nb_gold_build.py run yet? {exc}") from exc
    if n == 0:
        raise RuntimeError(f"gold_lh.{table} is queryable but empty — refusing to signal Warehouse publish readiness")
    counts[table] = n

rows_read = sum(counts.values())
for table, n in counts.items():
    print(f"{table}: {n}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils  # noqa: E402

notebookutils.notebook.run(
    "pipeline_log",
    120,
    {
        "run_id": run_id,
        "stage": "warehouse_publish",
        "status": "Succeeded",
        "start_ts": _start_ts.isoformat(),
        "end_ts": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_written": 0,
        "error_message": "",
        "run_date": run_date,
    },
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
