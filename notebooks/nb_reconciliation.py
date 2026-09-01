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

# Writes gold_lh.reconciliation_results, one row per check. Four groups of
# checks, per ARCHITECTURE.md / this notebook's task description:
#   1. Databricks source-vs-Silver — validates two access paths to the SAME
#      underlying Delta data (the mirror's shortcut vs. Silver's read of
#      that same shortcut), NOT real replication. fabric_count reconstructs
#      the mirror's total row count as (Silver valid + Silver quarantine)
#      so a match proves Silver classified every row without losing or
#      duplicating any, not just that two reads of the same table agree.
#   2. Cosmos source-vs-Silver — this one DOES validate a genuinely
#      replicated target snapshot (Cosmos mirroring physically copies
#      documents into Delta), same reconstruction logic.
#   3. Silver-vs-Gold — exact match expected for merchants/DimMerchant,
#      devices/DimDevice, transactions/FactTransactions (LEFT-joined to
#      risk in nb_gold_build.py specifically so this stays a clean 1:1),
#      sessions/FactDigitalSessions, fraud_alerts/FactFraudAlerts.
#      DimCustomer/DimAccount/AggCustomerRiskProfile are NOT row-count
#      comparable to any single Silver table (union/derived grain) — each
#      gets a documented note instead of a PASS/FAIL count check.
#   4. Quarantine counts, each its own row — a zero count is a real PASS
#      signal, not "nothing to check", so every quarantine table gets a row
#      regardless of count.
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

_start_ts = datetime.now(timezone.utc)
if not run_date:
    run_date = _start_ts.strftime("%Y-%m-%d")

# The mirror item is healthy but NOT consumable: its Fabric connection uses a Databricks PAT
# (credentialType "Key"), which Fabric rejects for any OneLake-shortcut-resolution-based
# read -- confirmed live 2026-09-01 across direct paths, a Lakehouse shortcut, and the SQL
# analytics endpoint. See docs/databricks-fabric-integration.md "Consumption blocker".
# DATABRICKS_SHORTCUT_TABLES (already-created Lakehouse shortcuts) is the correct pattern
# and needs no code change once a human fixes the connection's credential type. Kept in sync
# with nb_source_validation.py / nb_silver_transform.py.
DATABRICKS_MIRROR_ITEM_NAME = "BLOCKED_databricks_mirror_key_credential_see_docs"
DATABRICKS_SHORTCUT_TABLES = {
    "transactions": "src_databricks_transactions",
    "transaction_risk_scores": "src_databricks_transaction_risk_scores",
    "merchants": "src_databricks_merchants",
}
# Still blocked as of 2026-09-01 -- see docs/cosmos-fabric-mirroring.md "What's not done,
# and exactly why" and infra/fabric/mirror_cosmos.py's docstring for the one remaining
# manual step (portal-only OAuth sign-in for the Cosmos DB v2 connection -- all networking
# prerequisites are already done and verified). Deliberately BLOCKED-prefixed so the loop
# below annotates its FAIL rows with why, instead of looking like a real mismatch.
COSMOS_MIRROR_ITEM_NAME = "BLOCKED_no_cosmos_mirror_see_docs"

WORKSPACE_ID = "7e206237-aef1-4932-9f94-1f6ae343407a"
SILVER_LH_ID = "6575b89d-6ea5-4273-9b7a-8c9dc67ef2c0"


def silver_table(name: str):
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LH_ID}/Tables/{name}"
    return spark.read.format("delta").load(path)


def mirror_count(fq_name: str) -> int | None:
    try:
        return spark.table(fq_name).count()
    except AnalysisException:
        return None


rows: list[Row] = []


def add_row(source, obj, source_count, fabric_count, validation_type, status, notes):
    rows.append(Row(
        source=source, object=obj,
        source_count=int(source_count) if source_count is not None else None,
        fabric_count=int(fabric_count) if fabric_count is not None else None,
        validation_type=validation_type, status=status, notes=notes,
        run_id=run_id, run_date=run_date,
    ))

# CELL ********************

# 1. Databricks source-vs-Silver (two access paths to the same data).
DATABRICKS_CHECKS = [
    ("transactions", "transactions", "quarantine_transactions"),
    ("transaction_risk_scores", "transaction_risk", "quarantine_transaction_risk"),
    ("merchants", "merchants", "quarantine_merchants"),
]
for mirror_table, silver_valid, silver_quarantine in DATABRICKS_CHECKS:
    fabric_count = silver_table(silver_valid).count() + silver_table(silver_quarantine).count()
    if DATABRICKS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        src_count = None
        notes = (
            "EXPECTED FAIL: the Databricks mirror is healthy but not consumable (Key-type "
            "connection credential unsupported for OneLake-shortcut reads -- see "
            "docs/databricks-fabric-integration.md 'Consumption blocker'). Not a real "
            "reconciliation mismatch."
        )
    else:
        src_count = mirror_count(DATABRICKS_SHORTCUT_TABLES[mirror_table])
        notes = (
            "Validates two access paths to the SAME underlying Delta data via the OneLake "
            "shortcut, not real replication — see ARCHITECTURE.md 'Sources and Bronze'. "
            "fabric_count = Silver valid + Silver quarantine (reconstructs the mirror's total)."
        )
    status = "PASS" if (src_count is not None and src_count == fabric_count) else "FAIL"
    add_row("databricks", mirror_table, src_count, fabric_count, "row_count", status, notes)

# CELL ********************

# 2. Cosmos source-vs-Silver (genuinely replicated target snapshot).
COSMOS_CHECKS = [
    ("digitalSessions", "sessions", "quarantine_sessions"),
    ("devices", "devices", "quarantine_devices"),
    ("fraudAlerts", "fraud_alerts", "quarantine_fraud_alerts"),
]
for mirror_table, silver_valid, silver_quarantine in COSMOS_CHECKS:
    fq = f"`{COSMOS_MIRROR_ITEM_NAME}`.`{mirror_table}`"
    src_count = mirror_count(fq)
    fabric_count = silver_table(silver_valid).count() + silver_table(silver_quarantine).count()
    status = "PASS" if (src_count is not None and src_count == fabric_count) else "FAIL"
    notes = (
        "Validates a genuinely replicated target snapshot (Cosmos mirroring physically "
        "copies documents into Delta) — unlike the Databricks checks above. fabric_count = "
        "Silver valid + Silver quarantine."
    )
    if COSMOS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        notes = (
            "EXPECTED FAIL: the Cosmos mirror does not exist yet (blocked on a portal-only "
            "OAuth sign-in for the Cosmos DB v2 connection -- all networking prerequisites "
            "are done; see docs/cosmos-fabric-mirroring.md). Not a real reconciliation mismatch."
        )
    add_row("cosmos", mirror_table, src_count, fabric_count, "row_count", status, notes)

# CELL ********************

# 3. Silver-vs-Gold — exact match expected.
SILVER_GOLD_CHECKS = [
    ("merchants", "merchants"),
    ("devices", "dimdevice"),
    ("transactions", "facttransactions"),
    ("sessions", "factdigitalsessions"),
    ("fraud_alerts", "factfraudalerts"),
]
for silver_name, gold_name in SILVER_GOLD_CHECKS:
    silver_count = silver_table(silver_name).count()
    gold_count = spark.table(gold_name).count()
    status = "PASS" if silver_count == gold_count else "FAIL"
    add_row(
        "silver_gold", f"{silver_name}->{gold_name}", silver_count, gold_count, "row_count", status,
        "Exact 1:1 match expected — Gold applies no additional filtering for this table.",
    )

# Grain-mismatch notes — documented, not a count check (ARCHITECTURE.md
# calls this out explicitly for AggCustomerRiskProfile; DimCustomer/
# DimAccount have the same property for the same underlying reason).
add_row(
    "silver_gold", "*->dimcustomer", None, spark.table("dimcustomer").count(), "grain_note", "PASS",
    "DimCustomer's grain is the UNION of distinct CustomerID across transactions/sessions/"
    "devices/fraud_alerts — not a row-count match against any single Silver table.",
)
add_row(
    "silver_gold", "transactions->dimaccount", None, spark.table("dimaccount").count(), "grain_note", "PASS",
    "DimAccount is distinct AccountID from Silver transactions (the only table either "
    "source carries AccountID on) — not a row-count match against transactions itself.",
)
add_row(
    "silver_gold", "*->aggcustomerriskprofile", None, spark.table("aggcustomerriskprofile").count(),
    "grain_note", "PASS",
    "AggCustomerRiskProfile's grain is the customer-union (same as DimCustomer) with "
    "independent 30-day source-relative watermarks — not a raw row-count match against "
    "any single Silver table. See ARCHITECTURE.md 'AggCustomerRiskProfile'.",
)

# CELL ********************

# 4. Quarantine counts, each its own row — zero is a real PASS, not "nothing to check".
QUARANTINE_TABLES = [
    ("databricks", "quarantine_transactions"), ("databricks", "quarantine_transaction_risk"),
    ("databricks", "quarantine_merchants"), ("cosmos", "quarantine_sessions"),
    ("cosmos", "quarantine_devices"), ("cosmos", "quarantine_fraud_alerts"),
]
for source, table in QUARANTINE_TABLES:
    n = silver_table(table).count()
    add_row(source, table, None, n, "quarantine_count", "PASS", f"{n} row(s) quarantined this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RECONCILIATION_SCHEMA = StructType([
    StructField("source", StringType()), StructField("object", StringType()),
    StructField("source_count", LongType()), StructField("fabric_count", LongType()),
    StructField("validation_type", StringType()), StructField("status", StringType()),
    StructField("notes", StringType()), StructField("run_id", StringType()),
    StructField("run_date", StringType()),
])
reconciliation_df = spark.createDataFrame(rows, schema=RECONCILIATION_SCHEMA)
reconciliation_df.write.mode("append").saveAsTable("reconciliation_results")

n_fail = reconciliation_df.where(F.col("status") == "FAIL").count()
for r in reconciliation_df.collect():
    print(f"[{r.status}] {r.source}/{r.object} ({r.validation_type}): source={r.source_count} fabric={r.fabric_count}")

import notebookutils  # noqa: E402

# Reflects the true outcome: if any check FAILed, this notebook's own run
# is Failed too (status here + the raise below both say so), even though
# every check still got recorded in reconciliation_results either way.
# Every other notebook in this repo only calls pipeline_log on ITS OWN
# success path (see nb_pipeline_log.py header) — this is the one exception,
# because reconciliation's "own success" (running every check and writing
# the results table) and "overall pass/fail" are genuinely different
# things, and both are worth capturing in the audit trail, not just the
# pipeline's LogXFailure activity.
_status = "Succeeded" if n_fail == 0 else "Failed"
_error_message = "" if n_fail == 0 else f"{n_fail} reconciliation check(s) FAILed — see reconciliation_results"

notebookutils.notebook.run(
    "pipeline_log",
    120,
    {
        "run_id": run_id,
        "stage": "reconciliation",
        "status": _status,
        "start_ts": _start_ts.isoformat(),
        "end_ts": datetime.now(timezone.utc).isoformat(),
        "rows_read": len(rows),
        "rows_written": len(rows),
        "error_message": _error_message,
        "run_date": run_date,
    },
)

if n_fail > 0:
    raise RuntimeError(f"{n_fail} reconciliation check(s) FAILed — see gold_lh.reconciliation_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
