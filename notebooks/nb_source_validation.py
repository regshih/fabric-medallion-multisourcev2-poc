# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6575b89d-6ea5-4273-9b7a-8c9dc67ef2c0",
# META       "default_lakehouse_name": "silver_lh",
# META       "default_lakehouse_workspace_id": "7e206237-aef1-4932-9f94-1f6ae343407a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# source = "databricks" | "cosmos". The pipeline runs this notebook twice in
# parallel (Validate Databricks, Validate Cosmos — see ARCHITECTURE.md
# "Pipeline"), once per value. Default lets a standalone run still do
# something sensible.
run_id = "manual"
run_date = ""
source = "databricks"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Confirms the two mirrored sources are queryable and non-empty before
# Silver runs. Fails loudly (raises) on anything short of that — this is a
# gate, not a best-effort check.
#
# *** UNVERIFIED AGAINST LIVE FABRIC — ITEM NAMES ARE PLACEHOLDERS ***
# The real Fabric display names for the "Mirrored Azure Databricks Catalog"
# item and the Cosmos DB mirror item are not known to this notebook — those
# two mirrors don't exist yet (another workstream provisions the source
# infra). Fill in DATABRICKS_MIRROR_ITEM_NAME / COSMOS_MIRROR_ITEM_NAME
# below once they do; everything else in this notebook is otherwise
# complete. See this repo's top-level report for the assumption this rests
# on: that both mirror items are queryable via Spark SQL using the item's
# display name as a catalog, e.g. `` `item_name`.`schema`.`table` `` for the
# multi-schema Databricks catalog mirror and `` `item_name`.`table` `` for
# the single-level Cosmos mirror (Fabric's Spark runtime auto-registers
# every workspace item as a catalog by display name — this is the standard
# mechanism, but has not been exercised live in this repo).
from datetime import datetime, timezone

from pyspark.sql.utils import AnalysisException

_start_ts = datetime.now(timezone.utc)
if not run_date:
    run_date = _start_ts.strftime("%Y-%m-%d")
if source not in ("databricks", "cosmos"):
    raise ValueError(f"source must be 'databricks' or 'cosmos', got {source!r}")

# --- Databricks (Unity Catalog, per ARCHITECTURE.md) ---
# The mirror item itself is healthy (mirrorStatus=Mirrored, syncDetails.status=Success,
# confirmed live 2026-09-01 by infra/fabric/mirror_databricks.py) but is NOT consumable from
# any Fabric surface: its connection uses a Databricks PAT (credentialType "Key"), which
# Fabric does not support for OneLake-shortcut-resolution-based reads. Confirmed live across
# three independent paths, all failing the same way ("Stored connections with authentication
# type 'Key' are not supported..."): direct mirror-path reads, a Lakehouse OneLake shortcut
# (infra/fabric/shortcut_databricks_mirror.py -- created successfully, but unreadable), and
# the mirror's own auto-generated SQL analytics endpoint (refreshMetadata returns a per-table
# 400 BadRequest). See docs/databricks-fabric-integration.md "Consumption blocker" for the
# full trace and the human-only fix (recreate the Fabric connection with an OAuth2/
# Resolved 2026-09-01: both mirror connections fixed via human interactive OAuth2 sign-in --
# see docs/databricks-fabric-integration.md / docs/cosmos-fabric-mirroring.md for the full
# traces (exact row-count matches to source confirmed for both).
#
# DATABRICKS_MIRROR_ITEM_NAME is only used as a BLOCKED_-prefix guard flag; the actual read
# path is DATABRICKS_SHORTCUT_TABLES (Lakehouse OneLake shortcuts already created in silver_lh).
DATABRICKS_MIRROR_ITEM_NAME = "fmv2poc_databricks_banking_mirror"
DATABRICKS_SHORTCUT_TABLES = {
    "transactions": "src_databricks_transactions",
    "transaction_risk_scores": "src_databricks_transaction_risk_scores",
    "merchants": "src_databricks_merchants",
}

# --- Cosmos DB (per ARCHITECTURE.md) ---
COSMOS_MIRROR_ITEM_NAME = "fmv2poc_cosmos_multisource_mirror"
COSMOS_MIRROR_ITEM_ID = "0a4ace01-7f5b-4c83-b9f4-6e267d167a7d"
COSMOS_MIRROR_SCHEMA = "multisource"
COSMOS_TABLES = ["digitalSessions", "devices", "fraudAlerts"]
WORKSPACE_ID = "7e206237-aef1-4932-9f94-1f6ae343407a"


def _count(fq_name: str) -> int:
    try:
        return spark.table(fq_name).count()
    except AnalysisException as exc:
        raise RuntimeError(f"Could not query {fq_name}. Original error: {exc}") from exc


def _count_cosmos_table(table: str) -> int:
    # Read via the direct OneLake ABFSS path, not spark.table(): Fabric's
    # spark_catalog rejects multi-part namespaces for a cross-item reference
    # ("REQUIRES_SINGLE_PART_NAMESPACE" -- confirmed live), so a MirroredDatabase
    # item's schema-qualified tables aren't addressable through spark_catalog SQL
    # name resolution at all. The ABFSS path bypasses catalog resolution entirely.
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{COSMOS_MIRROR_ITEM_ID}/Tables/{COSMOS_MIRROR_SCHEMA}/{table}"
    try:
        return spark.read.format("delta").load(path).count()
    except AnalysisException as exc:
        raise RuntimeError(f"Could not query mirrored Cosmos table at {path}. Original error: {exc}") from exc


def validate_databricks() -> dict[str, int]:
    if DATABRICKS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        raise RuntimeError(
            "The Databricks mirror item is healthy but not consumable: its Fabric connection "
            "uses a Databricks PAT (credentialType 'Key'), which Fabric rejects for any "
            "OneLake-shortcut-resolution-based read (confirmed live across direct paths, a "
            "Lakehouse shortcut, and the SQL analytics endpoint -- see "
            "docs/databricks-fabric-integration.md 'Consumption blocker'). A human must "
            "recreate the Fabric connection with an OAuth2/Organizational-account or Service "
            "Principal credential (both require real interactive auth). Once fixed, this "
            "notebook needs no code change -- DATABRICKS_SHORTCUT_TABLES already points at "
            "the correct, working Lakehouse shortcuts."
        )
    counts = {}
    for table, shortcut_name in DATABRICKS_SHORTCUT_TABLES.items():
        n = _count(shortcut_name)
        if n == 0:
            raise RuntimeError(f"Databricks shortcut table {shortcut_name} is queryable but has zero rows")
        counts[table] = n
    return counts


def validate_cosmos() -> dict[str, int]:
    if COSMOS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        raise RuntimeError(
            "The Cosmos DB mirror does not exist yet: its Fabric connection requires a "
            "one-time interactive OAuth sign-in in the Fabric portal that cannot be "
            "scripted (confirmed live -- POSTing OAuth2 credentials for a "
            "VirtualNetworkGateway connection returns 400 "
            "OAuth2CredentialsNotSupportedForConnection). All networking prerequisites "
            "(VNet gateway, NAT gateway, RBAC role, Network ACL Bypass) are already done "
            "and verified. See docs/cosmos-fabric-mirroring.md 'What's not done, and "
            "exactly why' for the exact remaining portal steps. Once done, update "
            "COSMOS_MIRROR_ITEM_NAME here (and in nb_silver_transform.py / "
            "nb_reconciliation.py) to the real mirror item name."
        )
    counts = {}
    for table in COSMOS_TABLES:
        n = _count_cosmos_table(table)
        if n == 0:
            raise RuntimeError(f"Cosmos mirrored table {table} is queryable but has zero rows")
        counts[table] = n
    return counts


counts = validate_databricks() if source == "databricks" else validate_cosmos()
rows_read = sum(counts.values())
print(f"source_validation[{source}]: {counts} (total {rows_read} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils  # noqa: E402

# useRootDefaultLakehouse=True: this notebook is attached to silver_lh but
# pipeline_log is attached to gold_lh -- without this, notebookutils.notebook.run
# raises "Cannot reference a Notebook that attaching to a different default
# lakehouse" (confirmed live 2026-09-01; this call was previously unreached
# because every earlier run raised its own guard/error before getting here).
notebookutils.notebook.run(
    "pipeline_log",
    120,
    {
        "run_id": run_id,
        "stage": f"source_validation_{source}",
        "status": "Succeeded",
        "start_ts": _start_ts.isoformat(),
        "end_ts": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_written": 0,
        "error_message": "",
        "run_date": run_date,
        "useRootDefaultLakehouse": True,
    },
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
