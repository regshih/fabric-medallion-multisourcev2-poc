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
# Real Fabric display name, confirmed live 2026-09-01 by infra/fabric/mirror_databricks.py.
DATABRICKS_MIRROR_ITEM_NAME = "fmv2poc_databricks_banking_mirror"
DATABRICKS_SCHEMA = "banking"
DATABRICKS_TABLES = ["transactions", "transaction_risk_scores", "merchants"]

# --- Cosmos DB (per ARCHITECTURE.md) ---
# Still blocked as of 2026-09-01 -- see docs/cosmos-fabric-mirroring.md "What's not done,
# and exactly why" and infra/fabric/mirror_cosmos.py's docstring. The gateway is deployed
# and all Cosmos-side networking prerequisites (RBAC role, Network ACL Bypass, NAT gateway)
# are done and verified, but the Cosmos DB v2 connection through the gateway requires a
# one-time interactive OAuth sign-in in the Fabric portal (confirmed live: the REST API
# rejects OAuth2 credentials for VirtualNetworkGateway connections outright). Deliberately
# BLOCKED-prefixed, not PLACEHOLDER-prefixed, so validate_cosmos() below fails with a clear,
# specific message instead of a generic Spark AnalysisException.
COSMOS_MIRROR_ITEM_NAME = "BLOCKED_no_cosmos_mirror_see_docs"
COSMOS_TABLES = ["digitalSessions", "devices", "fraudAlerts"]


def _count(fq_name: str) -> int:
    try:
        return spark.table(fq_name).count()
    except AnalysisException as exc:
        raise RuntimeError(
            f"Could not query {fq_name}. If the mirror item name is still the "
            f"PLACEHOLDER_ value above, fill it in with the real Fabric display "
            f"name once the mirror is provisioned. Original error: {exc}"
        ) from exc


def validate_databricks() -> dict[str, int]:
    counts = {}
    for table in DATABRICKS_TABLES:
        fq = f"`{DATABRICKS_MIRROR_ITEM_NAME}`.`{DATABRICKS_SCHEMA}`.`{table}`"
        n = _count(fq)
        if n == 0:
            raise RuntimeError(f"Databricks mirrored table {fq} is queryable but has zero rows")
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
        fq = f"`{COSMOS_MIRROR_ITEM_NAME}`.`{table}`"
        n = _count(fq)
        if n == 0:
            raise RuntimeError(f"Cosmos mirrored table {fq} is queryable but has zero rows")
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
    },
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
