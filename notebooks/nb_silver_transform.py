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

run_id = "manual"
run_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reads both mirrored sources directly (no physical Bronze — see
# ARCHITECTURE.md "Sources and Bronze"), applies the quality rules from
# ARCHITECTURE.md's Silver section, and writes 6 valid + 6 quarantine_*
# Delta tables into silver_lh. Every quarantine table is always written,
# even when empty (an empty quarantine table is a real PASS signal, not an
# absent one).
#
# *** ASSUMED COSMOS DOCUMENT SHAPE — COORDINATE WITH THE COSMOS GENERATOR ***
# The Cosmos generator (generators/ — owned by a parallel workstream) did
# not exist yet when this notebook was written, so the exact field names on
# digitalSessions/devices/fraudAlerts documents are an assumption, not a
# verified fact. Field names below follow the camelCase convention
# ARCHITECTURE.md already establishes for customerId, and are the most
# literal reading of ARCHITECTURE.md's own wording ("nested device/geo/
# authentication flattened to columns", "riskSignals[]/geoHistory[] kept as
# JSON strings"). If the real generator uses different field names, only
# the constants/get_json_object paths in the sessions/devices/fraud_alerts
# cells below need to change — the quality-rule logic itself does not.
# Assumed shapes (documented in full in this repo's delivery report):
#   digitalSessions: sessionId, customerId, deviceId, loginTimestamp,
#     logoutTimestamp, device{deviceType,os,browser},
#     geo{country,city,ipAddress},
#     authentication{method,mfaUsed,success}, activities[]
#   devices: deviceId, customerId, deviceType, os, deviceFingerprint,
#     isTrusted, riskSignals[], geoHistory[] (each entry assumed to carry
#     country, city, observedAt, isAnomaly)
#   fraudAlerts: alertId, customerId, transactionId (nullable — the
#     transaction the alert references, needed for
#     nb_gold_consumption_demo's fraud-to-transaction join), severity,
#     alertTimestamp, alertType
#
# DeviceID format is "DEV-" + 6 digits (ARCHITECTURE.md was briefly
# inconsistent with this — the generators were correct; ARCHITECTURE.md has
# been corrected to match, not the other way around). This notebook doesn't
# hardcode the prefix anywhere, so no logic change was needed here.
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

_start_ts = datetime.now(timezone.utc)
if not run_date:
    run_date = _start_ts.strftime("%Y-%m-%d")

# Resolved 2026-09-01: both mirror connections were fixed via human interactive OAuth2
# sign-in (Key-type credential -> OAuth2 for Databricks; new connection created for Cosmos,
# which has no unattended-mintable credential path at all for a VirtualNetworkGateway
# connection). Both confirmed live via exact row-count matches to source -- see
# docs/databricks-fabric-integration.md and docs/cosmos-fabric-mirroring.md for the full
# traces. DATABRICKS_MIRROR_ITEM_NAME is only used as a BLOCKED_-prefix guard flag here; the
# actual read path is DATABRICKS_SHORTCUT_TABLES (Lakehouse OneLake shortcuts in silver_lh).
DATABRICKS_MIRROR_ITEM_NAME = "fmv2poc_databricks_banking_mirror"
DATABRICKS_SHORTCUT_TABLES = {
    "transactions": "src_databricks_transactions",
    "transaction_risk_scores": "src_databricks_transaction_risk_scores",
    "merchants": "src_databricks_merchants",
}
COSMOS_MIRROR_ITEM_NAME = "fmv2poc_cosmos_multisource_mirror"
COSMOS_MIRROR_ITEM_ID = "0a4ace01-7f5b-4c83-b9f4-6e267d167a7d"
COSMOS_MIRROR_SCHEMA = "multisource"
WORKSPACE_ID = "7e206237-aef1-4932-9f94-1f6ae343407a"


def mirror_databricks_table(table: str):
    if DATABRICKS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        raise RuntimeError(
            "The Databricks mirror is not consumable yet -- its Fabric connection uses a "
            "Key-type credential (a Databricks PAT), which Fabric rejects for any "
            "OneLake-shortcut-resolution-based read. See "
            "docs/databricks-fabric-integration.md 'Consumption blocker' for the trace and "
            "the human-only fix (recreate the connection with OAuth2 or a Service "
            "Principal). No code change needed here once fixed."
        )
    shortcut_name = DATABRICKS_SHORTCUT_TABLES[table]
    try:
        return spark.table(shortcut_name)
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read Databricks shortcut table {shortcut_name}. Original error: {exc}") from exc


def mirror_cosmos_table(table: str):
    if COSMOS_MIRROR_ITEM_NAME.startswith("BLOCKED_"):
        raise RuntimeError(
            "The Cosmos DB mirror does not exist yet -- creating its Fabric connection "
            "requires a one-time interactive OAuth sign-in in the Fabric portal that "
            "cannot be scripted (confirmed live, not assumed). See "
            "docs/cosmos-fabric-mirroring.md 'What's not done, and exactly why' for the "
            "precise manual steps, then update COSMOS_MIRROR_ITEM_NAME here (and in "
            "nb_source_validation.py / nb_reconciliation.py) to the real mirror item name "
            "and re-run infra/fabric/mirror_cosmos.py to finish setup."
        )
    # Read via the direct OneLake ABFSS path rather than spark.table(): Fabric's
    # spark_catalog rejects multi-part namespaces for a cross-item reference
    # ("REQUIRES_SINGLE_PART_NAMESPACE" -- confirmed live), so a MirroredDatabase
    # item's schema-qualified tables aren't addressable through spark_catalog SQL
    # name resolution at all. The ABFSS path bypasses catalog resolution entirely.
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{COSMOS_MIRROR_ITEM_ID}/Tables/{COSMOS_MIRROR_SCHEMA}/{table}"
    try:
        return spark.read.format("delta").load(path)
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read mirrored Cosmos table at {path}. Original error: {exc}") from exc


def cast_if_present(df, col_name: str, spark_type: str):
    return df.withColumn(col_name, F.col(col_name).cast(spark_type)) if col_name in df.columns else df


def quarantine_split(df, is_valid, reason: str):
    """NULL in is_valid must fail closed into quarantine, not silently
    vanish from both sets — per ARCHITECTURE.md, wrap with coalesce(...,
    False) so an unevaluable condition is treated as invalid."""
    is_valid_safe = F.coalesce(is_valid, F.lit(False))
    valid = df.filter(is_valid_safe)
    invalid = df.filter(~is_valid_safe).withColumn("_quarantine_reason", F.lit(reason))
    return valid, invalid


def write_silver(df, name: str):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)


counts: dict[str, int] = {}

# CELL ********************

# transactions (Databricks) — TransactionID matches TXN-\d{9}, non-null
# CustomerID/TransactionTimestamp, Amount >= 0.
transactions_raw = mirror_databricks_table("transactions").withColumn(
    "TransactionTimestamp", F.col("TransactionTimestamp").cast("timestamp")
)
counts["transactions_in"] = transactions_raw.count()

is_valid_txn = (
    F.col("TransactionID").rlike(r"^TXN-\d{9}$")
    & F.col("CustomerID").isNotNull()
    & F.col("TransactionTimestamp").isNotNull()
    & (F.col("Amount") >= 0)
)
transactions_valid, transactions_quarantine = quarantine_split(
    transactions_raw, is_valid_txn, "silver_quality_rule_violation"
)
write_silver(transactions_valid, "transactions")
write_silver(transactions_quarantine, "quarantine_transactions")
counts["transactions_out"] = transactions_valid.count()
counts["quarantine_transactions"] = transactions_quarantine.count()
print(f"transactions: {counts['transactions_out']} valid, {counts['quarantine_transactions']} quarantined")

# CELL ********************

# transaction_risk (Databricks, source table transaction_risk_scores) —
# RiskScore in [0,100]; TransactionID must exist in the VALIDATED
# transactions set (cross-table check against transactions_valid, not the
# raw read).
risk_raw = mirror_databricks_table("transaction_risk_scores").withColumn(
    "ScoredTimestamp", F.col("ScoredTimestamp").cast("timestamp")
)
counts["transaction_risk_in"] = risk_raw.count()

valid_txn_ids = transactions_valid.select(F.col("TransactionID").alias("_valid_txn_id")).distinct()
risk_joined = risk_raw.join(valid_txn_ids, risk_raw.TransactionID == F.col("_valid_txn_id"), "left")
is_valid_risk = F.col("RiskScore").between(0, 100) & F.col("_valid_txn_id").isNotNull()
risk_valid, risk_quarantine = quarantine_split(risk_joined, is_valid_risk, "silver_quality_rule_violation")
risk_valid = risk_valid.drop("_valid_txn_id")
risk_quarantine = risk_quarantine.drop("_valid_txn_id")
write_silver(risk_valid, "transaction_risk")
write_silver(risk_quarantine, "quarantine_transaction_risk")
counts["transaction_risk_out"] = risk_valid.count()
counts["quarantine_transaction_risk"] = risk_quarantine.count()
print(f"transaction_risk: {counts['transaction_risk_out']} valid, {counts['quarantine_transaction_risk']} quarantined")

# CELL ********************

# merchants (Databricks) — non-null MerchantID/MerchantCategory.
merchants_raw = mirror_databricks_table("merchants")
counts["merchants_in"] = merchants_raw.count()

is_valid_merchant = F.col("MerchantID").isNotNull() & F.col("MerchantCategory").isNotNull()
merchants_valid, merchants_quarantine = quarantine_split(merchants_raw, is_valid_merchant, "silver_quality_rule_violation")
write_silver(merchants_valid, "merchants")
write_silver(merchants_quarantine, "quarantine_merchants")
counts["merchants_out"] = merchants_valid.count()
counts["quarantine_merchants"] = merchants_quarantine.count()
print(f"merchants: {counts['merchants_out']} valid, {counts['quarantine_merchants']} quarantined")

# CELL ********************

# sessions (Cosmos, container digitalSessions) — non-null customerId/
# sessionId; nested device/geo/authentication flattened to columns;
# activities[] kept as a JSON string, untouched. See the ASSUMED COSMOS
# DOCUMENT SHAPE note at the top of this notebook.
sessions_raw = mirror_cosmos_table("digitalSessions")
sessions_raw = cast_if_present(sessions_raw, "loginTimestamp", "timestamp")
sessions_raw = cast_if_present(sessions_raw, "logoutTimestamp", "timestamp")
counts["sessions_in"] = sessions_raw.count()

is_valid_session = F.col("customerId").isNotNull() & F.col("sessionId").isNotNull()
sessions_valid_raw, sessions_quarantine = quarantine_split(sessions_raw, is_valid_session, "silver_quality_rule_violation")

sessions_flat = (
    # Real generator schema (generators/generate_cosmos_data.py _generate_sessions,
    # confirmed live 2026-09-01 via a real pipeline run failure -- the original
    # extraction paths below were speculative, written before the Cosmos
    # generator was finalized, and didn't match): device has
    # {deviceId, deviceType, operatingSystem, appVersion} -- no "os"/"browser".
    # geo has {country, state, city} -- no "ipAddress" (that's the top-level
    # ipAddress field, already a plain column, no extraction needed).
    # authentication has {method, mfaUsed, failedAttempts} -- no "success".
    sessions_valid_raw.withColumn("device_deviceId", F.get_json_object(F.col("device"), "$.deviceId"))
    .withColumn("device_deviceType", F.get_json_object(F.col("device"), "$.deviceType"))
    .withColumn("device_operatingSystem", F.get_json_object(F.col("device"), "$.operatingSystem"))
    .withColumn("geo_country", F.get_json_object(F.col("geo"), "$.country"))
    .withColumn("geo_state", F.get_json_object(F.col("geo"), "$.state"))
    .withColumn("geo_city", F.get_json_object(F.col("geo"), "$.city"))
    .withColumn("auth_method", F.get_json_object(F.col("authentication"), "$.method"))
    .withColumn("auth_mfaUsed", F.get_json_object(F.col("authentication"), "$.mfaUsed").cast("boolean"))
    .withColumn("auth_failedAttempts", F.get_json_object(F.col("authentication"), "$.failedAttempts").cast("int"))
    .drop("device", "geo", "authentication")
    # activities[] is left as-is (already a JSON string per the Cosmos
    # mirror's generic nested-array handling) — arrays don't flatten to a
    # fixed row shape, so ARCHITECTURE.md keeps them opaque JSON text here.
)
write_silver(sessions_flat, "sessions")
write_silver(sessions_quarantine, "quarantine_sessions")
counts["sessions_out"] = sessions_flat.count()
counts["quarantine_sessions"] = sessions_quarantine.count()
print(f"sessions: {counts['sessions_out']} valid, {counts['quarantine_sessions']} quarantined")

# CELL ********************

# devices (Cosmos) — non-null deviceId/customerId; riskSignals[]/
# geoHistory[] kept as JSON strings, untouched (no nested object flattening
# specified for devices in ARCHITECTURE.md, only arrays).
devices_raw = mirror_cosmos_table("devices")
counts["devices_in"] = devices_raw.count()

is_valid_device = F.col("deviceId").isNotNull() & F.col("customerId").isNotNull()
devices_valid, devices_quarantine = quarantine_split(devices_raw, is_valid_device, "silver_quality_rule_violation")
write_silver(devices_valid, "devices")
write_silver(devices_quarantine, "quarantine_devices")
counts["devices_out"] = devices_valid.count()
counts["quarantine_devices"] = devices_quarantine.count()
print(f"devices: {counts['devices_out']} valid, {counts['quarantine_devices']} quarantined")

# CELL ********************

# fraud_alerts (Cosmos, container fraudAlerts) — non-null alertId/
# customerId; severity in {low,medium,high,critical}. Real generator schema
# (generators/generate_cosmos_data.py _generate_alerts, confirmed live
# 2026-09-01): the timestamp field is createdTimestamp, not alertTimestamp,
# and severity values are lowercase, not PascalCase -- the original
# PascalCase isin() check would have silently quarantined every real row.
fraud_alerts_raw = mirror_cosmos_table("fraudAlerts")
fraud_alerts_raw = cast_if_present(fraud_alerts_raw, "createdTimestamp", "timestamp")
counts["fraud_alerts_in"] = fraud_alerts_raw.count()

is_valid_alert = (
    F.col("alertId").isNotNull()
    & F.col("customerId").isNotNull()
    & F.col("severity").isin("low", "medium", "high", "critical")
)
fraud_valid, fraud_quarantine = quarantine_split(fraud_alerts_raw, is_valid_alert, "silver_quality_rule_violation")
write_silver(fraud_valid, "fraud_alerts")
write_silver(fraud_quarantine, "quarantine_fraud_alerts")
counts["fraud_alerts_out"] = fraud_valid.count()
counts["quarantine_fraud_alerts"] = fraud_quarantine.count()
print(f"fraud_alerts: {counts['fraud_alerts_out']} valid, {counts['quarantine_fraud_alerts']} quarantined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils  # noqa: E402

rows_read = sum(v for k, v in counts.items() if k.endswith("_in"))
rows_written = sum(v for k, v in counts.items() if k.endswith("_out"))

# useRootDefaultLakehouse=True: this notebook is attached to silver_lh but
# pipeline_log is attached to gold_lh -- see nb_source_validation.py for the
# full explanation of this requirement.
notebookutils.notebook.run(
    "pipeline_log",
    180,
    {
        "run_id": run_id,
        "stage": "silver_transform",
        "status": "Succeeded",
        "start_ts": _start_ts.isoformat(),
        "end_ts": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_written": rows_written,
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
