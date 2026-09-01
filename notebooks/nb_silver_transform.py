# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "silver_lh",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
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

DATABRICKS_MIRROR_ITEM_NAME = "PLACEHOLDER_databricks_mirror_item_name"  # TODO fill in
DATABRICKS_SCHEMA = "banking"
COSMOS_MIRROR_ITEM_NAME = "PLACEHOLDER_cosmos_mirror_item_name"  # TODO fill in


def mirror_databricks_table(table: str):
    fq = f"`{DATABRICKS_MIRROR_ITEM_NAME}`.`{DATABRICKS_SCHEMA}`.`{table}`"
    try:
        return spark.table(fq)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Could not read mirrored Databricks table {fq}. Fill in "
            f"DATABRICKS_MIRROR_ITEM_NAME with the real mirror item name "
            f"once it exists. Original error: {exc}"
        ) from exc


def mirror_cosmos_table(table: str):
    fq = f"`{COSMOS_MIRROR_ITEM_NAME}`.`{table}`"
    try:
        return spark.table(fq)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Could not read mirrored Cosmos table {fq}. Fill in "
            f"COSMOS_MIRROR_ITEM_NAME with the real mirror item name once "
            f"it exists. Original error: {exc}"
        ) from exc


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
    sessions_valid_raw.withColumn("device_deviceType", F.get_json_object(F.col("device"), "$.deviceType"))
    .withColumn("device_os", F.get_json_object(F.col("device"), "$.os"))
    .withColumn("device_browser", F.get_json_object(F.col("device"), "$.browser"))
    .withColumn("geo_country", F.get_json_object(F.col("geo"), "$.country"))
    .withColumn("geo_city", F.get_json_object(F.col("geo"), "$.city"))
    .withColumn("geo_ipAddress", F.get_json_object(F.col("geo"), "$.ipAddress"))
    .withColumn("auth_method", F.get_json_object(F.col("authentication"), "$.method"))
    .withColumn("auth_mfaUsed", F.get_json_object(F.col("authentication"), "$.mfaUsed").cast("boolean"))
    .withColumn("auth_success", F.get_json_object(F.col("authentication"), "$.success").cast("boolean"))
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
# customerId; severity in {Low,Medium,High,Critical}.
fraud_alerts_raw = mirror_cosmos_table("fraudAlerts")
fraud_alerts_raw = cast_if_present(fraud_alerts_raw, "alertTimestamp", "timestamp")
counts["fraud_alerts_in"] = fraud_alerts_raw.count()

is_valid_alert = (
    F.col("alertId").isNotNull()
    & F.col("customerId").isNotNull()
    & F.col("severity").isin("Low", "Medium", "High", "Critical")
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
    },
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
