# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "gold_lh",
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

# Star schema over Silver, per ARCHITECTURE.md's Gold section. Surrogate
# keys are xxhash64(business_key) — stable, no join needed to populate a FK
# (any fact/dim just re-hashes the business key it already carries), no SCD.
#
# Neither source exposes a standalone "customers" master table (Databricks
# has transactions/transaction_risk_scores/merchants only; Cosmos has
# digitalSessions/devices/fraudAlerts only) — so DimCustomer and DimAccount
# are themselves derived, not sourced 1:1 from a Silver table:
#   DimCustomer = distinct CustomerID across the union of the four
#     customer-bearing Silver tables (transactions, sessions, devices,
#     fraud_alerts) — the same "union, not inner join" grain ARCHITECTURE.md
#     specifies for AggCustomerRiskProfile.
#   DimAccount = distinct AccountID from Silver transactions (the only
#     table either source carries AccountID on).
# This is a direct, unavoidable consequence of the source schemas as
# described in ARCHITECTURE.md, not a scope decision — flagged in the
# delivery report as worth confirming once real data is available.
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

_start_ts = datetime.now(timezone.utc)
if not run_date:
    run_date = _start_ts.strftime("%Y-%m-%d")

WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"  # TODO fill in after provisioning
SILVER_LH_ID = "00000000-0000-0000-0000-000000000000"  # TODO fill in with silver_lh's item id


def silver_table(name: str):
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LH_ID}/Tables/{name}"
    return spark.read.format("delta").load(path)


def write_gold(df, name: str):
    (df.withColumn("_gold_loaded_at", F.current_timestamp())
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name))


def sk(col_name: str):
    return F.xxhash64(F.col(col_name))


gold_counts: dict[str, int] = {}

# CELL ********************

# DimDate — standard calendar dimension, wide enough to cover the
# generation window with headroom.
dim_date = (
    spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS FullDate")
    .withColumn("DateKey", F.date_format("FullDate", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("FullDate"))
    .withColumn("Quarter", F.quarter("FullDate"))
    .withColumn("Month", F.month("FullDate"))
    .withColumn("MonthName", F.date_format("FullDate", "MMMM"))
    .withColumn("Day", F.dayofmonth("FullDate"))
    .withColumn("DayOfWeek", F.dayofweek("FullDate"))
    .withColumn("DayName", F.date_format("FullDate", "EEEE"))
    .withColumn("IsWeekend", F.dayofweek("FullDate").isin(1, 7))
)
write_gold(dim_date, "DimDate")
gold_counts["DimDate"] = dim_date.count()

# CELL ********************

transactions_silver = silver_table("transactions")
risk_silver = silver_table("transaction_risk")
merchants_silver = silver_table("merchants")
sessions_silver = silver_table("sessions")
devices_silver = silver_table("devices")
fraud_alerts_silver = silver_table("fraud_alerts")

# DimCustomer — union grain across the four customer-bearing Silver tables.
customer_ids = (
    transactions_silver.select(F.col("CustomerID"))
    .unionByName(sessions_silver.select(F.col("customerId").alias("CustomerID")))
    .unionByName(devices_silver.select(F.col("customerId").alias("CustomerID")))
    .unionByName(fraud_alerts_silver.select(F.col("customerId").alias("CustomerID")))
    .where(F.col("CustomerID").isNotNull())
    .distinct()
)
dim_customer = customer_ids.withColumn("CustomerSK", sk("CustomerID")).select("CustomerSK", "CustomerID")
write_gold(dim_customer, "DimCustomer")
gold_counts["DimCustomer"] = dim_customer.count()

# CELL ********************

# DimAccount — only Databricks transactions carries AccountID.
dim_account = (
    transactions_silver.select("AccountID", "CustomerID").distinct()
    .withColumn("AccountSK", sk("AccountID"))
    .withColumn("CustomerSK", sk("CustomerID"))
    .select("AccountSK", "AccountID", "CustomerID", "CustomerSK")
)
write_gold(dim_account, "DimAccount")
gold_counts["DimAccount"] = dim_account.count()

# CELL ********************

# DimMerchant — 1:1 from Silver merchants.
dim_merchant = (
    merchants_silver
    .withColumn("MerchantSK", sk("MerchantID"))
    .select("MerchantSK", "MerchantID", "MerchantName", "MerchantCategory", "City", "State",
            "Country", "MerchantRiskCategory")
)
write_gold(dim_merchant, "DimMerchant")
gold_counts["DimMerchant"] = dim_merchant.count()

# CELL ********************

# DimDevice — 1:1 from Silver devices. DeviceFingerprint is the sensitive
# column governed in warehouse/10_apply_security.sql and
# infra/governance/onelake_security.py (COLUMN_RESTRICTIONS references
# /Tables/dimdevice -> devicefingerprint, lowercase-metastore form of this
# column — keep this exact name).
dim_device = (
    devices_silver
    .withColumn("DeviceSK", sk("deviceId"))
    .withColumn("CustomerSK", sk("customerId"))
    .select(
        "DeviceSK",
        F.col("deviceId").alias("DeviceID"),
        F.col("customerId").alias("CustomerID"),
        "CustomerSK",
        F.col("deviceType").alias("DeviceType"),
        F.col("os").alias("OS"),
        F.col("deviceFingerprint").alias("DeviceFingerprint"),
        F.col("isTrusted").alias("IsTrusted"),
    )
)
write_gold(dim_device, "DimDevice")
gold_counts["DimDevice"] = dim_device.count()

# CELL ********************

# FactTransactions — LEFT join to transaction_risk (not inner): a
# transaction can be valid in Silver even if its own risk score got
# quarantined (e.g. an out-of-range RiskScore), so a plain inner join would
# silently drop rows and break the Silver-vs-Gold count reconciliation in
# nb_reconciliation.py. RiskScore/RiskBand are simply NULL in that case.
fact_transactions = (
    transactions_silver.alias("t")
    .join(risk_silver.select("TransactionID", "RiskScore", "RiskBand").alias("r"), "TransactionID", "left")
    .withColumn("CustomerSK", sk("t.CustomerID"))
    .withColumn("AccountSK", sk("t.AccountID"))
    .withColumn("MerchantSK", sk("t.MerchantID"))
    .withColumn("DateKey", F.date_format(F.col("t.TransactionTimestamp"), "yyyyMMdd").cast("int"))
    .select(
        "TransactionID", "CustomerSK", "AccountSK", "MerchantSK", "DateKey",
        F.col("t.TransactionTimestamp").alias("TransactionTimestamp"),
        F.col("t.Amount").alias("Amount"), F.col("t.Currency").alias("Currency"),
        F.col("t.TransactionType").alias("TransactionType"), F.col("t.Channel").alias("Channel"),
        F.col("t.Country").alias("Country"), F.col("t.DeviceID").alias("DeviceID"),
        F.col("t.CardPresent").alias("CardPresent"), F.col("t.TransactionStatus").alias("TransactionStatus"),
        "RiskScore", "RiskBand",
    )
)
write_gold(fact_transactions, "FactTransactions")
gold_counts["FactTransactions"] = fact_transactions.count()

# CELL ********************

# FactDigitalSessions — flattened columns from Silver's sessions table
# carry the device_/geo_/auth_ prefixes nb_silver_transform.py produced.
fact_sessions = (
    sessions_silver
    .withColumn("CustomerSK", sk("customerId"))
    .withColumn("DeviceSK", sk("deviceId"))
    .withColumn("DateKey", F.date_format(F.col("loginTimestamp"), "yyyyMMdd").cast("int"))
    .select(
        F.col("sessionId").alias("SessionID"), "CustomerSK",
        F.col("customerId").alias("CustomerID"), F.col("deviceId").alias("DeviceID"), "DeviceSK",
        "DateKey", F.col("loginTimestamp").alias("LoginTimestamp"),
        F.col("logoutTimestamp").alias("LogoutTimestamp"),
        F.col("device_deviceType").alias("DeviceType"), F.col("device_os").alias("DeviceOS"),
        F.col("device_browser").alias("DeviceBrowser"),
        F.col("geo_country").alias("GeoCountry"), F.col("geo_city").alias("GeoCity"),
        F.col("geo_ipAddress").alias("GeoIpAddress"),
        F.col("auth_method").alias("AuthMethod"), F.col("auth_mfaUsed").alias("AuthMfaUsed"),
        F.col("auth_success").alias("AuthSuccess"),
        F.col("activities").alias("ActivitiesJson"),
    )
)
write_gold(fact_sessions, "FactDigitalSessions")
gold_counts["FactDigitalSessions"] = fact_sessions.count()

# CELL ********************

# FactFraudAlerts — transactionId is nullable (not every alert references a
# specific transaction); when present it's the join key
# nb_gold_consumption_demo.py uses to prove the fraud-to-transaction
# cross-source query.
fact_fraud_alerts = (
    fraud_alerts_silver
    .withColumn("CustomerSK", sk("customerId"))
    .withColumn("DateKey", F.date_format(F.col("alertTimestamp"), "yyyyMMdd").cast("int"))
    .select(
        F.col("alertId").alias("AlertID"), "CustomerSK", F.col("customerId").alias("CustomerID"),
        F.col("transactionId").alias("TransactionID"), "DateKey",
        F.col("alertTimestamp").alias("AlertTimestamp"), F.col("severity").alias("Severity"),
        F.col("alertType").alias("AlertType"),
    )
)
write_gold(fact_fraud_alerts, "FactFraudAlerts")
gold_counts["FactFraudAlerts"] = fact_fraud_alerts.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# AggCustomerRiskProfile — the cross-source analytical outcome. Grain:
# CustomerID, union across the four fact sources (dim_customer already is
# that union). Independent source-relative 30-day watermarks: txn_as_of
# from Databricks-derived facts, session_as_of from Cosmos-derived facts —
# every Cosmos-side metric (FraudAlertCount, FailedLoginCount,
# DistinctDeviceCount, UntrustedDeviceCount, GeographicAnomalyCount) is
# windowed by session_as_of, since ARCHITECTURE.md attributes that anchor
# to "Cosmos-derived facts" generally, not to sessions alone. Devices
# themselves have no session-like activity timestamp of their own, so
# "device count" here means devices seen IN a session within the window
# (semantically the right thing for a 30-day risk profile, and it keeps
# every Cosmos metric on one consistent anchor).
txn_as_of_row = fact_transactions.agg(F.max(F.to_date("TransactionTimestamp"))).collect()[0]
txn_as_of = txn_as_of_row[0]
session_as_of_row = fact_sessions.agg(F.max(F.to_date("LoginTimestamp"))).collect()[0]
session_as_of = session_as_of_row[0]

txn_window_start = txn_as_of - timedelta(days=29) if txn_as_of else None
session_window_start = session_as_of - timedelta(days=29) if session_as_of else None

txn_windowed = fact_transactions.where(
    (F.to_date("TransactionTimestamp") >= F.lit(txn_window_start)) & (F.to_date("TransactionTimestamp") <= F.lit(txn_as_of))
) if txn_as_of else fact_transactions.limit(0)

session_windowed = fact_sessions.where(
    (F.to_date("LoginTimestamp") >= F.lit(session_window_start)) & (F.to_date("LoginTimestamp") <= F.lit(session_as_of))
) if session_as_of else fact_sessions.limit(0)

fraud_windowed = fact_fraud_alerts.where(
    (F.to_date("AlertTimestamp") >= F.lit(session_window_start)) & (F.to_date("AlertTimestamp") <= F.lit(session_as_of))
) if session_as_of else fact_fraud_alerts.limit(0)

txn_agg = txn_windowed.groupBy("CustomerID").agg(
    F.count("*").alias("TransactionCount30D"),
    F.sum("Amount").alias("TotalTransactionAmount30D"),
    F.avg("RiskScore").alias("AverageTransactionRiskScore"),
    F.sum(F.when(F.col("RiskBand").isin("High", "Critical"), 1).otherwise(0)).alias("HighRiskTransactionCount"),
)

fraud_agg = fraud_windowed.groupBy("CustomerID").agg(F.count("*").alias("FraudAlertCount"))

login_agg = session_windowed.groupBy("CustomerID").agg(
    F.sum(F.when(F.col("AuthSuccess") == False, 1).otherwise(0)).alias("FailedLoginCount"),  # noqa: E712
    F.countDistinct("DeviceID").alias("DistinctDeviceCount"),
)

untrusted_agg = (
    session_windowed.select("CustomerID", "DeviceID").distinct()
    .join(dim_device.select(F.col("DeviceID"), F.col("IsTrusted")), "DeviceID", "inner")
    .where(F.col("IsTrusted") == False)  # noqa: E712
    .groupBy("CustomerID")
    .agg(F.countDistinct("DeviceID").alias("UntrustedDeviceCount"))
)

# GeographicAnomalyCount — parses devices_silver.geoHistory[] (kept as a
# JSON string per ARCHITECTURE.md's Silver section) for devices actually
# used in a session within the window. Assumed entry shape: {country, city,
# observedAt, isAnomaly} — see the ASSUMED COSMOS DOCUMENT SHAPE note in
# nb_silver_transform.py; this is the corresponding Gold-side assumption.
GEO_HISTORY_SCHEMA = ArrayType(StructType([
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("observedAt", StringType()),
    StructField("isAnomaly", BooleanType()),
]))

devices_in_window = session_windowed.select("CustomerID", "DeviceID").distinct()
geo_anomaly_agg = (
    devices_in_window
    .join(devices_silver.select(F.col("deviceId").alias("DeviceID"), F.col("geoHistory")), "DeviceID", "inner")
    .withColumn("geo_entry", F.explode(F.from_json(F.col("geoHistory"), GEO_HISTORY_SCHEMA)))
    .where(
        F.col("geo_entry.isAnomaly")
        & (F.to_date("geo_entry.observedAt") >= F.lit(session_window_start))
        & (F.to_date("geo_entry.observedAt") <= F.lit(session_as_of))
    )
    .groupBy("CustomerID")
    .agg(F.count("*").alias("GeographicAnomalyCount"))
)

risk_profile = (
    dim_customer.select("CustomerID")
    .join(txn_agg, "CustomerID", "left")
    .join(fraud_agg, "CustomerID", "left")
    .join(login_agg, "CustomerID", "left")
    .join(untrusted_agg, "CustomerID", "left")
    .join(geo_anomaly_agg, "CustomerID", "left")
    .fillna({
        "TransactionCount30D": 0, "TotalTransactionAmount30D": 0.0, "HighRiskTransactionCount": 0,
        "FraudAlertCount": 0, "FailedLoginCount": 0, "DistinctDeviceCount": 0,
        "UntrustedDeviceCount": 0, "GeographicAnomalyCount": 0,
    })
)

# CustomerRiskScore — copied verbatim from ARCHITECTURE.md's Python
# expression, each factor capped before summing so no single signal
# dominates. Explainable synthetic heuristic, NOT a trained/calibrated
# model — not suitable for real risk decisions (see ARCHITECTURE.md).
risk_score_expr = F.round(
    F.least(
        F.lit(100.0),
        F.coalesce(F.col("AverageTransactionRiskScore"), F.lit(0.0)) * F.lit(0.45)
        + F.least(F.col("FraudAlertCount") * F.lit(12.0), F.lit(24.0))
        + F.least(F.col("FailedLoginCount") * F.lit(2.0), F.lit(12.0))
        + F.least(F.col("UntrustedDeviceCount") * F.lit(5.0), F.lit(10.0))
        + F.least(F.col("GeographicAnomalyCount") * F.lit(3.0), F.lit(9.0)),
    ),
    2,
)

risk_profile = (
    risk_profile
    .withColumn("CustomerRiskScore", risk_score_expr)
    .withColumn(
        "CustomerRiskBand",
        F.when(F.col("CustomerRiskScore") >= 80, "High")
        .when(F.col("CustomerRiskScore") >= 45, "Medium")
        .otherwise("Low"),
    )
    .select(
        "CustomerID", "TransactionCount30D", "TotalTransactionAmount30D", "AverageTransactionRiskScore",
        "HighRiskTransactionCount", "FraudAlertCount", "FailedLoginCount", "DistinctDeviceCount",
        "UntrustedDeviceCount", "GeographicAnomalyCount", "CustomerRiskScore", "CustomerRiskBand",
    )
)
write_gold(risk_profile, "AggCustomerRiskProfile")
gold_counts["AggCustomerRiskProfile"] = risk_profile.count()

for name, n in gold_counts.items():
    print(f"{name}: {n}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils  # noqa: E402

rows_read = (
    transactions_silver.count() + risk_silver.count() + merchants_silver.count()
    + sessions_silver.count() + devices_silver.count() + fraud_alerts_silver.count()
)
rows_written = sum(gold_counts.values())

notebookutils.notebook.run(
    "pipeline_log",
    180,
    {
        "run_id": run_id,
        "stage": "gold_build",
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
