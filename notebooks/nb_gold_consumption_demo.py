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

# CELL ********************

# Standalone, analyst-facing discovery notebook — no pipeline parameters,
# not part of pl_multisource_medallion, run interactively. Inventories
# Silver/Gold, then runs and PERSISTS (not just .show()/print — RunNotebook's
# job API has no way to retrieve stdout, confirmed in the sibling banking
# POC) at least two business queries proving the cross-source point of this
# whole repo.
from datetime import datetime, timezone

from pyspark.sql import functions as F

WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"  # TODO fill in
SILVER_LH_ID = "00000000-0000-0000-0000-000000000000"  # TODO fill in

_run_at = datetime.now(timezone.utc)


def silver_table(name: str):
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LH_ID}/Tables/{name}"
    return spark.read.format("delta").load(path)


SILVER_TABLES = [
    "transactions", "transaction_risk", "merchants", "sessions", "devices", "fraud_alerts",
    "quarantine_transactions", "quarantine_transaction_risk", "quarantine_merchants",
    "quarantine_sessions", "quarantine_devices", "quarantine_fraud_alerts",
]
GOLD_TABLES = [
    "dimcustomer", "dimaccount", "dimmerchant", "dimdevice", "dimdate",
    "facttransactions", "factdigitalsessions", "factfraudalerts", "aggcustomerriskprofile",
]

inventory_rows = []
for t in SILVER_TABLES:
    inventory_rows.append(("silver", t, silver_table(t).count()))
for t in GOLD_TABLES:
    inventory_rows.append(("gold", t, spark.table(t).count()))

inventory_df = (
    spark.createDataFrame(inventory_rows, ["layer", "table_name", "row_count"])
    .withColumn("_demo_run_at", F.lit(_run_at))
)
inventory_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("demo_table_inventory")
for layer, table_name, row_count in inventory_rows:
    print(f"{layer}.{table_name}: {row_count}")

# CELL ********************

# Business query 1: top-N customers by CustomerRiskScore, joined to
# DimCustomer — the cross-source proof point (AggCustomerRiskProfile blends
# Databricks transaction/risk signal with Cosmos session/device/fraud-alert
# signal into one number).
top_risk_customers = (
    spark.table("aggcustomerriskprofile").alias("arp")
    .join(spark.table("dimcustomer").alias("dc"), "CustomerID", "inner")
    .orderBy(F.col("CustomerRiskScore").desc())
    .limit(20)
    .select(
        "dc.CustomerSK", "arp.CustomerID", "arp.CustomerRiskScore", "arp.CustomerRiskBand",
        "arp.TransactionCount30D", "arp.TotalTransactionAmount30D", "arp.AverageTransactionRiskScore",
        "arp.FraudAlertCount", "arp.FailedLoginCount", "arp.UntrustedDeviceCount", "arp.GeographicAnomalyCount",
    )
    .withColumn("_demo_run_at", F.lit(_run_at))
)
top_risk_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("demo_top_risk_customers")
print(f"demo_top_risk_customers: {top_risk_customers.count()} rows")

# CELL ********************

# Business query 2: fraud-alert-to-transaction investigation — joins
# FactFraudAlerts to FactTransactions on TransactionID (and confirms via
# CustomerID that the alert and the transaction belong to the same
# customer). Demonstrates something genuinely impossible from either source
# alone: a Cosmos-native fraud alert record shown next to the exact
# Databricks-native transaction it references.
fraud_investigation = (
    spark.table("factfraudalerts").alias("fa")
    .join(
        spark.table("facttransactions").alias("ft"),
        (F.col("fa.TransactionID") == F.col("ft.TransactionID")) & (F.col("fa.CustomerSK") == F.col("ft.CustomerSK")),
        "inner",
    )
    .select(
        "fa.AlertID", "fa.CustomerID", "fa.Severity", "fa.AlertType", "fa.AlertTimestamp",
        "ft.TransactionID", "ft.Amount", "ft.TransactionType", "ft.Channel", "ft.TransactionTimestamp",
        "ft.RiskScore", "ft.RiskBand",
    )
    .withColumn("_demo_run_at", F.lit(_run_at))
)
fraud_investigation.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("demo_fraud_investigation")
print(f"demo_fraud_investigation: {fraud_investigation.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
