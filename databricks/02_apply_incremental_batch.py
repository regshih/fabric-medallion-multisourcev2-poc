# Databricks notebook source
"""Apply a small, deterministic incremental batch on top of the seeded Delta
tables, to demonstrate change propagation later (Fabric mirroring, CDF,
etc.). Keeps things simple per this POC's philosophy: a handful of new
transactions, two changed risk scores, and one new merchant, applied via
Delta MERGE so the script is safely re-runnable (same batch_id -> same
resulting state).

Run this only after 01_seed_delta_tables.py has succeeded.
"""

# COMMAND ----------
import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("catalog", "dbw_fmv2poc_915d", "Unity Catalog catalog")
dbutils.widgets.text("schema", "banking", "Schema")
dbutils.widgets.text("base_row_count", "750000", "Initial transaction row count from the seed job")
dbutils.widgets.text("new_transaction_count", "25", "New transactions to add")
dbutils.widgets.text("batch_id", "incremental_001", "Stable batch id (label only)")

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,254}$")


def ident(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return f"`{value}`"


catalog_raw, schema_raw = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
catalog, schema = ident(catalog_raw), ident(schema_raw)
base_count = int(dbutils.widgets.get("base_row_count"))
new_count = int(dbutils.widgets.get("new_transaction_count"))
batch_id = dbutils.widgets.get("batch_id")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", batch_id):
    raise ValueError("batch_id must contain only letters, numbers, underscore, or hyphen")
if base_count < 1 or new_count < 1:
    raise ValueError("base_row_count and new_transaction_count must be positive")


def table(name: str) -> str:
    return f"{catalog}.{schema}.{ident(name)}"


required = ("transactions", "transaction_risk_scores", "merchants")
missing = [name for name in required if not spark.catalog.tableExists(f"{catalog_raw}.{schema_raw}.{name}")]
if missing:
    raise RuntimeError(f"Seed tables first (01_seed_delta_tables.py); missing: {missing}")

# COMMAND ----------
# New transactions: deterministic pseudo-random values derived from the row
# ordinal via xxhash64, matching the CUST-/ACCT/DEV-/MER/TXN- key format
# used by generators/generate_databricks_data.py.
ordinal = spark.range(base_count, base_count + new_count).withColumnRenamed("id", "Ordinal")
customer_num = F.pmod(F.xxhash64(F.lit("customer"), "Ordinal"), F.lit(50_000)) + 1
merchant_num = F.pmod(F.xxhash64(F.lit("merchant"), "Ordinal"), F.lit(1_500)) + 1
account_num = customer_num * 2 - F.pmod(F.xxhash64(F.lit("account"), "Ordinal"), F.lit(2))
device_num = customer_num * 3 - F.pmod(F.xxhash64(F.lit("device"), "Ordinal"), F.lit(3))

new_transactions = ordinal.select(
    F.format_string("TXN-%09d", F.col("Ordinal") + 1).alias("TransactionID"),
    F.format_string("ACCT%09d", account_num).alias("AccountID"),
    F.format_string("CUST-%06d", customer_num).alias("CustomerID"),
    F.format_string("MER%06d", merchant_num).alias("MerchantID"),
    F.current_timestamp().alias("TransactionTimestamp"),
    ((F.pmod(F.xxhash64(F.lit("amount"), "Ordinal"), F.lit(249_900)) + 100) / 100).cast("double").alias("Amount"),
    F.lit("USD").alias("Currency"),
    F.lit("Purchase").alias("TransactionType"),
    F.lit("Digital Goods").alias("MerchantCategory"),
    F.lit("Mobile").alias("Channel"),
    F.lit("US").alias("Country"),
    F.format_string("DEV-%06d", device_num).alias("DeviceID"),
    F.lit(False).alias("CardPresent"),
    F.lit("Approved").alias("TransactionStatus"),
)

DeltaTable.forName(spark, table("transactions")).alias("t").merge(
    new_transactions.alias("s"), "t.TransactionID = s.TransactionID"
).whenNotMatchedInsertAll().execute()

new_risk_scores = new_transactions.select(
    "TransactionID",
    F.lit(72.50).cast("double").alias("RiskScore"),
    F.lit("High").alias("RiskBand"),
    F.lit("synthetic-risk-v1").alias("ModelVersion"),
    F.current_timestamp().alias("ScoredTimestamp"),
    F.lit("velocity,new_account").alias("RiskFactors"),
)
DeltaTable.forName(spark, table("transaction_risk_scores")).alias("t").merge(
    new_risk_scores.alias("s"), "t.TransactionID = s.TransactionID"
).whenNotMatchedInsertAll().execute()

# COMMAND ----------
# Two changed risk scores on existing transactions, chosen deterministically
# (the first two ordinals) so re-running this script converges to the same
# final state rather than drifting.
changed = spark.createDataFrame(
    [
        ("TXN-000000001", 91.25, "Critical", "synthetic-risk-v2", "controlled_score_change"),
        ("TXN-000000002", 88.00, "Critical", "synthetic-risk-v2", "controlled_score_change"),
    ],
    ["TransactionID", "RiskScore", "RiskBand", "ModelVersion", "RiskFactors"],
).withColumn("ScoredTimestamp", F.current_timestamp())

DeltaTable.forName(spark, table("transaction_risk_scores")).alias("t").merge(
    changed.alias("s"), "t.TransactionID = s.TransactionID"
).whenMatchedUpdateAll().execute()

# COMMAND ----------
# One new merchant.
new_merchant_id = "MER001501"
new_merchant = spark.createDataFrame(
    [(new_merchant_id, "Synthetic Incremental Merchant Co", "Digital Goods", "Springfield", "IL", "US", "Medium")],
    T.StructType(
        [
            T.StructField("MerchantID", T.StringType()),
            T.StructField("MerchantName", T.StringType()),
            T.StructField("MerchantCategory", T.StringType()),
            T.StructField("City", T.StringType()),
            T.StructField("State", T.StringType()),
            T.StructField("Country", T.StringType()),
            T.StructField("MerchantRiskCategory", T.StringType()),
        ]
    ),
)
DeltaTable.forName(spark, table("merchants")).alias("t").merge(
    new_merchant.alias("s"), "t.MerchantID = s.MerchantID"
).whenNotMatchedInsertAll().execute()

# COMMAND ----------
display(
    spark.sql(
        f"""
        SELECT 'transactions' AS object, count(*) AS rows FROM {table('transactions')}
        UNION ALL SELECT 'transaction_risk_scores', count(*) FROM {table('transaction_risk_scores')}
        UNION ALL SELECT 'merchants', count(*) FROM {table('merchants')}
        """
    )
)
