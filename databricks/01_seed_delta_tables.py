# Databricks notebook source
"""Seed Unity Catalog managed Delta tables from the Parquet files uploaded to
a Unity Catalog Volume by infra/databricks/upload_to_workspace.py.

Attach to (or run as a job against) a Unity Catalog-enabled cluster. Reads
``transactions.parquet``, ``transaction_risk_scores.parquet`` and
``merchants.parquet`` from the source volume and writes them as managed
Delta tables ``<catalog>.<schema>.transactions`` /
``transaction_risk_scores`` / ``merchants``, overwriting on each run so this
script is safely re-runnable. Change Data Feed is enabled on every table so
a later incremental batch (02_apply_incremental_batch.py) can be observed as
a genuine change stream, and so Fabric-side change propagation timing can
eventually be measured against a real CDC signal.

Uses only standard PySpark — no external dependencies beyond the Databricks
Runtime, and no secrets or storage credentials are referenced (managed
volume + managed tables only).
"""

# COMMAND ----------
import re

dbutils.widgets.text("catalog", "dbw_fmv2poc_915d", "Unity Catalog catalog")
dbutils.widgets.text("schema", "banking", "Schema")
dbutils.widgets.text("volume", "landing", "Source volume (holds uploaded Parquet)")

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,254}$")


def identifier(name: str) -> str:
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"Unsafe Unity Catalog identifier: {name!r}")
    return f"`{name}`"


catalog_raw = dbutils.widgets.get("catalog")
schema_raw = dbutils.widgets.get("schema")
volume_raw = dbutils.widgets.get("volume")
catalog, schema = identifier(catalog_raw), identifier(schema_raw)

volume_path = f"/Volumes/{catalog_raw}/{schema_raw}/{volume_raw}"
source_files = {
    "transactions": f"{volume_path}/transactions.parquet",
    "transaction_risk_scores": f"{volume_path}/transaction_risk_scores.parquet",
    "merchants": f"{volume_path}/merchants.parquet",
}

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def target(table: str) -> str:
    return f"{catalog}.{schema}.{identifier(table)}"


def seed_table(table: str, path: str) -> int:
    df = spark.read.format("parquet").load(path)
    name = target(table)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    return spark.table(name).count()

# COMMAND ----------
counts = {table: seed_table(table, path) for table, path in source_files.items()}

display(spark.createDataFrame(list(counts.items()), ["table", "row_count"]))
