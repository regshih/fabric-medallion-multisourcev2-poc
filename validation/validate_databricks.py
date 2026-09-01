#!/usr/bin/env python3
"""Validate the Databricks-source synthetic banking/fraud data.

Two modes:

  --mode local  (default, always available)
      Validates the locally generated Parquet under --data-dir. No Azure
      credentials or workspace access required.

  --mode remote
      Connects to the deployed Databricks workspace's Unity Catalog and
      validates the actual managed Delta tables (catalog/schema/table
      existence, row counts, null business keys, RiskScore range, Amount
      sanity). Requires --workspace-host, --workspace-resource-id,
      --catalog, --schema. Auth is AAD via `az login` (the SDK's
      "azure-cli" auth type). Uses only `databricks-sdk` (already a project
      dependency) via the SQL Statement Execution API against a small
      serverless SQL warehouse that this script creates on first use (name
      "poc-validation-xs", 2X-Small, 10-minute auto-stop) and reuses on
      later runs; the warehouse is explicitly stopped after each run to
      avoid idle spend.

      All remote checks run as server-side aggregate SQL (COUNT/MIN/MAX/SUM
      over the whole table), not `SELECT *` pulled into pandas: at this
      table's scale (750k+ rows), `SELECT *` exceeds the Statement Execution
      API's 25 MB INLINE result limit (BAD_REQUEST "Inline byte limit
      exceeded"). Aggregates return one row regardless of table size.

      Windows/Git Bash note: MSYS auto-converts leading-`/` CLI arguments
      (like --workspace-resource-id /subscriptions/...) into bogus Windows
      paths, which Databricks then rejects as "Invalid resource ID". Run
      this from PowerShell, or set MSYS_NO_PATHCONV=1 in Git Bash first.

Checks performed (both modes, where applicable):
  - row counts for transactions / transaction_risk_scores / merchants
  - no null business keys (TransactionID, CustomerID, AccountID, MerchantID,
    DeviceID / MerchantID for merchants)
  - RiskScore in [0, 100]
  - Amount > 0 and finite
  - every TransactionID in transaction_risk_scores exists in transactions
  - every MerchantID referenced by transactions exists in merchants

Exits non-zero if any check fails.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("validate_databricks")


class ValidationError(Exception):
    pass


def check(condition: bool, message: str, failures: list[str]) -> None:
    if condition:
        logger.info("PASS: %s", message)
    else:
        logger.error("FAIL: %s", message)
        failures.append(message)


def validate_frames(transactions: pd.DataFrame, risk_scores: pd.DataFrame, merchants: pd.DataFrame) -> list[str]:
    failures: list[str] = []

    check(len(transactions) > 0, "transactions has rows", failures)
    check(len(risk_scores) > 0, "transaction_risk_scores has rows", failures)
    check(len(merchants) > 0, "merchants has rows", failures)

    for col in ("TransactionID", "CustomerID", "AccountID", "MerchantID", "DeviceID"):
        check(transactions[col].notna().all(), f"transactions.{col} has no nulls", failures)
    check(risk_scores["TransactionID"].notna().all(), "transaction_risk_scores.TransactionID has no nulls", failures)
    check(merchants["MerchantID"].notna().all(), "merchants.MerchantID has no nulls", failures)

    check(transactions["TransactionID"].is_unique, "transactions.TransactionID is unique", failures)
    check(merchants["MerchantID"].is_unique, "merchants.MerchantID is unique", failures)
    check(risk_scores["TransactionID"].is_unique, "transaction_risk_scores.TransactionID is unique", failures)

    check(
        risk_scores["RiskScore"].between(0, 100).all(),
        "RiskScore is within [0, 100] for all rows",
        failures,
    )
    check(
        risk_scores["RiskBand"].isin(["Low", "Medium", "High", "Critical"]).all(),
        "RiskBand values are within the expected set",
        failures,
    )

    amounts = pd.to_numeric(transactions["Amount"], errors="coerce")
    check(amounts.notna().all(), "Amount parses as numeric for all rows", failures)
    check((amounts > 0).all(), "Amount is positive for all rows", failures)
    check(amounts.lt(1_000_000).all(), "Amount has no implausible outliers (< 1,000,000)", failures)

    risk_txn_ids = set(risk_scores["TransactionID"])
    txn_ids = set(transactions["TransactionID"])
    orphan_risk = risk_txn_ids - txn_ids
    check(len(orphan_risk) == 0, f"every risk score TransactionID exists in transactions (orphans={len(orphan_risk)})", failures)

    merchant_ids = set(merchants["MerchantID"])
    txn_merchant_ids = set(transactions["MerchantID"])
    unknown_merchants = txn_merchant_ids - merchant_ids
    check(len(unknown_merchants) == 0, f"every transaction MerchantID exists in merchants (unknown={len(unknown_merchants)})", failures)

    for col, prefix in (("CustomerID", "CUST-"), ("AccountID", "ACCT"), ("DeviceID", "DEV-")):
        bad = (~transactions[col].astype(str).str.startswith(prefix)).sum()
        check(bad == 0, f"transactions.{col} follows the {prefix!r} business-key convention (violations={bad})", failures)
    bad_merchant = (~transactions["MerchantID"].astype(str).str.startswith("MER")).sum()
    check(bad_merchant == 0, f"transactions.MerchantID follows the 'MER' business-key convention (violations={bad_merchant})", failures)
    bad_txn = (~transactions["TransactionID"].astype(str).str.startswith("TXN-")).sum()
    check(bad_txn == 0, f"transactions.TransactionID follows the 'TXN-' business-key convention (violations={bad_txn})", failures)

    return failures


def run_local(data_dir: Path) -> list[str]:
    logger.info("Running in LOCAL mode against Parquet files under %s", data_dir)
    transactions = pd.read_parquet(data_dir / "transactions.parquet")
    risk_scores = pd.read_parquet(data_dir / "transaction_risk_scores.parquet")
    merchants = pd.read_parquet(data_dir / "merchants.parquet")
    logger.info(
        "Loaded: transactions=%d transaction_risk_scores=%d merchants=%d",
        len(transactions), len(risk_scores), len(merchants),
    )
    return validate_frames(transactions, risk_scores, merchants)


WAREHOUSE_NAME = "poc-validation-xs"


def ensure_warehouse(ws, resource_id: str) -> str:
    """Reuse a small serverless SQL warehouse for ad-hoc validation queries if
    one already exists under WAREHOUSE_NAME, else create one. Serverless +
    a short auto_stop_mins keeps this cheap; the warehouse is also explicitly
    stopped after use (see run_remote's finally block)."""
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

    for wh in ws.warehouses.list():
        if wh.name == WAREHOUSE_NAME:
            logger.info("Reusing existing SQL warehouse %r (id=%s).", WAREHOUSE_NAME, wh.id)
            return wh.id
    logger.info("Creating a small serverless SQL warehouse %r for validation queries.", WAREHOUSE_NAME)
    waiter = ws.warehouses.create(
        name=WAREHOUSE_NAME,
        cluster_size="2X-Small",
        enable_serverless_compute=True,
        auto_stop_mins=10,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        max_num_clusters=1,
    )
    warehouse = waiter.result()
    return warehouse.id


def run_one_row_query(ws, warehouse_id: str, query: str) -> dict:
    """Execute a query expected to return exactly one row and return it as a
    dict of column name -> value (still strings; caller coerces as needed).
    Used instead of pulling full tables into pandas: at 750k+ rows, a
    `SELECT *` blows past the Statement Execution API's 25 MB INLINE result
    limit (BAD_REQUEST: "Inline byte limit exceeded ... disposition=INLINE
    can have a result size of at most 26214400 bytes"). All validation here
    is therefore done as server-side aggregate SQL (COUNT/MIN/MAX/SUM), which
    is also just a more sensible way to validate hundreds of thousands of
    rows than shipping them all to the client."""
    from databricks.sdk.service.sql import StatementState

    resp = ws.statement_execution.execute_statement(statement=query, warehouse_id=warehouse_id, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = ws.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise ValidationError(f"Query failed ({resp.status.state}): {query!r} -> {resp.status.error}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    if len(rows) != 1:
        raise ValidationError(f"Expected exactly one row from aggregate query, got {len(rows)}: {query!r}")
    return dict(zip(cols, rows[0]))


def validate_remote_aggregates(ws, warehouse_id: str, catalog: str, schema: str) -> list[str]:
    failures: list[str] = []
    t = f"{catalog}.{schema}.transactions"
    r = f"{catalog}.{schema}.transaction_risk_scores"
    m = f"{catalog}.{schema}.merchants"

    counts = run_one_row_query(
        ws, warehouse_id,
        f"SELECT (SELECT COUNT(*) FROM {t}) AS txn_count, "
        f"(SELECT COUNT(*) FROM {r}) AS risk_count, "
        f"(SELECT COUNT(*) FROM {m}) AS merchant_count",
    )
    txn_count, risk_count, merchant_count = int(counts["txn_count"]), int(counts["risk_count"]), int(counts["merchant_count"])
    logger.info("Row counts: transactions=%d transaction_risk_scores=%d merchants=%d", txn_count, risk_count, merchant_count)
    check(txn_count > 0, "transactions has rows", failures)
    check(risk_count > 0, "transaction_risk_scores has rows", failures)
    check(merchant_count > 0, "merchants has rows", failures)

    txn = run_one_row_query(
        ws, warehouse_id,
        f"""SELECT
            SUM(CASE WHEN TransactionID IS NULL THEN 1 ELSE 0 END) AS null_txn_id,
            SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) AS null_cust_id,
            SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END) AS null_acct_id,
            SUM(CASE WHEN MerchantID IS NULL THEN 1 ELSE 0 END) AS null_merch_id,
            SUM(CASE WHEN DeviceID IS NULL THEN 1 ELSE 0 END) AS null_device_id,
            COUNT(*) AS total,
            COUNT(DISTINCT TransactionID) AS distinct_txn_id,
            SUM(CASE WHEN TransactionID NOT LIKE 'TXN-%' THEN 1 ELSE 0 END) AS bad_txn_format,
            SUM(CASE WHEN CustomerID NOT LIKE 'CUST-%' THEN 1 ELSE 0 END) AS bad_cust_format,
            SUM(CASE WHEN AccountID NOT LIKE 'ACCT%' THEN 1 ELSE 0 END) AS bad_acct_format,
            SUM(CASE WHEN DeviceID NOT LIKE 'DEV-%' THEN 1 ELSE 0 END) AS bad_device_format,
            SUM(CASE WHEN MerchantID NOT LIKE 'MER%' THEN 1 ELSE 0 END) AS bad_merch_format,
            MIN(Amount) AS min_amount, MAX(Amount) AS max_amount,
            SUM(CASE WHEN Amount IS NULL OR Amount <= 0 THEN 1 ELSE 0 END) AS non_positive_amount,
            SUM(CASE WHEN Amount >= 1000000 THEN 1 ELSE 0 END) AS outlier_amount
        FROM {t}""",
    )
    for key, label in (
        ("null_txn_id", "TransactionID"), ("null_cust_id", "CustomerID"), ("null_acct_id", "AccountID"),
        ("null_merch_id", "MerchantID"), ("null_device_id", "DeviceID"),
    ):
        check(int(txn[key]) == 0, f"transactions.{label} has no nulls", failures)
    check(int(txn["distinct_txn_id"]) == int(txn["total"]), f"transactions.TransactionID is unique ({txn['distinct_txn_id']} distinct / {txn['total']} total)", failures)
    for key, label, prefix in (
        ("bad_txn_format", "TransactionID", "TXN-"), ("bad_cust_format", "CustomerID", "CUST-"),
        ("bad_acct_format", "AccountID", "ACCT"), ("bad_device_format", "DeviceID", "DEV-"),
        ("bad_merch_format", "MerchantID", "MER"),
    ):
        check(int(txn[key]) == 0, f"transactions.{label} follows the {prefix!r} business-key convention (violations={txn[key]})", failures)
    check(int(txn["non_positive_amount"]) == 0, f"Amount is positive for all rows (violations={txn['non_positive_amount']})", failures)
    check(int(txn["outlier_amount"]) == 0, f"Amount has no implausible outliers >= 1,000,000 (violations={txn['outlier_amount']})", failures)
    logger.info("Amount range: [%s, %s]", txn["min_amount"], txn["max_amount"])

    risk = run_one_row_query(
        ws, warehouse_id,
        f"""SELECT
            SUM(CASE WHEN TransactionID IS NULL THEN 1 ELSE 0 END) AS null_txn_id,
            COUNT(*) AS total,
            COUNT(DISTINCT TransactionID) AS distinct_txn_id,
            MIN(RiskScore) AS min_score, MAX(RiskScore) AS max_score,
            SUM(CASE WHEN RiskScore IS NULL OR RiskScore < 0 OR RiskScore > 100 THEN 1 ELSE 0 END) AS out_of_range,
            SUM(CASE WHEN RiskBand NOT IN ('Low', 'Medium', 'High', 'Critical') THEN 1 ELSE 0 END) AS bad_band
        FROM {r}""",
    )
    check(int(risk["null_txn_id"]) == 0, "transaction_risk_scores.TransactionID has no nulls", failures)
    check(int(risk["distinct_txn_id"]) == int(risk["total"]), f"transaction_risk_scores.TransactionID is unique ({risk['distinct_txn_id']} distinct / {risk['total']} total)", failures)
    check(int(risk["out_of_range"]) == 0, f"RiskScore is within [0, 100] for all rows (violations={risk['out_of_range']})", failures)
    check(int(risk["bad_band"]) == 0, f"RiskBand values are within the expected set (violations={risk['bad_band']})", failures)
    logger.info("RiskScore range: [%s, %s]", risk["min_score"], risk["max_score"])

    merch = run_one_row_query(
        ws, warehouse_id,
        f"""SELECT
            SUM(CASE WHEN MerchantID IS NULL THEN 1 ELSE 0 END) AS null_id,
            COUNT(*) AS total,
            COUNT(DISTINCT MerchantID) AS distinct_id
        FROM {m}""",
    )
    check(int(merch["null_id"]) == 0, "merchants.MerchantID has no nulls", failures)
    check(int(merch["distinct_id"]) == int(merch["total"]), f"merchants.MerchantID is unique ({merch['distinct_id']} distinct / {merch['total']} total)", failures)

    orphans = run_one_row_query(
        ws, warehouse_id,
        f"""SELECT
            (SELECT COUNT(*) FROM {r} r LEFT JOIN {t} t ON r.TransactionID = t.TransactionID WHERE t.TransactionID IS NULL) AS orphan_risk,
            (SELECT COUNT(*) FROM {t} t LEFT JOIN {m} m ON t.MerchantID = m.MerchantID WHERE m.MerchantID IS NULL) AS unknown_merchant
        """,
    )
    check(int(orphans["orphan_risk"]) == 0, f"every risk score TransactionID exists in transactions (orphans={orphans['orphan_risk']})", failures)
    check(int(orphans["unknown_merchant"]) == 0, f"every transaction MerchantID exists in merchants (unknown={orphans['unknown_merchant']})", failures)

    return failures


def run_remote(workspace_host: str, workspace_resource_id: str, catalog: str, schema: str) -> list[str]:
    logger.info("Running in REMOTE mode against %s catalog=%s schema=%s", workspace_host, catalog, schema)
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient(host=workspace_host, azure_workspace_resource_id=workspace_resource_id, auth_type="azure-cli")
    warehouse_id = ensure_warehouse(ws, workspace_resource_id)
    try:
        return validate_remote_aggregates(ws, warehouse_id, catalog, schema)
    finally:
        logger.info("Stopping validation warehouse %s to avoid idle spend.", warehouse_id)
        ws.warehouses.stop(warehouse_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("local", "remote"), default="local")
    parser.add_argument("--data-dir", type=Path, default=Path("./data/databricks"), help="Local mode: Parquet directory")
    parser.add_argument("--workspace-host", default=None, help="Remote mode: https://adb-....azuredatabricks.net")
    parser.add_argument("--workspace-resource-id", default=None, help="Remote mode: ARM resource ID of the Databricks workspace")
    parser.add_argument("--catalog", default=None, help="Remote mode: Unity Catalog catalog name")
    parser.add_argument("--schema", default=None, help="Remote mode: schema name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.mode == "local":
        failures = run_local(args.data_dir)
    else:
        missing = [
            name for name, val in (
                ("--workspace-host", args.workspace_host),
                ("--workspace-resource-id", args.workspace_resource_id),
                ("--catalog", args.catalog),
                ("--schema", args.schema),
            ) if not val
        ]
        if missing:
            raise ValueError(f"--mode remote requires: {', '.join(missing)}")
        failures = run_remote(args.workspace_host, args.workspace_resource_id, args.catalog, args.schema)

    if failures:
        logger.error("Validation FAILED with %d issue(s).", len(failures))
        return 1
    logger.info("Validation PASSED — all checks green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
