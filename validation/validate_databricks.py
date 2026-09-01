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


def fetch_df_via_sql(ws, warehouse_id: str, query: str) -> pd.DataFrame:
    from databricks.sdk.service.sql import StatementState

    resp = ws.statement_execution.execute_statement(statement=query, warehouse_id=warehouse_id, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = ws.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise ValidationError(f"Query failed ({resp.status.state}): {query!r} -> {resp.status.error}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return pd.DataFrame(rows, columns=cols)


def run_remote(workspace_host: str, workspace_resource_id: str, catalog: str, schema: str) -> list[str]:
    logger.info("Running in REMOTE mode against %s catalog=%s schema=%s", workspace_host, catalog, schema)
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient(host=workspace_host, azure_workspace_resource_id=workspace_resource_id, auth_type="azure-cli")
    warehouse_id = ensure_warehouse(ws, workspace_resource_id)
    try:
        transactions = fetch_df_via_sql(ws, warehouse_id, f"SELECT * FROM {catalog}.{schema}.transactions")
        risk_scores = fetch_df_via_sql(ws, warehouse_id, f"SELECT * FROM {catalog}.{schema}.transaction_risk_scores")
        merchants = fetch_df_via_sql(ws, warehouse_id, f"SELECT * FROM {catalog}.{schema}.merchants")
    finally:
        logger.info("Stopping validation warehouse %s to avoid idle spend.", warehouse_id)
        ws.warehouses.stop(warehouse_id)

    # Statement Execution API returns everything as strings; coerce the columns
    # validate_frames() treats numerically/logically.
    transactions["Amount"] = pd.to_numeric(transactions["Amount"], errors="coerce")
    risk_scores["RiskScore"] = pd.to_numeric(risk_scores["RiskScore"], errors="coerce")

    logger.info(
        "Fetched from Unity Catalog: transactions=%d transaction_risk_scores=%d merchants=%d",
        len(transactions), len(risk_scores), len(merchants),
    )
    return validate_frames(transactions, risk_scores, merchants)


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
