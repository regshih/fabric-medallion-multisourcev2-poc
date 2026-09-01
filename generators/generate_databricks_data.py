#!/usr/bin/env python3
"""Generate synthetic Databricks-source banking/fraud data as local Parquet.

Produces three fictional, clearly-synthetic datasets under ``--output-dir``
(default ``./data/databricks/``):

- ``transactions.parquet``       — card/account transactions
- ``transaction_risk_scores.parquet`` — one risk score per transaction
- ``merchants.parquet``          — merchant reference data

Cross-source business-key convention (must match the Cosmos DB generator run
in parallel elsewhere in this repo, so the two sources can be joined):

    CustomerID  = "CUST-" + 6-digit zero-padded number   e.g. CUST-000042
    AccountID   = "ACCT"  + 9-digit zero-padded number    e.g. ACCT000000123
    DeviceID    = "DEV-"  + 6-digit zero-padded number    e.g. DEV-000042
    MerchantID  = "MER"   + 6-digit zero-padded number    e.g. MER000042
    TransactionID = "TXN-" + 9-digit zero-padded number   e.g. TXN-000000001

All values are generated with ``Faker`` seeded deterministically via
``--seed`` so re-runs with the same arguments reproduce identical data. All
data is entirely fictional synthetic test data (no real people, no real
institutions) — safe for a public repository.

Usage:
    python generators/generate_databricks_data.py --help
    python generators/generate_databricks_data.py --rows 750000 --seed 42
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("generate_databricks_data")

DEFAULT_ROWS = 750_000
DEFAULT_MERCHANTS = 1_500
DEFAULT_CUSTOMERS = 50_000
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW_SECONDS = 60 * 60 * 24 * 30  # 30-day generation window

CURRENCIES = ("USD", "USD", "USD", "CAD", "EUR", "GBP")
TRANSACTION_TYPES = ("Purchase", "Purchase", "Purchase", "Refund", "Transfer", "ATM Withdrawal")
MERCHANT_CATEGORIES = (
    "Grocery", "Dining", "Travel", "Fuel", "Retail",
    "Healthcare", "Digital Goods", "Entertainment", "Utilities",
)
CHANNELS = ("Mobile", "Web", "POS", "ATM", "Phone")
COUNTRIES = ("US", "US", "US", "US", "CA", "GB", "DE", "AU")
STATUSES = ("Approved", "Approved", "Approved", "Approved", "Declined", "Pending", "Reversed")
RISK_BANDS_ORDER = ("Low", "Medium", "High", "Critical")


def cust_id(n: int) -> str:
    return f"CUST-{n:06d}"


def acct_id(n: int) -> str:
    return f"ACCT{n:09d}"


def dev_id(n: int) -> str:
    return f"DEV-{n:06d}"


def merch_id(n: int) -> str:
    return f"MER{n:06d}"


def txn_id(n: int) -> str:
    return f"TXN-{n:09d}"


@dataclass
class GenerationParams:
    rows: int
    customers: int
    merchants: int
    seed: int


def generate_merchants(fake: Faker, count: int) -> pd.DataFrame:
    records = []
    for i in range(1, count + 1):
        category = fake.random_element(MERCHANT_CATEGORIES)
        risk_roll = fake.random_int(0, 99)
        risk_category = "High" if risk_roll >= 92 else "Medium" if risk_roll >= 65 else "Low"
        records.append(
            {
                "MerchantID": merch_id(i),
                "MerchantName": f"{fake.company()} {fake.company_suffix()}",
                "MerchantCategory": category,
                "City": fake.city(),
                "State": fake.state_abbr(),
                "Country": fake.random_element(COUNTRIES),
                "MerchantRiskCategory": risk_category,
            }
        )
    return pd.DataFrame.from_records(records)


def generate_transactions(fake: Faker, rows: int, customers: int, merchants: int) -> pd.DataFrame:
    records = []
    for i in range(1, rows + 1):
        customer_num = fake.random_int(1, customers)
        # Each customer typically has 1-2 accounts and 1-3 devices; derive
        # deterministically-ish from the customer number plus a small jitter
        # so the same customer's transactions cluster on a small set of
        # accounts/devices (more realistic than one random account per txn).
        account_jitter = fake.random_int(0, 1)
        device_jitter = fake.random_int(0, 2)
        account_num = customer_num * 2 - account_jitter
        device_num = customer_num * 3 - device_jitter
        merchant_num = fake.random_int(1, merchants)
        offset_seconds = fake.random_int(0, WINDOW_SECONDS)
        timestamp = BASE_TIME + timedelta(seconds=offset_seconds)
        amount = round(fake.pyfloat(min_value=1.00, max_value=2500.00, right_digits=2), 2)

        records.append(
            {
                "TransactionID": txn_id(i),
                "AccountID": acct_id(account_num),
                "CustomerID": cust_id(customer_num),
                "MerchantID": merch_id(merchant_num),
                "TransactionTimestamp": timestamp,
                "Amount": amount,
                "Currency": fake.random_element(CURRENCIES),
                "TransactionType": fake.random_element(TRANSACTION_TYPES),
                "MerchantCategory": fake.random_element(MERCHANT_CATEGORIES),
                "Channel": fake.random_element(CHANNELS),
                "Country": fake.random_element(COUNTRIES),
                "DeviceID": dev_id(device_num),
                "CardPresent": fake.boolean(chance_of_getting_true=55),
                "TransactionStatus": fake.random_element(STATUSES),
            }
        )
        if i % 100_000 == 0:
            logger.info("Generated %d/%d transactions...", i, rows)
    return pd.DataFrame.from_records(records)


def generate_risk_scores(fake: Faker, transactions: pd.DataFrame) -> pd.DataFrame:
    """One risk score per transaction (full coverage, not a subset — simplest
    to generate and simplest to validate 1:1 join coverage against)."""
    records = []
    factor_pool = (
        "velocity", "merchant_risk", "device_novelty", "geo_mismatch",
        "amount_outlier", "new_account", "card_not_present",
    )
    for row in transactions.itertuples(index=False):
        score = round(fake.pyfloat(min_value=0.0, max_value=100.0, right_digits=2), 2)
        if score >= 85:
            band = "Critical"
        elif score >= 60:
            band = "High"
        elif score >= 30:
            band = "Medium"
        else:
            band = "Low"
        n_factors = 0 if band == "Low" else fake.random_int(1, 3)
        factors = fake.random_elements(factor_pool, length=n_factors, unique=True) if n_factors else []
        scored_at = row.TransactionTimestamp + timedelta(seconds=fake.random_int(1, 45))
        records.append(
            {
                "TransactionID": row.TransactionID,
                "RiskScore": score,
                "RiskBand": band,
                "ModelVersion": "synthetic-risk-v1",
                "ScoredTimestamp": scored_at,
                "RiskFactors": ",".join(factors),
            }
        )
    return pd.DataFrame.from_records(records)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # pandas Timestamp columns default to nanosecond precision, which pyarrow
    # writes as Parquet's INT64 TIMESTAMP(NANOS) — Spark's Parquet reader
    # rejects that outright (PARQUET_TYPE_ILLEGAL), it only reads MICROS/
    # MILLIS. Downcast every datetime column to microsecond precision before
    # writing so Spark can read it with no special config.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            tz = getattr(df[col].dtype, "tz", None)
            df[col] = df[col].astype(f"datetime64[us, {tz}]" if tz is not None else "datetime64[us]")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)
    logger.info("Wrote %d rows -> %s", len(df), path)


def generate(params: GenerationParams, output_dir: Path) -> dict[str, int]:
    fake = Faker()
    Faker.seed(params.seed)

    logger.info(
        "Generating synthetic data: rows=%d customers=%d merchants=%d seed=%d",
        params.rows, params.customers, params.merchants, params.seed,
    )

    merchants = generate_merchants(fake, params.merchants)
    write_parquet(merchants, output_dir / "merchants.parquet")

    transactions = generate_transactions(fake, params.rows, params.customers, params.merchants)
    write_parquet(transactions, output_dir / "transactions.parquet")

    risk_scores = generate_risk_scores(fake, transactions)
    write_parquet(risk_scores, output_dir / "transaction_risk_scores.parquet")

    return {
        "transactions": len(transactions),
        "transaction_risk_scores": len(risk_scores),
        "merchants": len(merchants),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help=f"Transaction row count (default {DEFAULT_ROWS})")
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS, help=f"Distinct customers (default {DEFAULT_CUSTOMERS})")
    parser.add_argument("--merchants", type=int, default=DEFAULT_MERCHANTS, help=f"Distinct merchants (default {DEFAULT_MERCHANTS})")
    parser.add_argument("--seed", type=int, default=42, help="Faker seed for deterministic output (default 42)")
    parser.add_argument("--output-dir", type=Path, default=Path("./data/databricks"), help="Output directory (default ./data/databricks)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.rows <= 0 or args.customers <= 0 or args.merchants <= 0:
        raise ValueError("--rows, --customers, and --merchants must all be positive")
    counts = generate(
        GenerationParams(rows=args.rows, customers=args.customers, merchants=args.merchants, seed=args.seed),
        args.output_dir,
    )
    logger.info("Done. Row counts: %s", counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
