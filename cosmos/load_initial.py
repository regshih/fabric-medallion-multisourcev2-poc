#!/usr/bin/env python3
"""Bulk-load the generated initial Cosmos DB snapshot into the deployed account.

Idempotent (upsert-based) — safe to rerun. Requires network reachability to the
Cosmos account: with the account's steady-state ``publicNetworkAccess: Disabled``
(see infra/cosmos/main.bicep), that means running from inside the account's VNet
(see infra/cosmos/run_loader.ps1) rather than directly from an arbitrary dev
machine. Uses Entra ID auth only; no account key is ever read or accepted.

Usage:
    python cosmos/load_initial.py --help
    python cosmos/load_initial.py --data-dir data/cosmos/initial
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cosmos.common import DEFAULT_CONCURRENCY, DEFAULT_MAX_RETRIES, load_directory


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=Path("data/cosmos/initial"))
    parser.add_argument("--endpoint", help="Cosmos DB endpoint URI. Defaults to $COSMOS_ENDPOINT.")
    parser.add_argument("--database-name", help="Defaults to $COSMOS_DATABASE_NAME or 'multisource'.")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    args = parser.parse_args()
    result = load_directory(
        args.data_dir,
        endpoint=args.endpoint,
        database_name=args.database_name,
        concurrency=args.concurrency,
        max_retries=args.max_retries,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
