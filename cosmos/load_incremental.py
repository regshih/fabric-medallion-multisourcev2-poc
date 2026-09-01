#!/usr/bin/env python3
"""Apply the deterministic incremental Cosmos DB batch (proves change propagation):
one new session, one device flipping trusted True->False, one new fraud alert,
and one existing alert's status changing.

Idempotent (upsert-based) — safe to rerun. Same networking/auth constraints as
cosmos/load_initial.py: run from inside the account's VNet, Entra ID auth only.

Usage:
    python cosmos/load_incremental.py --help
    python cosmos/load_incremental.py --data-dir data/cosmos/incremental
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
    parser.add_argument("--data-dir", type=Path, default=Path("data/cosmos/incremental"))
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
