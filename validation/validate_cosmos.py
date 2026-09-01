#!/usr/bin/env python3
"""Validate the deployed Cosmos DB account against what the generators/loaders
are supposed to have produced. Connects via Entra ID only (no account key).

Checks, per container (digitalSessions, devices, fraudAlerts):
  - the container exists with partition key /customerId
  - the live document count matches the number of distinct document ids
    across every local data/cosmos/*/​<container>.jsonl batch (upserts dedupe
    by id, so this is the correct expected count regardless of batch overlap)
  - every sampled document has its required fields present
  - every customerId matches the CUST-NNNNNN convention, and every deviceId
    (in `devices` and in `digitalSessions.device.deviceId`) matches DEV-NNNNNN
  - the deliberate schema variation genuinely exists in the live data (some
    `devices` docs lack geoHistory, some `fraudAlerts` lack resolution) —
    checked against the live account, not just asserted from the generator

Logs total RU consumed by its own read queries (informational only; POC scale,
not optimized).

Same networking/auth constraints as cosmos/load_initial.py: with
publicNetworkAccess Disabled, run this from inside the account's VNet (see
infra/cosmos/run_loader.ps1).

Usage:
    python validation/validate_cosmos.py --help
    python validation/validate_cosmos.py --data-dir data/cosmos
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cosmos.common import CONTAINERS, read_jsonl, resolve_endpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("validate_cosmos")

CUST_ID_RE = re.compile(r"^CUST-\d{6}$")
DEV_ID_RE = re.compile(r"^DEV-\d{6}$")

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "digitalSessions": ("id", "sessionId", "customerId", "device", "loginTimestamp", "logoutTimestamp", "ipAddress", "geo", "authentication", "activities", "sessionRiskScore"),
    "devices": ("id", "deviceId", "customerId", "firstSeen", "lastSeen", "trusted", "deviceFingerprint", "operatingSystem", "appVersion", "riskSignals"),
    "fraudAlerts": ("id", "alertId", "customerId", "transactionId", "createdTimestamp", "alertType", "severity", "status", "signals", "investigatorNotes"),
}
# Fields that must show up on SOME but not ALL documents, proving the schema
# variation is real rather than just documented intent.
VARIABLE_FIELDS: dict[str, str] = {"devices": "geoHistory", "fraudAlerts": "resolution"}
SAMPLE_SIZE = 500


class ValidationError(RuntimeError):
    pass


def expected_ids(data_dir: Path, container: str) -> set[str]:
    ids: set[str] = set()
    for batch_dir in sorted(p for p in data_dir.glob("*") if p.is_dir()):
        path = batch_dir / f"{container}.jsonl"
        if path.exists():
            ids.update(doc["id"] for doc in read_jsonl(path))
    return ids


def _get_sync_client(endpoint: str):
    from azure.cosmos import CosmosClient
    from azure.identity import DefaultAzureCredential

    return CosmosClient(endpoint, credential=DefaultAzureCredential())


def validate_container(database: Any, container_name: str, expected: set[str], ru_tracker: list[float]) -> dict[str, Any]:
    container = database.get_container_client(container_name)

    def _record_ru(headers: dict[str, str], _result: Any) -> None:
        charge = headers.get("x-ms-request-charge")
        if charge is not None:
            ru_tracker.append(float(charge))

    props = container.read(response_hook=_record_ru)
    partition_key_paths = props["partitionKey"]["paths"]
    if partition_key_paths != ["/customerId"]:
        raise ValidationError(f"{container_name}: expected partition key /customerId, got {partition_key_paths}")

    live_docs = list(
        container.query_items(query="SELECT * FROM c", enable_cross_partition_query=True, response_hook=_record_ru)
    )
    live_ids = {doc["id"] for doc in live_docs}
    if len(live_docs) != len(live_ids):
        # Cosmos DB only enforces id uniqueness *within* a partition key value, not across
        # the whole container — so a set comparison alone can silently pass even when the
        # same id exists twice under two different customerId partitions (e.g. an
        # incremental "update" that guessed the wrong customerId for an existing document
        # and created a ghost instead of replacing it). Catch that explicitly.
        from collections import Counter

        dupes = [doc_id for doc_id, n in Counter(doc["id"] for doc in live_docs).items() if n > 1]
        raise ValidationError(
            f"{container_name}: {len(live_docs)} live documents but only {len(live_ids)} distinct ids — "
            f"the same id exists under more than one customerId partition (e.g. {sorted(dupes)[:5]})"
        )
    if live_ids != expected:
        missing = expected - live_ids
        unexpected = live_ids - expected
        raise ValidationError(
            f"{container_name}: live document set doesn't match expected. "
            f"missing={len(missing)} (e.g. {sorted(missing)[:5]}) unexpected={len(unexpected)} (e.g. {sorted(unexpected)[:5]})"
        )

    sample = live_docs[:SAMPLE_SIZE]
    required = REQUIRED_FIELDS[container_name]
    for doc in sample:
        missing_fields = [f for f in required if f not in doc]
        if missing_fields:
            raise ValidationError(f"{container_name}/{doc.get('id')}: missing required fields {missing_fields}")
        cust = doc.get("customerId", "")
        if not CUST_ID_RE.match(cust):
            raise ValidationError(f"{container_name}/{doc.get('id')}: customerId {cust!r} doesn't match CUST-NNNNNN")
        dev_id = doc.get("deviceId") or (doc.get("device") or {}).get("deviceId")
        if dev_id is not None and not DEV_ID_RE.match(dev_id):
            raise ValidationError(f"{container_name}/{doc.get('id')}: deviceId {dev_id!r} doesn't match DEV-NNNNNN")

    variable_field = VARIABLE_FIELDS.get(container_name)
    schema_variation_confirmed = None
    if variable_field:
        has_field = sum(1 for doc in live_docs if variable_field in doc)
        schema_variation_confirmed = 0 < has_field < len(live_docs)
        if not schema_variation_confirmed:
            raise ValidationError(
                f"{container_name}: expected genuine schema variation on '{variable_field}' "
                f"({has_field}/{len(live_docs)} documents have it) — either all or none do, so the "
                "flexible-schema claim isn't actually demonstrated by this data"
            )

    return {
        "container": container_name,
        "partitionKey": partition_key_paths,
        "liveCount": len(live_docs),
        "expectedCount": len(expected),
        "sampledForFieldChecks": len(sample),
        "schemaVariationField": variable_field,
        "schemaVariationConfirmed": schema_variation_confirmed,
    }


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=Path("data/cosmos"))
    parser.add_argument("--endpoint", help="Cosmos DB endpoint URI. Defaults to $COSMOS_ENDPOINT.")
    parser.add_argument("--database-name", help="Defaults to $COSMOS_DATABASE_NAME or 'multisource'.")
    args = parser.parse_args()

    endpoint = resolve_endpoint(args.endpoint)
    database_name = args.database_name or os.environ.get("COSMOS_DATABASE_NAME", "multisource")

    client = _get_sync_client(endpoint)
    ru_tracker: list[float] = []
    results = []
    database = client.get_database_client(database_name)
    for container_name in CONTAINERS:
        expected = expected_ids(args.data_dir, container_name)
        logger.info("validating container=%s expectedCount=%d", container_name, len(expected))
        result = validate_container(database, container_name, expected, ru_tracker)
        logger.info("container=%s OK liveCount=%d", container_name, result["liveCount"])
        results.append(result)

    summary = {"database": database_name, "containers": results, "totalRU": round(sum(ru_tracker), 2)}
    logger.info("validation PASSED, totalRU=%.2f", summary["totalRU"])
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
