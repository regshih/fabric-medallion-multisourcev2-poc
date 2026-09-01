"""Shared, passwordless (Entra ID / AAD-only) Cosmos DB for NoSQL loader utilities.

No account key is read, accepted, or used anywhere in this module — every client
is built with ``DefaultAzureCredential`` (run ``az login`` first, or rely on a
managed identity when running inside Azure). This intentionally matches the
account's ``disableLocalAuth: true`` setting (see infra/cosmos/main.bicep):
key-based auth would fail even if attempted.

Bulk loads use the async SDK with bounded concurrency and per-request 429
(rate-limited) retry with backoff, because Cosmos DB rate limiting is real
even at POC scale — a naive synchronous loop over ~2,000 documents can start
tripping RU limits on a serverless account under default indexing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterable

logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("cosmos.common")

CONTAINERS = ("digitalSessions", "devices", "fraudAlerts")
DEFAULT_CONCURRENCY = 25
DEFAULT_MAX_RETRIES = 5


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    """Yield documents from a JSON Lines file, failing loudly on malformed rows."""
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            document = json.loads(line)
            if not document.get("id") or not document.get("customerId"):
                raise ValueError(f"{path}:{line_number}: 'id' and 'customerId' are required fields")
            yield document


def resolve_endpoint(endpoint: str | None) -> str:
    endpoint = endpoint or os.environ.get("COSMOS_ENDPOINT")
    if not endpoint:
        raise ValueError(
            "Set COSMOS_ENDPOINT (e.g. https://<account>.documents.azure.com:443/) or pass --endpoint. "
            "Account keys are intentionally unsupported by this account (disableLocalAuth=true)."
        )
    return endpoint


async def _bulk_upsert_container(container: Any, documents: list[dict[str, Any]], *, concurrency: int, max_retries: int) -> dict[str, Any]:
    from azure.cosmos.exceptions import CosmosHttpResponseError

    semaphore = asyncio.Semaphore(concurrency)
    ru_charges: list[float] = []
    errors: list[str] = []

    def _record_ru(headers: dict[str, str], _result: Any) -> None:
        charge = headers.get("x-ms-request-charge")
        if charge is not None:
            ru_charges.append(float(charge))

    async def _upsert_one(document: dict[str, Any]) -> None:
        async with semaphore:
            attempt = 0
            while True:
                try:
                    await container.upsert_item(body=document, response_hook=_record_ru)
                    return
                except CosmosHttpResponseError as exc:
                    attempt += 1
                    if exc.status_code == 429 and attempt <= max_retries:
                        retry_after_ms = exc.headers.get("x-ms-retry-after-ms") if exc.headers else None
                        delay = (float(retry_after_ms) / 1000.0) if retry_after_ms else min(2 ** attempt, 30)
                        logger.warning("429 rate-limited on %s, retry %d/%d after %.2fs", document.get("id"), attempt, max_retries, delay)
                        await asyncio.sleep(delay)
                        continue
                    errors.append(f"{document.get('id')}: HTTP {exc.status_code} {exc.message}")
                    raise

    results = await asyncio.gather(*(_upsert_one(doc) for doc in documents), return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        raise RuntimeError(f"{len(failures)} of {len(documents)} upserts failed after retries: {errors[:5]}{' ...' if len(errors) > 5 else ''}")
    return {"count": len(documents), "totalRU": round(sum(ru_charges), 2)}


async def _load_directory_async(
    data_dir: Path,
    *,
    endpoint: str,
    database_name: str,
    concurrency: int,
    max_retries: int,
) -> dict[str, dict[str, Any]]:
    from azure.cosmos.aio import CosmosClient
    from azure.identity.aio import DefaultAzureCredential

    credential = DefaultAzureCredential()
    client = CosmosClient(endpoint, credential=credential)
    results: dict[str, dict[str, Any]] = {}
    try:
        database = client.get_database_client(database_name)
        for container_name in CONTAINERS:
            path = data_dir / f"{container_name}.jsonl"
            if not path.exists():
                logger.info("no file for container=%s at %s, skipping", container_name, path)
                continue
            documents = list(read_jsonl(path))
            if not documents:
                results[container_name] = {"count": 0, "totalRU": 0.0}
                continue
            container = database.get_container_client(container_name)
            outcome = await _bulk_upsert_container(container, documents, concurrency=concurrency, max_retries=max_retries)
            logger.info("container=%s upserted=%d totalRU=%.2f", container_name, outcome["count"], outcome["totalRU"])
            results[container_name] = outcome
    finally:
        await client.close()
        await credential.close()
    return results


def load_directory(
    data_dir: Path,
    *,
    endpoint: str | None = None,
    database_name: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> dict[str, dict[str, Any]]:
    """Upsert every ``<container>.jsonl`` file in ``data_dir`` into Cosmos DB.

    Upsert (not insert/replace) makes every load idempotent — rerunning after a
    partial failure never duplicates documents, it just overwrites with the
    same content.
    """
    resolved_endpoint = resolve_endpoint(endpoint)
    resolved_database = database_name or os.environ.get("COSMOS_DATABASE_NAME", "multisource")
    return asyncio.run(
        _load_directory_async(
            data_dir,
            endpoint=resolved_endpoint,
            database_name=resolved_database,
            concurrency=concurrency,
            max_retries=max_retries,
        )
    )
