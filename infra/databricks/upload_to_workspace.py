#!/usr/bin/env python3
"""Upload locally generated Parquet files into a Unity Catalog managed Volume,
so the seed Databricks job can read them with plain PySpark.

Creates the target volume (idempotent) at
``<catalog>.<schema>.<volume>`` and uploads every ``*.parquet`` file found in
``--source-dir`` to ``/Volumes/<catalog>/<schema>/<volume>/<filename>``,
overwriting if already present (safe to re-run after regenerating data).

Auth: AAD via `az login`, exchanged for a Databricks token through the SDK's
"azure-cli" auth type. No secrets are read, stored, or printed.

Windows/Git Bash note: MSYS auto-converts leading-`/` CLI arguments (like
--workspace-resource-id /subscriptions/...) into bogus Windows paths (e.g.
C:/Program Files/Git/subscriptions/...), which Databricks then rejects as
"Invalid resource ID". Run this from PowerShell, or set MSYS_NO_PATHCONV=1
in Git Bash before invoking it.

Usage:
    python infra/databricks/upload_to_workspace.py --help
    python infra/databricks/upload_to_workspace.py \\
        --workspace-host https://adb-XXXXXXXXXXXXXXXX.XX.azuredatabricks.net \\
        --workspace-resource-id /subscriptions/.../workspaces/dbw-fmv2poc-915d \\
        --catalog dbw_fmv2poc_915d --schema banking --volume landing \\
        --source-dir ./data/databricks
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import VolumeType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("upload_to_workspace")


def ensure_volume(ws: WorkspaceClient, catalog: str, schema: str, volume: str) -> str:
    existing = {v.name for v in ws.volumes.list(catalog_name=catalog, schema_name=schema)}
    if volume not in existing:
        logger.info("Creating managed volume %s.%s.%s", catalog, schema, volume)
        ws.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume,
            volume_type=VolumeType.MANAGED,
            comment="Landing area for locally generated synthetic Parquet, uploaded for the seed job",
        )
    else:
        logger.info("Volume %s.%s.%s already exists.", catalog, schema, volume)
    return f"/Volumes/{catalog}/{schema}/{volume}"


def upload_files(ws: WorkspaceClient, source_dir: Path, volume_path: str) -> list[str]:
    files = sorted(source_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No *.parquet files found in {source_dir}. Run generators/generate_databricks_data.py first.")
    uploaded = []
    for f in files:
        target = f"{volume_path}/{f.name}"
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info("Uploading %s (%.1f MB) -> %s", f, size_mb, target)
        with f.open("rb") as fh:
            ws.files.upload(target, fh, overwrite=True)
        uploaded.append(target)
    return uploaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-host", required=True)
    parser.add_argument("--workspace-resource-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume", default="landing")
    parser.add_argument("--source-dir", type=Path, default=Path("./data/databricks"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ws = WorkspaceClient(host=args.workspace_host, azure_workspace_resource_id=args.workspace_resource_id, auth_type="azure-cli")

    try:
        volume_path = ensure_volume(ws, args.catalog, args.schema, args.volume)
    except DatabricksError as exc:
        logger.error(
            "Could not create/access volume %s.%s.%s: %s. Ensure the catalog and schema exist "
            "(run infra/databricks/setup_unity_catalog.py first).",
            args.catalog, args.schema, args.volume, exc,
        )
        raise

    uploaded = upload_files(ws, args.source_dir, volume_path)
    print(json.dumps({"volume_path": volume_path, "uploaded_files": uploaded}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
