#!/usr/bin/env python3
"""Import a notebook into the Databricks workspace and run it once, as a job,
on an ephemeral (auto-terminating) job cluster — no long-running interactive
cluster is left behind.

Used to actually execute databricks/01_seed_delta_tables.py and
databricks/02_apply_incremental_batch.py against the real workspace, rather
than leaving them as scripts nobody ran.

Auth: AAD via `az login`, exchanged for a Databricks token through the SDK's
"azure-cli" auth type. No secrets are read, stored, or printed.

Usage:
    python infra/databricks/run_job.py --help
    python infra/databricks/run_job.py \\
        --workspace-host https://adb-XXXXXXXXXXXXXXXX.XX.azuredatabricks.net \\
        --workspace-resource-id /subscriptions/.../workspaces/dbw-fmv2poc-915d \\
        --local-notebook databricks/01_seed_delta_tables.py \\
        --workspace-path /Shared/fabric-medallion-multisourcev2-poc/01_seed_delta_tables \\
        --param catalog=dbw_fmv2poc_915d --param schema=banking --param volume=landing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat, Language

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("run_job")

# Small, cheap single-node VM candidates for a short-lived POC seed job, in
# preference order. Azure regional capacity for any one SKU can be
# transiently unavailable (CLOUD_PROVIDER_RESOURCE_STOCKOUT), so
# run_notebook_job() retries down this list rather than hardcoding one SKU.
PREFERRED_NODE_TYPES = (
    "Standard_DS3_v2",
    "Standard_D4s_v3",
    "Standard_D4s_v5",
    "Standard_D4ds_v5",
    "Standard_D4as_v5",
    "Standard_E4s_v3",
    "Standard_F4s_v2",
)


def candidate_node_types(ws: WorkspaceClient) -> list[str]:
    available = {n.node_type_id for n in ws.clusters.list_node_types().node_types}
    candidates = [c for c in PREFERRED_NODE_TYPES if c in available]
    if candidates:
        return candidates
    # Fall back to any general-purpose node type the SDK reports as available.
    fallback = [n.node_type_id for n in ws.clusters.list_node_types().node_types if n.node_type_id and "Standard_D" in n.node_type_id]
    if not fallback:
        raise RuntimeError("Could not find a suitable node type in this workspace/region.")
    return fallback[:5]


def pick_spark_version(ws: WorkspaceClient) -> str:
    versions = ws.clusters.spark_versions().versions
    # Prefer the newest non-ML, non-GPU LTS-style version.
    candidates = [v.key for v in versions if v.key and "scala2.12" in v.key and "ml" not in v.key.lower() and "photon" not in v.key.lower()]
    candidates.sort(reverse=True)
    if not candidates:
        raise RuntimeError("Could not find a suitable Databricks Runtime version.")
    return candidates[0]


def import_notebook(ws: WorkspaceClient, local_path: Path, workspace_path: str) -> None:
    content = local_path.read_bytes()
    parent = "/".join(workspace_path.split("/")[:-1])
    if parent:
        ws.workspace.mkdirs(parent)
    logger.info("Importing %s -> %s", local_path, workspace_path)
    ws.workspace.upload(workspace_path, content, format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)


def _is_capacity_failure(run: jobs.Run) -> bool:
    msg = (run.state.state_message or "") if run.state else ""
    return "CLOUD_PROVIDER_RESOURCE_STOCKOUT" in msg or "SkuNotAvailable" in msg or "not available in location" in msg


def run_notebook_job(ws: WorkspaceClient, workspace_path: str, params: dict[str, str], run_name: str) -> jobs.Run:
    spark_version = pick_spark_version(ws)
    candidates = candidate_node_types(ws)
    logger.info("Node type candidates (in order): %s", candidates)

    last_run: jobs.Run | None = None
    for i, node_type in enumerate(candidates):
        logger.info(
            "Attempt %d/%d: ephemeral job cluster spark_version=%s node_type=%s (single-node)",
            i + 1, len(candidates), spark_version, node_type,
        )
        new_cluster = compute.ClusterSpec(
            spark_version=spark_version,
            node_type_id=node_type,
            num_workers=0,
            spark_conf={
                "spark.master": "local[*, 4]",
                "spark.databricks.cluster.profile": "singleNode",
            },
            custom_tags={
                "ResourceClass": "SingleNode",
                "project": "fabric-medallion-multisourcev2-poc",
            },
            data_security_mode=compute.DataSecurityMode.SINGLE_USER,
        )

        logger.info("Submitting one-time job run %r for %s with params=%s", run_name, workspace_path, params)
        waiter = ws.jobs.submit(
            run_name=run_name,
            tasks=[
                jobs.SubmitTask(
                    task_key="run_notebook",
                    new_cluster=new_cluster,
                    notebook_task=jobs.NotebookTask(notebook_path=workspace_path, base_parameters=params),
                    timeout_seconds=3600,
                )
            ],
        )
        logger.info("Run submitted (run_id=%s). Waiting for completion (job cluster starts + runs + auto-terminates)...", waiter.run_id)
        run = waiter.result(timeout=timedelta(minutes=30))
        last_run = run

        if run.state and run.state.result_state == jobs.RunResultState.SUCCESS:
            return run
        if _is_capacity_failure(run) and i < len(candidates) - 1:
            logger.warning("Node type %s hit a capacity failure; retrying with next candidate.", node_type)
            continue
        return run

    assert last_run is not None
    return last_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-host", required=True)
    parser.add_argument("--workspace-resource-id", required=True)
    parser.add_argument("--local-notebook", type=Path, required=True)
    parser.add_argument("--workspace-path", required=True)
    parser.add_argument("--param", action="append", default=[], metavar="KEY=VALUE", help="Notebook widget parameter, may be repeated")
    parser.add_argument("--run-name", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    params = {}
    for kv in args.param:
        if "=" not in kv:
            raise ValueError(f"--param must be KEY=VALUE, got: {kv!r}")
        k, v = kv.split("=", 1)
        params[k] = v

    ws = WorkspaceClient(host=args.workspace_host, azure_workspace_resource_id=args.workspace_resource_id, auth_type="azure-cli")

    import_notebook(ws, args.local_notebook, args.workspace_path)
    run_name = args.run_name or f"poc-{args.local_notebook.stem}"
    started = time.monotonic()
    run = run_notebook_job(ws, args.workspace_path, params, run_name)
    elapsed = time.monotonic() - started

    state = run.state
    result = {
        "run_id": run.run_id,
        "run_page_url": run.run_page_url,
        "life_cycle_state": str(state.life_cycle_state) if state else None,
        "result_state": str(state.result_state) if state else None,
        "state_message": state.state_message if state else None,
        "elapsed_seconds": round(elapsed, 1),
    }
    print(json.dumps(result, indent=2))

    if state and state.result_state != jobs.RunResultState.SUCCESS:
        logger.error("Job run did not succeed: %s", result)
        return 1
    logger.info("Job run succeeded in %.1fs.", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
