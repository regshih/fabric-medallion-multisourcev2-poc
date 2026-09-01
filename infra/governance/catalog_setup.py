#!/usr/bin/env python
"""Set up OneLake catalog governance: descriptions on every item, and
(optionally) a domain assignment.

OneLake catalog itself is not a resource you provision — it's a tenant-wide
view Fabric already builds over every item you can access. What this script
does to help this project show up well in it:
  - Sets a description on every item via the Items Update Item API
    (PATCH /v1/workspaces/{id}/items/{id}). Description text is what the
    Catalog Search API (see catalog_search.py) full-text matches against.
  - Optionally creates a domain and assigns this workspace to it, via the
    Admin Domains API. This needs the caller's Entra directory role to carry
    tenant-settings access (confirmed live in the sibling banking POC: a
    Global Reader was blocked, a Global Administrator was not) — pass
    --try-domain to attempt it; a 403 is reported, not treated as fatal.

Idempotent: skips items that already have a description unless --force.

Usage:
    python infra/governance/catalog_setup.py [--force] [--try-domain]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402
from infra.fabric.common import list_all  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("catalog_setup")

# display_name -> description. Covers every item this repo's provisioning/
# deploy scripts create or resolve (see ARCHITECTURE.md for the fixed names).
ITEM_DESCRIPTIONS = {
    "silver_lh": (
        "Silver layer: cleansed/typed/quarantined data read directly from "
        "the mirrored Databricks Unity Catalog (shortcut) and mirrored "
        "Cosmos DB (Delta replica) sources. No physical Bronze copy exists."
    ),
    "gold_lh": (
        "Gold layer: cross-source star schema (5 dims, 3 facts) plus "
        "AggCustomerRiskProfile, the primary cross-source analytical output "
        "combining Databricks transaction/risk data with Cosmos session/"
        "device/fraud-alert data. See ARCHITECTURE.md."
    ),
    "gold_wh": (
        "Gold layer, governed SQL access: zero-copy cross-database views "
        "plus physical _base_* tables carrying native Dynamic Data Masking "
        "and SESSION_CONTEXT row-level security on sensitive columns."
    ),
    "pl_multisource_medallion": (
        "Orchestrates source validation -> Silver -> Gold -> Warehouse "
        "publish -> reconciliation, with a paired failure-logging activity "
        "on every stage writing to gold_lh.control_pipeline_run_log."
    ),
    "source_validation": (
        "Notebook: confirms both mirrored sources (Databricks shortcut, "
        "Cosmos replica) exist with expected schema/row counts before "
        "Silver runs. First stage of pl_multisource_medallion."
    ),
    "silver_transform": (
        "Notebook: reads both mirrored sources, applies quality rules and "
        "quarantine, writes Silver Delta tables."
    ),
    "gold_build": (
        "Notebook: builds the gold_lh star schema and AggCustomerRiskProfile "
        "from Silver."
    ),
    "warehouse_publish": (
        "Notebook/stage: refreshes gold_wh's physical _base_* tables and "
        "governed views from gold_lh."
    ),
    "reconciliation": (
        "Notebook: source-vs-Fabric and Silver-vs-Gold count/quality "
        "reconciliation, writing gold_lh.reconciliation_results."
    ),
    "pipeline_log": (
        "Shared logging notebook: writes stage start/end/status/row counts/"
        "errors to gold_lh.control_pipeline_run_log — invoked on both the "
        "success and failure path of every pipeline stage."
    ),
    "gold_consumption_demo": (
        "Analyst-facing discovery notebook: inventories all layers and runs "
        "the cross-source customer risk-profile and fraud-investigation "
        "business queries. Not part of the pipeline — run interactively."
    ),
}

TYPE_FALLBACK_DESCRIPTIONS = {
    "SQLEndpoint": (
        "Read-only SQL analytics endpoint auto-created by a Lakehouse item "
        "in this project. Not directly managed — see the parent item."
    ),
}

for _name, _desc in {**ITEM_DESCRIPTIONS, **TYPE_FALLBACK_DESCRIPTIONS}.items():
    assert len(_desc) <= 256, f"{_name!r} description is {len(_desc)} chars, over the 256 limit"


def find_workspace_id(session, workspace_name: str) -> str:
    for ws in list_all(session, f"{FABRIC_API}/workspaces"):
        if ws["displayName"] == workspace_name:
            return ws["id"]
    raise RuntimeError(f"workspace {workspace_name!r} not found")


def ensure_domain(session, display_name: str, description: str) -> str | None:
    resp = session.get(f"{FABRIC_API}/admin/domains?preview=false")
    if resp.status_code == 403:
        log.warning(
            "403 on domains API — needs a tenant-level Entra directory role this session may "
            "not carry (confirmed pattern in the sibling banking POC). Item descriptions will "
            "still be set."
        )
        return None
    resp.raise_for_status()
    domains = resp.json().get("domains", resp.json().get("value", []))
    existing = next((d for d in domains if d["displayName"] == display_name), None)
    if existing:
        log.info("Domain %r already exists (%s)", display_name, existing["id"])
        return existing["id"]

    resp = session.post(
        f"{FABRIC_API}/admin/domains?preview=false",
        json={"displayName": display_name, "description": description},
    )
    resp.raise_for_status()
    domain = resp.json()
    log.info("Created domain %r (%s)", display_name, domain["id"])
    return domain["id"]


def assign_workspace(session, domain_id: str, workspace_id: str) -> None:
    resp = session.post(
        f"{FABRIC_API}/admin/domains/{domain_id}/assignWorkspaces",
        json={"workspacesIds": [workspace_id]},
    )
    resp.raise_for_status()
    log.info("Assigned workspace %s to domain %s", workspace_id, domain_id)


def set_item_descriptions(session, workspace_id: str, force: bool) -> None:
    for item in list_all(session, f"{FABRIC_API}/workspaces/{workspace_id}/items"):
        description = ITEM_DESCRIPTIONS.get(item["displayName"]) or TYPE_FALLBACK_DESCRIPTIONS.get(item["type"])
        if not description:
            log.info("No description mapped for %s %r — skipping", item["type"], item["displayName"])
            continue
        if item.get("description") and not force:
            log.info("Skipping %s %r — already has a description (use --force to overwrite)", item["type"], item["displayName"])
            continue
        resp = session.patch(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item['id']}",
            json={"description": description},
        )
        resp.raise_for_status()
        log.info("Set description on %s %r", item["type"], item["displayName"])


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-name", default=os.getenv("FABRIC_WORKSPACE_NAME"))
    parser.add_argument("--domain-name", default="Retail Banking Analytics")
    parser.add_argument(
        "--domain-description",
        default="Cross-source (Databricks + Cosmos DB) fraud-risk medallion workloads.",
    )
    parser.add_argument("--try-domain", action="store_true", help="attempt domain create+assign")
    parser.add_argument("--force", action="store_true", help="overwrite existing item descriptions")
    args = parser.parse_args()

    if not args.workspace_name:
        log.error("FABRIC_WORKSPACE_NAME must be set (.env or --workspace-name).")
        sys.exit(1)

    session = get_session()
    workspace_id = find_workspace_id(session, args.workspace_name)

    if args.try_domain:
        domain_id = ensure_domain(session, args.domain_name, args.domain_description)
        if domain_id:
            assign_workspace(session, domain_id, workspace_id)
    else:
        log.info("Skipping domain create/assign (default — pass --try-domain to attempt it).")

    set_item_descriptions(session, workspace_id, args.force)
    log.info("Done.")


if __name__ == "__main__":
    sys.exit(main())
