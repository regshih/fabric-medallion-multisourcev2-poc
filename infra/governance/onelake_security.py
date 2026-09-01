#!/usr/bin/env python
"""Apply column-level OneLake security to gold_lh, safely.

The dataAccessRoles API is a full-replace — a naive PUT would silently wipe
every other role/rule/constraint. This GETs the current roles, preserves
everything, uses the GET response's ETag for optimistic concurrency
(If-Match), and always does a server-side dry run (?dryRun=true) before
--apply actually submits the replacement.

Restricts two sensitive columns from the DefaultReader role by allow-listing
every OTHER column on the governed tables:
  - gold_lh.DimCustomer.CustomerID  (business identifier)
  - gold_lh.DimDevice.DeviceFingerprint (device fingerprint)

Fabric's OneLake column-level security only supports columnEffect "Permit" --
a "Deny" constraint (this script's original design: blanket Path:* Permit
plus an explicit Deny on the sensitive columns) is rejected outright with
PolicyValidationError ("Column level security only supports Permit effect"),
confirmed live 2026-09-01, not assumed from docs. Redesigned as an allow-list:
each governed table gets an explicit Permit naming every column except the
sensitive one(s) -- anything not named is implicitly denied.

Like Dynamic Data Masking on the Warehouse side, this cannot be proven
enforced against this session's own identity — Admin/Member/Contributor
workspace roles carry implicit Write access that overrides OneLake's
Read-based restrictions. The policy IS verifiably stored server-side
(re-fetch after apply and confirm the constraint is present); enforcement
needs a second, genuinely least-privileged AAD principal to test with.

Usage:
    python infra/governance/onelake_security.py [--apply] [--role-name DefaultReader]
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402
from infra.fabric.common import find_item, find_workspace  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("onelake_security")

ROLE_FIELDS = ("name", "kind", "decisionRules", "members")

# tablePath -> ALLOWED column names (everything else on the table is implicitly
# denied). Table paths are OneLake's /Tables/<name> convention; names must
# match the lowercase metastore casing (see the sibling banking POC's
# CLAUDE.md re: Spark/Hive lowercasing). Full column lists per
# nb_gold_build.py's DimCustomer/DimDevice select() + the _gold_loaded_at
# audit column every write_gold() call adds.
TABLE_ALLOWED_COLUMNS: dict[str, list[str]] = {
    "/Tables/dimcustomer": ["customersk", "_gold_loaded_at"],  # excludes customerid
    "/Tables/dimdevice": [
        "devicesk", "deviceid", "customerid", "customersk", "os", "istrusted", "_gold_loaded_at",
    ],  # excludes devicefingerprint
}


def _is_read_rule(rule: dict[str, Any]) -> bool:
    for scope in rule.get("permission", []):
        if scope.get("attributeName") == "Action" and "Read" in scope.get("attributeValueIncludedIn", []):
            return True
    return False


def build_replacement(roles: list[dict[str, Any]], role_name: str) -> dict[str, Any]:
    replacement = copy.deepcopy(roles)
    targets = [role for role in replacement if role.get("name") == role_name]
    if len(targets) != 1:
        raise RuntimeError(f"Expected exactly one data access role named {role_name!r}; found {len(targets)}")

    changed = 0
    for rule in targets[0].get("decisionRules", []):
        if not _is_read_rule(rule):
            continue
        constraints = rule.setdefault("constraints", {})
        columns = constraints.setdefault("columns", [])
        for table_path, allowed in TABLE_ALLOWED_COLUMNS.items():
            columns[:] = [entry for entry in columns if entry.get("tablePath") != table_path]
            columns.append({
                "tablePath": table_path,
                "columnNames": allowed,
                "columnEffect": "Permit",
                "columnAction": ["Read"],
            })
            changed += 1
    if changed == 0:
        raise RuntimeError(f"Role {role_name!r} has no Read decision rule to attach constraints to")

    return {"value": [{key: role[key] for key in ROLE_FIELDS if key in role} for role in replacement]}


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="apply after the mandatory server dry run")
    parser.add_argument("--role-name", default="DefaultReader")
    parser.add_argument("--lakehouse-name", default="gold_lh")
    args = parser.parse_args()

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    session = get_session()
    workspace = find_workspace(session, workspace_name)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found")
    item = find_item(session, workspace["id"], args.lakehouse_name, "Lakehouse")
    if not item:
        raise RuntimeError(f"lakehouse {args.lakehouse_name!r} not found")

    path = f"{FABRIC_API}/workspaces/{workspace['id']}/items/{item['id']}/dataAccessRoles"
    current = session.get(path)
    current.raise_for_status()
    etag = current.headers.get("ETag") or current.headers.get("Etag")
    if not etag:
        raise RuntimeError("Data access role GET did not return an ETag; refusing a full replacement")

    payload = build_replacement(current.json().get("value", []), args.role_name)
    headers = {"If-Match": etag}

    dry_run = session.put(f"{path}?dryRun=true", json=payload, headers=headers)
    dry_run.raise_for_status()
    log.info("server dry run succeeded; existing roles and unrelated constraints preserved")

    if args.apply:
        resp = session.put(path, json=payload, headers=headers)
        resp.raise_for_status()
        log.info("full replacement applied with If-Match concurrency protection")
        print(json.dumps({"applied": True, "etag": resp.headers.get("ETag")}, indent=2))
    else:
        print(json.dumps({"applied": False, "etag": etag, "payload": payload}, indent=2))


if __name__ == "__main__":
    main()
