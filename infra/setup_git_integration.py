#!/usr/bin/env python
"""Connect this workspace to its GitHub repo via Fabric Git integration and
commit its current state. Idempotent — safe to re-run.

Prerequisites (human, one-time):
  - Fabric Admin Portal -> Tenant settings -> Git integration ->
    "Users can sync workspace items with GitHub repositories" must be
    enabled tenant-wide (this is a real, previously-hit blocker in the
    sibling banking POC — see its CLAUDE.md T8 for the full trace).
  - A GitHub fine-grained PAT scoped to just this repo, Contents: Read and
    write, short expiration — set as GITHUB_PAT in .env. Never commit a
    real value; .env is gitignored.
  - The target Fabric capacity must be Active, not Paused — git/connect can
    return a bare 500 InternalServerError (not a clear message) if it isn't.

What this does:
  1. Creates (or reuses) a Fabric Connection storing the GitHub PAT
     (POST /v1/connections, credentialType=Key) — GitHub does not support
     Automatic credentials.
  2. Connects the workspace to gitProviderDetails.gitProviderType="GitHub".
     directoryName is a dedicated subfolder (default "fabric_git"), not repo
     root, so Fabric's own per-item git folder format doesn't collide with
     this repo's existing layout. That subfolder must already exist in the
     repo (git doesn't track empty directories) before running this.
  3. Initializes the connection and, based on the required action, either
     commits the workspace's current state to git or updates the workspace
     from git.

Usage:
    python infra/setup_git_integration.py [--directory fabric_git] [--comment "..."]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from infra.fabric.auth import FABRIC_API, get_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("setup_git_integration")

CONNECTION_NAME = "fabric-medallion-multisourcev2-poc-git-sync"


def find_workspace_id(session, workspace_name: str) -> str:
    resp = session.get(f"{FABRIC_API}/workspaces")
    resp.raise_for_status()
    workspace = next((w for w in resp.json()["value"] if w["displayName"] == workspace_name), None)
    if not workspace:
        raise RuntimeError(f"workspace {workspace_name!r} not found")
    return workspace["id"]


def ensure_connection(session, repo_url: str, pat: str) -> str:
    resp = session.get(f"{FABRIC_API}/connections")
    resp.raise_for_status()
    existing = next((c for c in resp.json()["value"] if c["displayName"] == CONNECTION_NAME), None)
    if existing:
        log.info("Connection %r already exists (%s) — reusing", CONNECTION_NAME, existing["id"])
        return existing["id"]

    resp = session.post(f"{FABRIC_API}/connections", json={
        "connectivityType": "ShareableCloud",
        "displayName": CONNECTION_NAME,
        "connectionDetails": {
            "type": "GitHubSourceControl",
            "creationMethod": "GitHubSourceControl.Contents",
            "parameters": [{"dataType": "Text", "name": "url", "value": repo_url}],
        },
        "credentialDetails": {"credentials": {"credentialType": "Key", "key": pat}},
    })
    resp.raise_for_status()
    connection_id = resp.json()["id"]
    log.info("Created connection %r (%s)", CONNECTION_NAME, connection_id)
    return connection_id


def ensure_connected(session, workspace_id: str, owner: str, repo: str, branch: str,
                      directory: str, connection_id: str) -> None:
    resp = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/git/connection")
    resp.raise_for_status()
    if resp.json()["gitConnectionState"] != "NotConnected":
        log.info("workspace already connected (state=%s) — skipping connect", resp.json()["gitConnectionState"])
        return

    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/git/connect", json={
        "gitProviderDetails": {
            "gitProviderType": "GitHub",
            "ownerName": owner,
            "repositoryName": repo,
            "branchName": branch,
            "directoryName": directory,
        },
        "myGitCredentials": {"source": "ConfiguredConnection", "connectionId": connection_id},
    })
    resp.raise_for_status()
    log.info("connected workspace to %s/%s (%s)", owner, repo, directory)


def wait_for_operation(session, resp) -> None:
    if resp.status_code != 202:
        return
    loc = resp.headers["Location"]
    retry_after = int(resp.headers.get("Retry-After", 10))
    while True:
        time.sleep(retry_after)
        op = session.get(loc)
        op.raise_for_status()
        body = op.json()
        if body["status"] in ("Succeeded", "Failed"):
            if body["status"] == "Failed":
                raise RuntimeError(f"git operation failed: {body}")
            return


def sync(session, workspace_id: str, connection_id: str, comment: str) -> None:
    resp = session.patch(f"{FABRIC_API}/workspaces/{workspace_id}/git/myGitCredentials", json={
        "source": "ConfiguredConnection", "connectionId": connection_id,
    })
    resp.raise_for_status()

    state = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/git/connection").json()["gitConnectionState"]
    if state == "ConnectedAndInitialized":
        log.info("already initialized — use Git Status/Commit To Git APIs directly for ongoing sync")
        return

    resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/git/initializeConnection", json={})
    resp.raise_for_status()
    init = resp.json()

    if init["requiredAction"] == "CommitToGit":
        log.info("committing workspace state to git (head=%s)", init["workspaceHead"])
        resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/git/commitToGit", json={
            "mode": "All", "workspaceHead": init["workspaceHead"], "comment": comment,
        })
        resp.raise_for_status()
        wait_for_operation(session, resp)
    elif init["requiredAction"] == "UpdateFromGit":
        log.info("updating workspace from git (remote=%s)", init["remoteCommitHash"])
        resp = session.post(f"{FABRIC_API}/workspaces/{workspace_id}/git/updateFromGit", json={
            "remoteCommitHash": init["remoteCommitHash"], "workspaceHead": init["workspaceHead"],
        })
        resp.raise_for_status()
        wait_for_operation(session, resp)
    else:
        log.info("no action required (%s) — already in sync", init["requiredAction"])


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--directory", default="fabric_git")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--comment", default="Sync via infra/setup_git_integration.py")
    args = parser.parse_args()

    workspace_name = os.environ["FABRIC_WORKSPACE_NAME"]
    owner, repo = os.environ["GITHUB_REPO"].split("/")
    pat = os.environ.get("GITHUB_PAT")
    if not pat:
        log.error("GITHUB_PAT must be set in .env (fine-grained PAT scoped to this repo, Contents: Read and write).")
        sys.exit(1)

    session = get_session()
    workspace_id = find_workspace_id(session, workspace_name)
    connection_id = ensure_connection(session, f"https://github.com/{owner}/{repo}", pat)
    ensure_connected(session, workspace_id, owner, repo, args.branch, args.directory, connection_id)
    sync(session, workspace_id, connection_id, args.comment)
    log.info("Done. git connection state: %s",
              session.get(f"{FABRIC_API}/workspaces/{workspace_id}/git/connection").json()["gitConnectionState"])


if __name__ == "__main__":
    sys.exit(main())
