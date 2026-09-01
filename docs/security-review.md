# Public-repository security review

Performed 2026-09-01, before flipping this repository from private to public, per the
process [SECURITY.md](../SECURITY.md) commits to. Two passes were run over the course of this
project — an initial pass early in development, and a final pass after all pipeline/governance
work was complete — both are summarized here since the second pass re-scanned the entire
history, superseding the first.

## Scope

- Full git history: `git log -p --all` (every commit, every branch), not just the current tree
  — a secret committed once and later removed is still a leak if history isn't checked.
- Current working tree: every tracked file (`git ls-files`), plus `.gitignore` correctness.
- `.env` / secret-file handling: confirmed never tracked, confirmed ignored.

## Method

Pattern-based scanning for credential-shaped strings, run against the full history dump and
the working tree:

- Databricks personal access tokens (`dapi[0-9a-f]{20,}`)
- GitHub tokens, classic and fine-grained (`ghp_...`, `github_pat_...`)
- Azure Storage account keys / SAS / connection strings (`AccountKey=`,
  `SharedAccessSignature=`, `DefaultEndpointsProtocol=`)
- PEM-format private keys (`-----BEGIN`)
- Generic `client_secret=`/`password=` assignments with a non-trivial value
- Bearer tokens and JWTs (`Bearer <token>`, `eyJ...eyJ` two-segment pattern)
- AWS access keys (`AKIA[0-9A-Z]{16}`), Slack tokens (`xox[baprs]-...`)
- Long base64/hex blobs adjacent to key-shaped variable names (manual review of matches)

Followed by manual review of:

- `.env.example` for anything beyond variable names and non-secret resource identifiers
- Every script that handles a credential (`infra/fabric/mirror_databricks.py`,
  `infra/fabric/auth.py`, `infra/databricks/*.py`, `cosmos/common.py`) to confirm tokens are
  read from `DefaultAzureCredential`/environment at runtime and never logged or written to a
  tracked file
- `git ls-files` cross-referenced against `.gitignore`, confirming no data/log/parquet/`.env`
  file was ever committed
- Largest tracked files by size (`git ls-files -z | xargs du -h`), confirming nothing looks
  like an accidentally-committed binary or data dump

## Findings

**One finding, fixed.** `infra/databricks/provision_workspace.py`'s usage-example docstring
had the real Azure subscription ID hardcoded, while every other doc/example in the repo
correctly used a `<sub-id>` placeholder (`.env.example`, `docs/deployment.md`). A subscription
ID alone is not a credential — it grants no access without a properly authenticated Azure AD
identity behind it, and Microsoft's own guidance doesn't treat it as secret — but it
needlessly identifies the specific tenant, so it was redacted. Fixed in commit `08e7daa`. The
value remains in earlier commits' history (a subscription ID is not sensitive enough to
justify a history rewrite, which is itself a disruptive, hard-to-reverse operation on shared
history — this was a judgment call, documented here rather than silently made).

**Everything else scanned clean:**

- No PATs, API keys, connection strings, account keys, private keys, or JWTs anywhere in
  history or the working tree.
- `.env` has never been committed at any point in history; `.gitignore` correctly excludes it.
- No data, log, or parquet files were ever committed (all generated output stays local, per
  `.gitignore`).
- No unusually large tracked files (largest is 20 KB — ordinary source/doc files).
- Every credential-handling script confirmed to source tokens from `DefaultAzureCredential` or
  environment variables at runtime, never hardcoding or logging them. The one intentional
  exception — a Databricks PAT minted for the Fabric mirroring connection, and the
  human-provided GitHub PAT for Git integration — are both documented in SECURITY.md as
  deliberate, bounded-lifetime exceptions, passed directly over HTTPS into Fabric's own
  encrypted connection storage or the gitignored `.env`, never into a tracked file.

**On resource identifiers in general**: this repo's documentation quotes many real Azure/Fabric
resource IDs, GUIDs, hostnames, and workspace/item names verbatim (a deliberate project
convention — see `.env.example` and `docs/deployment.md`: "This POC's actual identifiers are
non-secret"). These are not access credentials; they identify *what* to connect to, not
*permission* to connect. Anyone with these values still needs a real, authenticated Azure AD
identity with the appropriate RBAC role to do anything with them. The subscription ID finding
above was fixed for consistency with this stated policy, not because it was independently
assessed as a credential.

## Conclusion

Clear to make public from a secrets standpoint. No credential of any kind — PAT, key,
connection string, or token — exists in this repository's tracked files or git history at any
point in time.
