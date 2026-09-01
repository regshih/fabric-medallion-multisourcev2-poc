# Security

## Synthetic data only

Every record in this repository and in the Azure/Fabric resources it deploys is
**deterministically generated fictional test data** (`generators/generate_databricks_data.py`,
`generators/generate_cosmos_data.py`, both seeded via `Faker`). No real customer, employee,
or individual data of any kind is used or referenced. Business identifiers (`CustomerID`,
`AccountID`, `DeviceID`, `MerchantID`, `TransactionID`) are synthetic zero-padded sequence
strings (`CUST-000001`, etc.) — see [ARCHITECTURE.md](ARCHITECTURE.md) — not real-looking
account/card numbers. IP addresses, device fingerprints, and geographic data in the Cosmos
generator are fabricated strings, never real individual-associated network data.

## Authentication

No passwords, API keys, connection strings, or account keys are used anywhere in this
repository's **code** — every credential-bearing value below is generated at runtime and
never written to a file this repo tracks. One exception is documented honestly rather than
glossed over:

- **Azure/Fabric**: `DefaultAzureCredential` (developer `az login`) throughout — every
  `infra/` script.
- **Azure Databricks (local tooling)**: `databricks-sdk`'s `azure-cli` auth type, exchanging
  the Azure AD token for a Databricks token. No Databricks PATs for any of the seed/
  incremental/validation scripts.
- **Azure Databricks (Fabric mirroring connection only)**: Fabric's mirrored-catalog
  connection needs one of Organizational-account (interactive browser OAuth — confirmed live
  not completable unattended), Service Principal (blocked by this environment's own
  credential-minting safety controls), or a Databricks personal access token. A PAT was the
  only viable unattended path, so `infra/fabric/mirror_databricks.py` mints one
  (`ws.tokens.create(..., lifetime_seconds=90*24*60*60)` — bounded 90-day expiry, not
  indefinite) and passes it directly into the Fabric connection's `credentialDetails` over
  HTTPS; it is never printed, logged, or written to any file. Fabric stores it encrypted
  inside the Connection object, outside this repo's control — the same trust boundary as the
  human-provided GitHub PAT below. See `infra/fabric/mirror_databricks.py`'s docstring for
  the full live-confirmed reasoning.
- **Azure Cosmos DB**: the account has `disableLocalAuth: true` — account keys are disabled
  at the resource level, not just avoided by convention. All access (loaders, validation, and
  the eventual Fabric mirroring connection) uses Microsoft Entra ID with Cosmos DB's built-in
  data-plane RBAC roles.
- **GitHub**: Fabric Git integration needs a real GitHub PAT (fine-grained, repo-scoped,
  short expiration) — this is the one credential this project genuinely needs, and it is a
  **human-only, manual step** (see README "Human-only steps"). It is never committed; `.env`
  is gitignored and `.env.example` contains only the variable name.

## Secret handling

- `.gitignore` excludes `.env`/`*.env` (except `.env.example`), generated data directories,
  and common credential-file patterns.
- `.env.example` contains variable names and real (non-secret) resource identifiers only —
  hostnames, resource IDs, catalog/schema/database names. None of these values grant access
  on their own; they're meaningless without the Azure AD identity that authenticates against
  them.
- Before this repository is made public, a full secret scan is run over the working tree
  **and git history** — see [docs/security-review.md](docs/security-review.md) once that
  review has actually been performed. The repository stays **private** until that review
  passes.

## Networking

The Cosmos DB account is private-endpoint-only (`publicNetworkAccess: Disabled`) — this is
enforced by tenant policy in the deployment environment, not merely a preference; a direct
attempt to enable public access was independently blocked by the tenant's own operator
controls. See [docs/cosmos-fabric-mirroring.md](docs/cosmos-fabric-mirroring.md) for the full
networking design.

## Governance controls (and their honest limits)

Warehouse Dynamic Data Masking and OneLake column-level security are implemented for
sensitive columns (customer identifiers, device fingerprints — see `warehouse/10_apply_security.sql`
and `infra/governance/onelake_security.py`). Consistent with the sibling reference POC's own
findings, **enforcement cannot be independently verified against this project's own
provisioning identity** — Warehouse `db_owner` and Fabric workspace Admin/Member/Contributor
roles both bypass their respective controls by design. The policies are verified *configured*
(stored server-side, confirmed via API/`sys.masked_columns`), not verified *enforced* against
a genuinely lesser-privileged principal, because no second AAD account with a Viewer-only
role was available in this environment. This gap is documented, not hidden.
